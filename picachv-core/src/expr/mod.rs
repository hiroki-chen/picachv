use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, vec};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use picachv_error::{picachv_bail, PicachvResult};
use picachv_message::binary_operator;
use rayon::prelude::*;
use spin::RwLock;
use uuid::Uuid;

use crate::arena::Arena;
use crate::constants::GroupByMethod;
use crate::dataframe::PolicyRef;
use crate::policy::context::ExpressionEvalContext;
use crate::policy::types::{AnyValue, ValueArrayRef};
use crate::policy::{Policy, TransformType};
use crate::policy_agg_label;
use crate::thread_pool::THREAD_POOL;
use crate::udf::Udf;

pub mod builder;
pub mod check;
pub mod pexpr;

/// Stores the expressions.
pub type ExprArena = Arena<AExpr>;

#[derive(PartialEq, Hash, Clone)]
pub enum AAggExpr {
    Min { input: Uuid, propagate_nans: bool },
    Max { input: Uuid, propagate_nans: bool },
    Median(Uuid),
    NUnique(Uuid),
    First(Uuid),
    Last(Uuid),
    Mean(Uuid),
    Implode(Uuid),
    // include_nulls
    Count(Uuid, bool),
    Quantile { expr: Uuid, quantile: Uuid },
    Sum(Uuid),
    AggGroups(Uuid),
    Std(Uuid, u8),
    Var(Uuid, u8),
}

impl AAggExpr {
    pub fn extract_expr(&self, expr_arena: &Arc<RwLock<ExprArena>>) -> PicachvResult<Arc<AExpr>> {
        let expr_uuid = match self {
            Self::Min { input, .. }
            | Self::Max { input, .. }
            | Self::Median(input)
            | Self::NUnique(input)
            | Self::First(input)
            | Self::Last(input)
            | Self::Mean(input)
            | Self::Implode(input)
            | Self::Count(input, _)
            | Self::Quantile { expr: input, .. }
            | Self::Sum(input)
            | Self::AggGroups(input)
            | Self::Std(input, _)
            | Self::Var(input, _) => input,
        };

        expr_arena.read().get(expr_uuid).cloned()
    }

    pub fn as_groupby_method(&self) -> GroupByMethod {
        match self {
            Self::Min { .. } => GroupByMethod::Min,
            Self::Max { .. } => GroupByMethod::Max,
            Self::Median(_) => GroupByMethod::Median,
            Self::NUnique(_) => GroupByMethod::NUnique,
            Self::First(_) => GroupByMethod::First,
            Self::Last(_) => GroupByMethod::Last,
            Self::Mean(_) => GroupByMethod::Mean,
            Self::Implode(_) => GroupByMethod::Implode,
            Self::Sum(_) => GroupByMethod::Sum,
            Self::Count(_, incl) => GroupByMethod::Count {
                include_nulls: *incl,
            },
            _ => unimplemented!(),
        }
    }
}

/// How we access a column.
#[derive(Debug, Hash, Clone, PartialEq)]
pub enum ColumnIdent {
    ColumnName(String),
    ColumnId(usize),
}

/// An [`AExpr`] represents a logical expression that is temporarily stored inside an arena
/// and has not yet been reified. This expression is created by the caller and serves as a
/// placeholder before further processing.
///
/// When performing query execution and policy checks, this expression must be reified to
/// enable proper evaluation. Reification converts an [`AExpr`] into a [`pexpr::PExpr`],
/// where values and expressions are directly managed by Picachv, eliminating the need for
/// a UUID as a pointer to reference the arena.
///
/// Performing policy checks on [`AExpr`] has a significant drawback: it requires intensive
/// lookups in the arena, which is inefficient and unfriendly to parallelism.
#[derive(Clone, Hash, PartialEq)]
pub enum AExpr {
    /// Aggregation.
    Agg {
        expr: AAggExpr,
        values: Option<Arc<Vec<ValueArrayRef>>>,
    },
    /// Select a column.
    Column(ColumnIdent),
    /// Count expression.
    Count,
    /// Making alias.
    Alias {
        expr: Uuid,
        name: String,
    },
    /// "*".
    Wildcard,
    /// Filter.
    Filter {
        input: Uuid,
        filter: Uuid,
    },
    /// Binary operations
    BinaryExpr {
        left: Uuid,
        op: binary_operator::Operator,
        right: Uuid,
        values: Option<Arc<Vec<ValueArrayRef>>>,
    },
    UnaryExpr {
        arg: Uuid,
        op: TransformType,
    },
    Literal,
    /// User-defined function.
    Apply {
        udf_desc: Udf,
        // This only takes one argument.
        args: Vec<Uuid>,
        // A type-erased array of values reified by the caller.
        values: Option<Arc<Vec<ValueArrayRef>>>,
    },
    /// a ? b : c
    Ternary {
        cond: Uuid,
        cond_values: Option<Arc<Vec<bool>>>, // should be boolean values.
        then: Uuid,
        otherwise: Uuid,
    },
}

impl AExpr {
    pub fn needs_reify(&self) -> bool {
        matches!(
            self,
            Self::Apply { .. }
                | Self::Agg { .. }
                | Self::Column(_)
                | Self::BinaryExpr { .. }
                | Self::Ternary { .. },
        )
    }

    pub fn resolve_columns(
        &self,
        expr_arena: &Arc<RwLock<ExprArena>>,
    ) -> PicachvResult<Vec<usize>> {
        match self {
            Self::Column(ColumnIdent::ColumnId(id)) => Ok(vec![*id]),
            Self::Alias { expr, .. } => {
                let expr = expr_arena.read().get(expr).cloned()?;
                expr.resolve_columns(expr_arena)
            },
            Self::Apply { args, .. } => args
                .iter()
                .map(|e| expr_arena.read().get(e).cloned())
                .collect::<PicachvResult<Vec<_>>>()?
                .into_iter()
                .map(|e| e.resolve_columns(expr_arena))
                .collect::<PicachvResult<Vec<_>>>()
                .map(|v| v.into_iter().flatten().collect()),
            Self::BinaryExpr { left, right, .. } => {
                let left = expr_arena.read().get(left).cloned()?;
                let right = expr_arena.read().get(right).cloned()?;
                let left = left.resolve_columns(expr_arena)?;
                let right = right.resolve_columns(expr_arena)?;
                Ok(left.into_iter().chain(right.into_iter()).collect())
            },
            Self::UnaryExpr { arg, .. } => {
                let arg = expr_arena.read().get(arg).cloned()?;
                arg.resolve_columns(expr_arena)
            },
            Self::Literal => Ok(vec![]),
            Self::Ternary {
                cond,
                then,
                otherwise,
                ..
            } => {
                let cond = expr_arena.read().get(cond).cloned()?;
                let then = expr_arena.read().get(then).cloned()?;
                let otherwise = expr_arena.read().get(otherwise).cloned()?;
                let cond = cond.resolve_columns(expr_arena)?;
                let then = then.resolve_columns(expr_arena)?;
                let otherwise = otherwise.resolve_columns(expr_arena)?;
                Ok(cond
                    .into_iter()
                    .chain(then.into_iter())
                    .chain(otherwise.into_iter())
                    .collect())
            },
            _ => picachv_bail!(ComputeError: "impossible {self:?}"),
        }
    }

    pub fn compute_hash(&self, row: &[&PolicyRef], expr_arena: &Arc<RwLock<ExprArena>>) -> u64 {
        let mut hasher: DefaultHasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let columns = self.resolve_columns(expr_arena).unwrap();

        for col in columns {
            row[col].hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Reifies the current expression with the provided `RecordBatch`.
    ///
    /// This operation prepares the expression for policy checking.
    pub fn reify(&mut self, values: RecordBatch) -> PicachvResult<()> {
        let values_mut = match self {
            AExpr::Apply { values, .. }
            | AExpr::BinaryExpr { values, .. }
            | AExpr::Agg { values, .. } => values,
            _ => picachv_bail!(ComputeError: "The expression does not need reification."),
        };

        let values = convert_record_batch(values)?;
        values_mut.replace(values.into());

        Ok(())
    }
}

pub(crate) fn convert_record_batch(rb: RecordBatch) -> PicachvResult<Vec<ValueArrayRef>> {
    let columns = rb.columns();

    if columns.is_empty() {
        return Ok(vec![]);
    }

    // Iterate over the rows.
    let rows = THREAD_POOL.install(|| {
        (0..rb.num_rows())
            .into_par_iter()
            .map(|i| {
                let res = columns
                    .into_par_iter()
                    .map(|column| match column.data_type() {
                        DataType::UInt8 => {
                            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::String(value.to_string())))
                        },
                        DataType::UInt16 => {
                            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::String(value.to_string())))
                        },
                        DataType::Int32 => {
                            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::Int32(value)))
                        },
                        DataType::UInt32 => {
                            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::UInt32(value as _)))
                        },
                        DataType::Int64 => {
                            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::Int64(value as _)))
                        },
                        DataType::Float64 => {
                            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::Float64(value.into())))
                        },
                        DataType::Date32 => {
                            let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
                            let value = array.value(i);
                            #[cfg(not(feature = "coq"))]
                            let val = Duration::from_days(value as _);
                            #[cfg(feature = "coq")]
                            let val = Duration::from_secs((86400 * value) as _);
                            Ok(Arc::new(AnyValue::Duration(val)))
                        },
                        DataType::Timestamp(timestamp, _) => match timestamp {
                            TimeUnit::Nanosecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .unwrap();
                                let value = array.value(i);
                                Ok(Arc::new(AnyValue::Duration(Duration::from_nanos(
                                    value as _,
                                ))))
                            },
                            _ => todo!(),
                        },
                        DataType::Utf8 => {
                            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::String(value.to_string())))
                        },
                        DataType::LargeUtf8 => {
                            let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::String(value.to_string())))
                        },
                        DataType::Boolean => {
                            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                            let value = array.value(i);
                            Ok(Arc::new(AnyValue::Boolean(value)))
                        },
                        ty => picachv_bail!(InvalidOperation: "{ty} is not supported"),
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                Ok(Arc::new(res))
            })
            .collect::<PicachvResult<Vec<_>>>()
    })?;

    Ok(rows)
}

impl fmt::Debug for AAggExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Min {
                input,
                propagate_nans,
            } => {
                write!(f, "MIN({input:?}, propagate_nans={propagate_nans})")
            },
            Self::Max {
                input,
                propagate_nans,
            } => {
                write!(f, "MAX({input:?}, propagate_nans={propagate_nans})")
            },
            Self::Median(input) => write!(f, "MEDIAN({input:?})"),
            Self::NUnique(input) => write!(f, "NUNIQUE({input:?})"),
            Self::First(input) => write!(f, "FIRST({input:?})"),
            Self::Last(input) => write!(f, "LAST({input:?})"),
            Self::Mean(input) => write!(f, "MEAN({input:?})"),
            Self::Implode(input) => write!(f, "IMPLODE({input:?})"),
            Self::Count(input, include_nulls) => {
                write!(f, "COUNT({input:?}, include_nulls={include_nulls})")
            },
            Self::Quantile { expr, quantile } => {
                write!(f, "QUANTILE({expr:?}, {quantile:?})")
            },
            Self::Sum(input) => write!(f, "SUM({input:?})"),
            Self::AggGroups(input) => write!(f, "AGG_GROUPS({input:?})"),
            Self::Std(input, n) => write!(f, "STD({input:?}, n={n})"),
            Self::Var(input, n) => write!(f, "VAR({input:?}, n={n})"),
        }
    }
}

impl fmt::Debug for AExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg { expr, .. } => write!(f, "{expr:?}"),
            Self::Column(column) => write!(f, "col({column:?})"),
            Self::Count => write!(f, "COUNT"),
            Self::Wildcard => write!(f, "*"),
            Self::Alias { expr, name } => write!(f, "ALIAS {expr:?} -> {name}"),
            Self::Filter {
                input: data,
                filter,
            } => write!(f, "{data:?} WHERE {filter:?}"),
            Self::BinaryExpr {
                left, op, right, ..
            } => write!(f, "({left:?} {op:?} {right:?})"),
            Self::UnaryExpr { arg, op } => write!(f, "{op:?} {arg:?}"),
            Self::Literal => write!(f, "LITERAL"),
            Self::Apply {
                udf_desc,
                args,
                values,
            } => write!(f, "{udf_desc:?}({args:?} + values {values:?})"),
            Self::Ternary {
                cond,
                then,
                otherwise,
                ..
            } => write!(f, "{cond:?} ? {then:?} : {otherwise:?}"),
        }
    }
}

impl fmt::Display for AExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// This functons folds the policies on the groups to check this operation is allowed.
///
/// See `eval_agg` in `expression.v`.
pub(crate) fn fold_on_groups(groups: &[PolicyRef], how: GroupByMethod) -> PicachvResult<Policy> {
    // Construct the operator.
    #[cfg(feature = "trace")]
    tracing::debug!("{how:?} {}", groups.len());

    let pf = Arc::new(policy_agg_label!(how, groups.len()));

    THREAD_POOL.install(|| {
        groups
            .par_iter()
            .fold(
                || Ok(Policy::PolicyClean),
                |p_output, p_cur| {
                    let p_after = p_cur.downgrade(&pf)?;
                    match p_output {
                        Ok(p_output) => {
                            if p_output.le(&p_after)? {
                                Ok(p_after)
                            } else {
                                Ok(p_output)
                            }
                        },
                        Err(e) => Err(e),
                    }
                },
            )
            .reduce(
                || Ok(Policy::PolicyClean),
                |p_output, p_cur| match (p_output, p_cur) {
                    (Ok(p_output), Ok(p_cur)) => {
                        if p_output.le(&p_cur)? {
                            Ok(p_cur)
                        } else {
                            Ok(p_output)
                        }
                    },
                    (Err(e), _) => Err(e),
                    (_, Err(e)) => Err(e),
                },
            )
    })
}

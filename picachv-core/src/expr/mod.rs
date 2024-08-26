use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, vec};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::binary_operator::Operator;
use picachv_message::{binary_operator, ArithmeticBinaryOperator, ContextOptions};
use rayon::prelude::*;
use spin::RwLock;
use uuid::Uuid;

use crate::arena::Arena;
use crate::constants::GroupByMethod;
use crate::dataframe::PolicyRef;
use crate::policy::context::ExpressionEvalContext;
use crate::policy::types::{AnyValue, ValueArrayRef};
use crate::policy::{policy_ok, BinaryTransformType, Policy, TransformType};
use crate::profiler::PROFILER;
use crate::thread_pool::THREAD_POOL;
use crate::udf::Udf;
use crate::{
    build_unary_expr, cast, policy_agg_label, policy_binary_transform_label,
    policy_unary_transform_label,
};

pub mod builder;

/// Stores the expressions.
pub type ExprArena = Arena<Expr>;

#[derive(PartialEq, Hash, Clone)]
pub enum AggExpr {
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

impl AggExpr {
    pub fn extract_expr(&self, expr_arena: &Arc<RwLock<ExprArena>>) -> PicachvResult<Arc<Expr>> {
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

/// An expression type for describing a node in the query.
#[derive(Clone, Hash, PartialEq)]
pub enum Expr {
    /// Aggregation.
    Agg {
        expr: AggExpr,
        values: Option<Vec<ValueArrayRef>>,
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
        values: Option<Vec<ValueArrayRef>>,
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
        values: Option<Vec<ValueArrayRef>>,
    },
    /// a ? b : c
    Ternary {
        cond: Uuid,
        cond_values: Option<Vec<bool>>, // should be boolean values.
        then: Uuid,
        otherwise: Uuid,
    },
}

impl Expr {
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
            _ => picachv_bail!(ComputeError: "impossible"),
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

    /// This function checks the policy enforcement for the expression type in aggregation context.
    ///
    /// `eval_expr` in `expression.v`.
    pub(crate) fn check_policy_in_group(
        &self,
        ctx: &ExpressionEvalContext,
        options: &ContextOptions,
    ) -> PicachvResult<Vec<PolicyRef>> {
        let expr_arena = ctx.arena.expr_arena.read();

        // Extract the groups as a sub-dataframe.
        let groups = ctx.gi.ok_or(PicachvError::ComputeError(
            "Group information not found, this is a fatal error.".into(),
        ))?;

        let groups = if options.enable_profiling {
            PROFILER.profile(|| ctx.df.groups(groups), "grouping".into())
        } else {
            ctx.df.groups(groups)
        }?;

        match self {
            Expr::Column(col) => {
                let col = match col {
                    ColumnIdent::ColumnId(id) => *id,
                    ColumnIdent::ColumnName(name) => picachv_bail!(
                        ComputeError: "Must reify column `{name}` into index"
                    ),
                };

                picachv_ensure!(
                    col < groups.columns.len(),
                    ComputeError: "The column index is out of bounds"
                );

                Ok(THREAD_POOL.install(|| {
                    (0..groups.shape().0)
                        .into_par_iter()
                        .map(|i| groups.columns[col][i].clone())
                        .collect::<Vec<_>>()
                }))
            },

            Expr::Apply {
                udf_desc,
                args,
                values,
            } => {
                let values = values.clone().ok_or(PicachvError::ComputeError(
                    format!("{udf_desc:?} does not have values reified.").into(),
                ))?;

                let args = args
                    .par_iter()
                    .map(|e| expr_arena.get(e))
                    .collect::<PicachvResult<Vec<_>>>()?;

                values
                    .par_iter()
                    .take(groups.shape().0)
                    .enumerate()
                    .map(|(i, value)| {
                        let mut p = Default::default();
                        for (j, arg) in args.iter().enumerate() {
                            let arg = arg.check_policy_in_row(ctx, i)?;
                            p = check_policy_binary_udf(
                                &groups.columns[j][i],
                                &arg,
                                &udf_desc.name,
                                value,
                            )?;
                        }

                        Ok(p.into())
                    })
                    .collect::<PicachvResult<Vec<_>>>()
            },

            Expr::BinaryExpr {
                left,
                op,
                right,
                values,
            } => {
                let values = values.clone().ok_or(PicachvError::ComputeError(
                    format!("{self:?} does not have values reified.").into(),
                ))?;

                let left = expr_arena.get(left)?;
                let right = expr_arena.get(right)?;

                THREAD_POOL.install(|| {
                    values
                        .par_iter()
                        .take(groups.shape().0)
                        .enumerate()
                        .map(|(i, value)| {
                            let (lhs, rhs) = rayon::join(
                                || left.check_policy_in_row(ctx, i),
                                || right.check_policy_in_row(ctx, i),
                            );
                            let (lhs, rhs) = (lhs?, rhs?);

                            Ok(Arc::new(check_policy_binary(&lhs, &rhs, op, value)?))
                        })
                        .collect::<PicachvResult<Vec<_>>>()
                })
            },

            _ => unimplemented!("{self:?} is not yet supported in aggregation context."),
        }
    }

    /// This function checks the policy enforcement for the expression type (not within aggregation!).
    ///
    /// The formalized part is described in `pcd-proof/theories/expression.v`.
    /// Note that since the check occurs at the tuple level!
    pub(crate) fn check_policy_in_row(
        &self,
        ctx: &ExpressionEvalContext,
        idx: usize,
    ) -> PicachvResult<PolicyRef> {
        match ctx.lookup(self, idx) {
            Some(p) => Ok(p),
            None => {
                let expr_arena = ctx.arena.expr_arena.read();

                let p = match self {
                    // A literal expression is always allowed because it does not
                    // contain any sensitive information.
                    Expr::Literal => Ok(Default::default()),
                    // Deal with the UDF case.
                    Expr::Apply {
                        udf_desc: Udf { name },
                        args,
                        values, // todo.
                    } => match values {
                        Some(values) => Ok(Arc::new(check_policy_in_row_apply(
                            ctx, name, args, values, idx,
                        )?)),
                        None => {
                            picachv_bail!(ComputeError: "The values are not reified for {self:?}")
                        },
                    },

                    Expr::BinaryExpr {
                        left,
                        right,
                        op,
                        values,
                    } => {
                        let left = expr_arena.get(left)?;
                        let right = expr_arena.get(right)?;

                        let lhs = || left.check_policy_in_row(ctx, idx);
                        let rhs = || right.check_policy_in_row(ctx, idx);
                        let (lhs, rhs) = THREAD_POOL.install(|| rayon::join(lhs, rhs));
                        let (lhs, rhs) = (lhs?, rhs?);

                        if matches!(
                            op,
                            binary_operator::Operator::ComparisonOperator(_)
                                | binary_operator::Operator::LogicalOperator(_)
                        ) {
                            return Ok(Arc::new(lhs.join(&rhs)?));
                        }

                        let values = values.as_ref().ok_or(PicachvError::ComputeError(
                            format!("The values are not reified for {self:?}").into(),
                        ))?;

                        picachv_ensure!(
                            !values.is_empty() && values[0].len() == 2,
                            InvalidOperation: "The argument to the binary expression is incorrect"
                        );

                        Ok(Arc::new(check_policy_binary(&lhs, &rhs, op, &values[idx])?))
                    },
                    // This is truly interesting.
                    //
                    // See `eval_unary_expression_in_cell`.
                    Expr::UnaryExpr { arg, op } => {
                        let arg = expr_arena.get(arg)?;
                        let policy = arg.check_policy_in_row(ctx, idx)?;
                        Ok(Arc::new(
                            policy.downgrade(&Arc::new(build_unary_expr!(op.clone())))?,
                        ))
                    },
                    Expr::Column(col) => {
                        let col = match col {
                            ColumnIdent::ColumnId(id) => *id,
                            ColumnIdent::ColumnName(name) => picachv_bail!(
                                ComputeError: "Must reify column `{name}` into index"
                            ),
                        };

                        // For column expression this is an interesting undecidable case
                        // where we cannot determine what operation it will be applied.
                        //
                        // We neverthelss approve this operation per evaluation semantics.
                        //
                        // See `EvalColumnNotAgg` in `expression.v`.
                        Ok(ctx.df.row(idx)?[col].clone())
                    },
                    Expr::Alias { expr, .. } => {
                        let expr = expr_arena.get(expr)?;
                        expr.check_policy_in_row(ctx, idx)
                    },
                    Expr::Filter { input, filter } => {
                        let input = expr_arena.get(input)?;
                        let filter = expr_arena.get(filter)?;
                        input.check_policy_in_row(ctx, idx)?;
                        filter.check_policy_in_row(ctx, idx)
                    },
                    Expr::Agg { .. } => Err(PicachvError::ComputeError(
                        "Aggregation expression is not allowed in row context.".into(),
                    )),
                    Expr::Ternary {
                        cond_values,
                        then,
                        otherwise,
                        ..
                    } => {
                        picachv_ensure!(
                            cond_values.is_some(),
                            ComputeError: "The condition values are not reified"
                        );

                        let then = expr_arena.get(then)?;
                        let otherwise = expr_arena.get(otherwise)?;

                        let cond_values =
                            cond_values.as_ref().ok_or(PicachvError::ComputeError(
                                "The condition values are not reified".into(),
                            ))?;

                        let cond = if cond_values.len() == 1 {
                            cond_values[0]
                        } else if idx >= cond_values.len() {
                            false
                        } else {
                            cond_values[idx]
                        };
                        let then = || then.check_policy_in_row(ctx, idx);
                        let otherwise = || otherwise.check_policy_in_row(ctx, idx);
                        let (then, otherwise) =
                            THREAD_POOL.install(|| rayon::join(then, otherwise));
                        let (then, otherwise) = (then?, otherwise?);

                        Ok(if cond { then } else { otherwise })
                    },

                    // todo.
                    _ => Ok(Default::default()),
                }?;

                ctx.update_cache(self, idx, p.clone());

                Ok(p)
            },
        }
    }

    pub fn reify(&mut self, values: RecordBatch) -> PicachvResult<()> {
        let values_mut = match self {
            Expr::Apply { values, .. }
            | Expr::BinaryExpr { values, .. }
            | Expr::Agg { values, .. } => values,
            _ => picachv_bail!(ComputeError: "The expression does not need reification."),
        };

        // Convert the RecordBatch into a vector of AnyValue.
        let values = convert_record_batch(values)?;
        values_mut.replace(values);

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

impl fmt::Debug for AggExpr {
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

impl fmt::Debug for Expr {
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

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The checking logic seems identical to the one in `expr.rs`.
/// FIXME: Optimize?
fn check_policy_binary(
    lhs: &Policy,
    rhs: &Policy,
    op: &Operator,
    value: &ValueArrayRef,
) -> PicachvResult<Policy> {
    match op {
        binary_operator::Operator::ComparisonOperator(_)
        | binary_operator::Operator::LogicalOperator(_) => lhs.join(rhs),
        binary_operator::Operator::ArithmeticOperator(op) => {
            let op = ArithmeticBinaryOperator::try_from(*op).map_err(|_| {
                PicachvError::ComputeError(
                    "Cannot convert i32 into `ArithmeticBinaryOperator`.".into(),
                )
            })?;

            let mut op = BinaryTransformType::try_from(op)?;

            match (policy_ok(lhs), policy_ok(rhs)) {
                (true, true) => Ok(Policy::PolicyClean),
                // lhs = ∎
                (true, _) => {
                    op.arg = value[0].clone();
                    let p_f = policy_binary_transform_label!(TransformType::Binary(op));

                    // Check if we can downgrade.
                    rhs.downgrade(&Arc::new(p_f))
                },

                // rhs = ∎
                (_, true) => {
                    op.arg = value[1].clone();
                    let p_f = policy_binary_transform_label!(TransformType::Binary(op));

                    // Check if we can downgrade.
                    lhs.downgrade(&Arc::new(p_f))
                },

                _ => lhs.join(rhs),
            }
        },
    }
}

fn check_policy_binary_udf(
    lhs: &PolicyRef,
    rhs: &PolicyRef,
    udf_name: &str,
    values: &ValueArrayRef,
) -> PicachvResult<Policy> {
    picachv_ensure!(
        values.len() == 2,
        ComputeError: "Checking policy for UDF requires two values."
    );

    tracing::debug!(
        "lhs = {:?}, rhs = {:?}, udf_name = {:?}",
        lhs,
        rhs,
        udf_name
    );

    match (policy_ok(lhs), policy_ok(rhs)) {
        (true, true) => Ok(Policy::PolicyClean),
        // lhs = ∎
        (true, _) => {
            let lhs_value = match udf_name {
                "dt.offset_by" => cast::into_duration(&values[0]),
                "+" => cast::into_i64(&values[0]),
                _ => unimplemented!("{udf_name} is not yet supported."),
            }?;

            let pf = policy_binary_transform_label!(udf_name.to_string(), lhs_value);
            rhs.downgrade(&Arc::new(pf))
        },
        // rhs = ∎
        (_, true) => {
            let rhs_value = match udf_name {
                "dt.offset_by" => cast::into_duration(&values[1]),
                "+" => cast::into_i64(&values[1]),
                _ => unimplemented!("{udf_name} is not yet supported."),
            }?;

            let pf = policy_binary_transform_label!(udf_name.to_string(), rhs_value);
            lhs.downgrade(&Arc::new(pf))
        },

        _ => lhs.join(rhs),
    }
}

fn check_policy_unary_udf(arg: &PolicyRef, udf_name: &str) -> PicachvResult<Policy> {
    match policy_ok(arg) {
        true => Ok(Policy::PolicyClean),
        false => {
            let pf = policy_unary_transform_label!(udf_name.to_string());
            arg.downgrade(&Arc::new(pf))
        },
    }
}

/// This function handles the UDF case.
fn check_policy_in_row_apply(
    ctx: &ExpressionEvalContext,
    udf_name: &str,
    args: &[Uuid],
    values: &[ValueArrayRef],
    idx: usize,
) -> PicachvResult<Policy> {
    let args = {
        let expr_arena = ctx.arena.expr_arena.read();
        args.par_iter()
            .map(|e| expr_arena.get(e).cloned())
            .collect::<PicachvResult<Vec<_>>>()?
    };

    match args.len() {
        // Because a UDF does not have any argument, it is safe since it does not de-
        // pend on any sensitive information.
        //
        // There is also no closure in relational algebra, so we do not need to worry
        // about the closure that may have its own context.
        0 => Ok(Default::default()),
        // The unary case.
        1 => {
            let arg = args[0].check_policy_in_row(ctx, idx)?;

            check_policy_unary_udf(&arg, udf_name)
        },
        // The binary case.
        2 => {
            let lhs = || args[0].check_policy_in_row(ctx, idx);
            let rhs = || args[1].check_policy_in_row(ctx, idx);

            let (lhs, rhs) = THREAD_POOL.install(|| rayon::join(lhs, rhs));
            let (lhs, rhs) = (lhs?, rhs?);

            match idx >= values.len() {
                true => Ok(Default::default()),
                false => check_policy_binary_udf(&lhs, &rhs, udf_name, &values[idx]),
            }
        },
        _ => Err(PicachvError::Unimplemented("UDF not implemented.".into())),
    }
}

/// This functons folds the policies on the groups to check this operation is allowed.
///
/// See `eval_agg` in `expression.v`.
pub(crate) fn fold_on_groups(groups: &[PolicyRef], how: GroupByMethod) -> PicachvResult<Policy> {
    // Construct the operator.
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

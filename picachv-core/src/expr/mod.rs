use std::fmt;
use std::time::Duration;

use arrow_array::{Array, Date32Array, Int32Array, LargeStringArray, RecordBatch};
use arrow_schema::DataType;
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::binary_operator;

use crate::arena::Arena;
use crate::policy::context::ExpressionEvalContext;
use crate::policy::types::AnyValue;
use crate::policy::{
    policy_ok, BinaryTransformType, Policy, PolicyLabel, TransformOps, TransformType,
};
use crate::udf::Udf;
use crate::{build_unary_expr, cast, policy_binary_transform_label};

pub mod builder;

/// Stores the expressions.
pub type ExprArena = Arena<Expr>;

#[derive(PartialEq, Clone)]
pub enum AggExpr {
    Min {
        input: Box<Expr>,
        propagate_nans: bool,
    },
    Max {
        input: Box<Expr>,
        propagate_nans: bool,
    },
    Median(Box<Expr>),
    NUnique(Box<Expr>),
    First(Box<Expr>),
    Last(Box<Expr>),
    Mean(Box<Expr>),
    Implode(Box<Expr>),
    // include_nulls
    Count(Box<Expr>, bool),
    Quantile {
        expr: Box<Expr>,
        quantile: Box<Expr>,
    },
    Sum(Box<Expr>),
    AggGroups(Box<Expr>),
    Std(Box<Expr>, u8),
    Var(Box<Expr>, u8),
}

/// An expression type for describing a node in the query.
#[derive(Clone, PartialEq)]
pub enum Expr {
    /// Aggregation.
    Agg(AggExpr),
    /// Select a column.
    Column(String),
    /// Count expression.
    Count,
    /// Making alias.
    Alias {
        expr: Box<Expr>,
        name: String,
    },
    /// "*".
    Wildcard,
    /// Filter.
    Filter {
        input: Box<Expr>,
        filter: Box<Expr>,
    },
    /// Binary operations
    BinaryExpr {
        left: Box<Expr>,
        op: binary_operator::Operator,
        right: Box<Expr>,
        values: Option<Vec<Vec<AnyValue>>>,
    },
    UnaryExpr {
        arg: Box<Expr>,
        op: TransformType,
    },
    Literal,
    /// User-defined function.
    Apply {
        udf_desc: Udf,
        // This only takes one argument.
        args: Vec<Box<Expr>>,
        // A type-erased array of values reified by the caller.
        values: Option<Vec<Vec<AnyValue>>>,
    },
}

impl Expr {
    pub fn needs_reify(&self) -> bool {
        matches!(self, Self::Apply { .. })
    }

    pub(crate) fn check_policy_within_agg(
        &self,
        ctx: &mut ExpressionEvalContext,
    ) -> PicachvResult<Policy<PolicyLabel>> {
        picachv_ensure!(
            ctx.in_agg,
            ComputeError: "The expression is not in an aggregation context."
        );
        match self {
            Expr::Agg(_) => Err(PicachvError::ComputeError(
                "Aggregation within aggregation is not allowed.".into(),
            )),

            _ => todo!(),
        }
    }

    /// This function checks the policy enforcement for the expression type.
    ///
    /// The formalized part is described in `pcd-proof/theories/expression.v`.
    /// Note that since the check occurs at the tuple level!
    pub(crate) fn check_policy_in_row(
        &self,
        ctx: &mut ExpressionEvalContext,
        idx: usize,
    ) -> PicachvResult<Policy<PolicyLabel>> {
        if !ctx.in_agg {
            match self {
                // A literal expression is always allowed because it does not
                // contain any sensitive information.
                Expr::Literal => Ok(Default::default()),
                // Deal with the UDF case.
                Expr::Apply {
                    udf_desc: Udf { name },
                    args,
                    values, // todo.
                } => match values {
                    Some(values) => check_policy_in_row_apply(ctx, name, args, values, idx),
                    None => panic!("The values are not reified."),
                },

                Expr::BinaryExpr {
                    left, right, op, ..
                } => {
                    let lhs = left.check_policy_in_row(ctx, idx)?;
                    let rhs = right.check_policy_in_row(ctx, idx)?;

                    // These operators occur only in predicates. Skip them.
                    if matches!(
                        op,
                        binary_operator::Operator::ComparisonOperator(_)
                            | binary_operator::Operator::LogicalOperator(_)
                    ) {
                        return lhs.join(&rhs);
                    }

                    match (policy_ok(&lhs), policy_ok(&rhs)) {
                        // lhs = ∎
                        (true, _) => {
                            todo!()
                        },

                        // rhs = ∎
                        (_, true) => {
                            todo!()
                        },

                        _ => lhs.join(&rhs),
                    }
                },
                // This is truly interesting.
                //
                // See `eval_unary_expression_in_cell`.
                Expr::UnaryExpr { arg, op } => {
                    let policy = arg.check_policy_in_row(ctx, idx)?;
                    policy.downgrade(build_unary_expr!(op.clone()))
                },
                Expr::Column(idx) => {
                    let idx = ctx.schema.iter().position(|e| e == idx).ok_or(
                        PicachvError::ComputeError("The column does not exist.".into()),
                    )?;

                    log::debug!("selected column: {}", ctx.current_row[idx]);

                    // For column expression this is an interesting undecidable case
                    // where we cannot determine what operation it will be applied.
                    //
                    // We neverthelss approve this operation per evaluation semantics.
                    //
                    // See `EvalColumnNotAgg` in `expression.v`.
                    Ok(ctx.current_row[idx].clone())
                },
                Expr::Alias { expr, .. } => expr.check_policy_in_row(ctx, idx),
                Expr::Filter { input, filter } => {
                    input.check_policy_in_row(ctx, idx)?;
                    filter.check_policy_in_row(ctx, idx)
                },
                Expr::Agg(agg_expr) => todo!(),
                // todo.
                _ => Ok(Default::default()),
            }
        } else {
            self.check_policy_within_agg(ctx)
        }
    }

    pub fn reify(&mut self, values: RecordBatch) -> PicachvResult<()> {
        log::debug!("reifying {values:?}");

        let values_mut = match self {
            Expr::Apply { values, .. } | Expr::BinaryExpr { values, .. } => values,
            _ => picachv_bail!(ComputeError: "The expression does not need reification."),
        };

        // Convert the RecordBatch into a vector of AnyValue.
        let values = convert_record_batch(values)?;
        values_mut.replace(values);

        Ok(())
    }
}

fn convert_record_batch(rb: RecordBatch) -> PicachvResult<Vec<Vec<AnyValue>>> {
    let mut rows = vec![];
    let columns = rb.columns();

    if columns.is_empty() {
        return Ok(rows);
    }

    // Iterate over the rows.
    for i in 0..rb.num_rows() {
        let mut row = vec![];

        // Iterate each column and convert it into AnyValue.
        for j in 0..columns.len() {
            match columns[j].data_type() {
                // TODO: We can wrap this into a macro?
                DataType::Int32 => {
                    let array = columns[j].as_any().downcast_ref::<Int32Array>().unwrap();
                    let value = array.value(i);
                    row.push(AnyValue::Int32(value));
                },
                DataType::Date32 => {
                    let array = columns[j].as_any().downcast_ref::<Date32Array>().unwrap();
                    let value = array.value(i);
                    row.push(AnyValue::Duration(Duration::from_secs(value as _)));
                },
                DataType::LargeUtf8 => {
                    let array = columns[j]
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap();
                    let value = array.value(i);
                    row.push(AnyValue::String(value.to_string()));
                },
                ty => picachv_bail!(InvalidOperation: "{ty} is not supported."),
            }
        }

        rows.push(row);
    }

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
            Self::Agg(agg) => write!(f, "{agg:?}"),
            Self::Column(column) => write!(f, "col({column})"),
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
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl ExprArena {}

fn check_policy_binary(
    lhs: &Policy<PolicyLabel>,
    rhs: &Policy<PolicyLabel>,
    op: BinaryTransformType,
) -> PicachvResult<Policy<PolicyLabel>> {
    todo!()
}

fn check_policy_binary_udf(
    lhs: Policy<PolicyLabel>,
    rhs: Policy<PolicyLabel>,
    udf_name: &str,
    values: &[AnyValue],
) -> PicachvResult<Policy<PolicyLabel>> {
    picachv_ensure!(
        values.len() == 2,
        ComputeError: "Checking policy for UDF requires two values."
    );

    log::debug!("lhs = {:?}, rhs = {:?}, udf_name = {:?}", lhs, rhs, udf_name);

    match (policy_ok(&lhs), policy_ok(&rhs)) {
        // lhs = ∎
        (true, _) => {
            todo!()
        },

        // rhs = ∎
        (_, true) => match udf_name {
            "dt.offset_by" => {
                // We now construct the downgrading operation.
                let rhs_value = cast::into_duration(values[1].clone())?;
                let pf = policy_binary_transform_label!(udf_name.to_string(), rhs_value);

                // Check if we can downgrade.
                lhs.downgrade(pf)
            },

            _ => todo!(),
        },

        _ => lhs.join(&rhs),
    }
}

/// This function handles the UDF case.
fn check_policy_in_row_apply(
    ctx: &mut ExpressionEvalContext,
    udf_name: &str,
    args: &[Box<Expr>],
    values: &[Vec<AnyValue>],
    idx: usize,
) -> PicachvResult<Policy<PolicyLabel>> {
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

            todo!()
        },
        // The binary case.
        2 => {
            let lhs = args[0].check_policy_in_row(ctx, idx)?;
            let rhs = args[1].check_policy_in_row(ctx, idx)?;

            check_policy_binary_udf(lhs, rhs, udf_name, &values[idx])
        },
        _ => Err(PicachvError::Unimplemented("UDF not implemented.".into())),
    }
}

use std::sync::Arc;

use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::binary_operator::Operator;
use picachv_message::{binary_operator, ArithmeticBinaryOperator, ContextOptions};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use super::pexpr::PExpr;
use super::{ColumnIdent, ExpressionEvalContext};
use crate::dataframe::PolicyRef;
use crate::policy::types::ValueArrayRef;
use crate::policy::{policy_ok, BinaryTransformType, Policy, TransformType};
use crate::profiler::PROFILER;
use crate::thread_pool::THREAD_POOL;
use crate::udf::Udf;
use crate::{build_unary_expr, cast, policy_binary_transform_label, policy_unary_transform_label};

impl PExpr {
    /// This function checks the policy enforcement for the expression type in aggregation context.
    pub fn check_policy_in_group(
        &self,
        ctx: &ExpressionEvalContext,
        options: &ContextOptions,
    ) -> PicachvResult<Vec<PolicyRef>> {
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
            PExpr::Column(col) => {
                let col = match col {
                    ColumnIdent::ColumnId(id) => *id,
                    ColumnIdent::ColumnName(name) => picachv_bail!(
                        ComputeError: "Must reify column `{name}` into index"
                    ),
                };

                Ok(THREAD_POOL.install(|| {
                    (0..groups.shape().0)
                        .into_par_iter()
                        .map(|i| groups.columns[col][i].clone())
                        .collect::<Vec<_>>()
                }))
            },

            PExpr::Apply {
                udf_desc,
                args,
                values,
            } => {
                let values = values.clone().ok_or(PicachvError::ComputeError(
                    format!("{udf_desc:?} does not have values reified.").into(),
                ))?;

                THREAD_POOL.install(|| {
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
                })
            },

            PExpr::BinaryExpr {
                left,
                op,
                right,
                values,
            } => {
                let values = values.as_ref().ok_or(PicachvError::ComputeError(
                    format!("does not have values reified.").into(),
                ))?;

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

            _ => unimplemented!(" not yet supported in aggregation context."),
        }
    }

    /// This function checks the policy enforcement for the expression type (not within aggregation!).
    ///
    /// The formalized part is described in `pcd-proof/theories/expression.v`.
    /// Note that since the check occurs at the tuple level!
    pub fn check_policy_in_row(
        &self,
        ctx: &ExpressionEvalContext,
        idx: usize,
    ) -> PicachvResult<PolicyRef> {
        match self {
            // A literal expression is always allowed because it does not
            // contain any sensitive information.
            PExpr::Literal => Ok(Default::default()),
            // Deal with the UDF case.
            PExpr::Apply {
                udf_desc: Udf { name },
                args,
                values,
            } => match values {
                Some(values) => Ok(Arc::new(check_policy_in_row_apply(
                    ctx, name, args, values, idx,
                )?)),
                None => {
                    picachv_bail!(ComputeError: "The values are not reified")
                },
            },

            PExpr::BinaryExpr {
                left,
                right,
                op,
                values,
            } => {
                let (lhs, rhs) = THREAD_POOL.install(|| {
                    rayon::join(
                        || left.check_policy_in_row(ctx, idx),
                        || right.check_policy_in_row(ctx, idx),
                    )
                });
                let (lhs, rhs) = (lhs?, rhs?);

                if matches!(
                    op,
                    binary_operator::Operator::ComparisonOperator(_)
                        | binary_operator::Operator::LogicalOperator(_)
                ) {
                    return Ok(Arc::new(lhs.join(&rhs)?));
                }

                let values = values.as_ref().ok_or(PicachvError::ComputeError(
                    format!("The values are not reified for").into(),
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
            PExpr::UnaryExpr { arg, op } => {
                let policy = arg.check_policy_in_row(ctx, idx)?;
                Ok(Arc::new(
                    policy.downgrade(&Arc::new(build_unary_expr!(op.clone())))?,
                ))
            },
            PExpr::Column(col) => {
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
            PExpr::Alias { expr, .. } => expr.check_policy_in_row(ctx, idx),
            PExpr::Filter { input, filter } => {
                input.check_policy_in_row(ctx, idx)?;
                filter.check_policy_in_row(ctx, idx)
            },
            PExpr::Agg { .. } => Err(PicachvError::ComputeError(
                "Aggregation expression is not allowed in row context.".into(),
            )),
            PExpr::Ternary {
                cond_values,
                then,
                otherwise,
                ..
            } => {
                picachv_ensure!(
                    cond_values.is_some(),
                    ComputeError: "The condition values are not reified"
                );

                let cond_values = cond_values.as_ref().ok_or(PicachvError::ComputeError(
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
                let (then, otherwise) = THREAD_POOL.install(|| rayon::join(then, otherwise));
                let (then, otherwise) = (then?, otherwise?);

                Ok(if cond { then } else { otherwise })
            },

            _ => Ok(Default::default()),
        }
    }
}

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

/// This function handles the UDF case.
fn check_policy_in_row_apply(
    ctx: &ExpressionEvalContext,
    udf_name: &str,
    args: &[Arc<PExpr>],
    values: &[ValueArrayRef],
    idx: usize,
) -> PicachvResult<Policy> {
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

fn check_policy_unary_udf(arg: &PolicyRef, udf_name: &str) -> PicachvResult<Policy> {
    match policy_ok(arg) {
        true => Ok(Policy::PolicyClean),
        false => {
            let pf = policy_unary_transform_label!(udf_name.to_string());
            arg.downgrade(&Arc::new(pf))
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

    #[cfg(feature = "trace")]
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

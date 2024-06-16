pub mod builder;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use picachv_error::{picachv_bail, picachv_ensure, PicachvResult};
use picachv_message::group_by_idx::Groups;
use picachv_message::group_by_proxy::GroupBy;
use picachv_message::{GroupByIdx, GroupByProxy, GroupBySlice};
use uuid::Uuid;

use crate::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
use crate::expr::{fold_on_groups, Expr};
use crate::policy::context::ExpressionEvalContext;
use crate::policy::{Policy, PolicyLabel};
use crate::udf::Udf;
use crate::{rwlock_unlock, Arenas};

/// This struct describes a physical plan that the caller wants to perform on the
/// raw data. We do not use the [`LogicalPlan`] shipped with polars because it contains too
/// many unnecessary operations.
///
/// Also one thing different from the original implementation is that we only consider
/// "operators"  that involve the expressions because operators without expressions are not
/// checked but must be applied at the epilogue.
///
/// # Note
///
/// - We only consider common and generic logical plans in this enum type and avoid adding
///   too implementation- or architecture-specific operations.
/// - We check if the physical plan conforms to the prescribed privacy policy. It is recommended
/// to give the checker the *optimized* plan.
/// - In fact the boxed plan do not need to be recursively checked since the caller will call
///   the `execute_prologue` function for each plan node on its side. We keep it here for the
///   purpose of debugging.
/// - We store UUIDs of each expression in the plan. This is because plans do not "own" expressions
///   since we may need to update the expressions in the plan.
#[derive(Clone)]
pub enum Plan {
    /// Select with *filter conditions* that work on a [`Plan`].
    Select { predicate: Uuid },

    /// Projection
    Projection {
        /// Column 'names' as we may apply some transformation on columns.
        expressions: Vec<Uuid>,
    },

    /// Aggregate and group by
    Aggregation {
        /// Group by `keys`.
        keys: Vec<Uuid>,
        aggs: Vec<Uuid>,
        // apply: Option<Arc<dyn UserDefinedFunction>>,
        maintain_order: bool,
        // An auxiliary data structure telling the monitor how data should be aggregated.
        gb_proxy: GroupByProxy,
        output_schema: Vec<String>,
    },

    DataFrameScan {
        schema: Vec<String>,
        projection: Option<Vec<String>>,
        selection: Option<Uuid>,
    },

    /// Horizontal stack: this is a special operation that is used to stack multiple
    /// dataframes horizontally. The dataframe being appended is evaluated using the
    /// expressions here.
    Hstack {
        cse_expressions: Vec<Uuid>,
        expressions: Vec<Uuid>,
    },
}

impl fmt::Debug for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format(f, 0)
    }
}

impl Plan {
    /// Formats the current physical plan according to the given `indent`.
    pub(crate) fn format(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        match self {
            Self::Select { predicate, .. } => {
                write!(f, "{:indent$}FILTER {predicate:?} FROM", "")
            },
            Self::Projection {
                expressions: expression,
                ..
            } => {
                write!(f, "{:indent$} SELECT {expression:?} FROM", "")
            },
            Self::DataFrameScan {
                schema,
                projection,
                selection,
                ..
            } => {
                let total_columns = schema.len();
                let mut n_columns = "*".to_string();
                if let Some(columns) = projection {
                    n_columns = format!("{}", columns.len());
                }
                let selection = match selection {
                    Some(s) => Cow::Owned(format!("{s:?}")),
                    None => Cow::Borrowed("None"),
                };
                write!(
                    f,
                    "{:indent$}DF {:?}; PROJECT {}/{} COLUMNS; SELECTION: {:?}",
                    "",
                    schema.iter().take(4).collect::<Vec<_>>(),
                    n_columns,
                    total_columns,
                    selection,
                )
            },
            Self::Aggregation { keys, aggs, .. } => {
                write!(f, "{:indent$}AGGREGATE", "")?;
                write!(f, "\n{:indent$}\t{aggs:?} GROUP BY {keys:?} FROM", "")
            },
            Self::Hstack {
                cse_expressions,
                expressions,
            } => {
                write!(
                    f,
                    "{:indent$}HSTACK 
                {cse_expressions:?} + {expressions:?} FROM",
                    ""
                )
            },
        }
    }

    /// This is the function eventually called to check if a physical operator is
    /// allowed to be executed. The caller is required to call this function *after*
    /// executing the physical plan as though it was instrumented. This functios
    /// returns the UUID (possibly updated) of the active dataframe that is being
    /// processed.
    ///
    /// The argument `active_df_uuid` is the UUID of the active dataframe that is being
    /// processed. This is used to check if the current physical plan is allowed to be
    /// executed because expressions are evalauted at the tuple level.
    ///
    /// # Example
    ///
    /// Consider for example you have the following C++ code snippet.
    ///
    /// ```c++
    /// int main(int argc, const char **argv) {
    ///     State state;
    ///     Plan *const plan = build_plan();
    ///     void *res = plan->execute(&state);
    /// }
    /// ```
    ///
    /// where
    ///
    /// ```c++
    /// void *Plan::execute(State *state) const {
    ///     Result *res = this->execute_impl(state);
    ///
    ///     if (!this->check(state, this->active_df_uuid)) {
    ///         return nullptr;
    ///     } else {
    ///         return (void*)res;
    ///     }
    /// }
    /// ```
    pub fn check_executor(
        &self,
        arena: &Arenas,
        active_df_uuid: Uuid,
        udfs: &HashMap<String, Udf>,
    ) -> PicachvResult<Uuid> {
        tracing::debug!(
            "execute_prologue: checking {:?} with {active_df_uuid}",
            self
        );

        match self {
            // See the semantics for `apply_proj_in_relation`.
            Plan::Projection {
                expressions: expression,
                ..
            } => {
                let expr_arena = rwlock_unlock!(arena.expr_arena, read);
                let expression = expression
                    .into_iter()
                    .map(|e| expr_arena.get(e).cloned())
                    .collect::<PicachvResult<Vec<_>>>()?;

                check_expressions(arena, active_df_uuid, &expression, false, udfs)
            },
            Plan::Select { predicate, .. } => {
                let predicate = {
                    let expr_arena = rwlock_unlock!(arena.expr_arena, read);
                    expr_arena.get(predicate)?.clone()
                };

                check_expressions(arena, active_df_uuid, &[predicate], true, udfs)?;

                Ok(active_df_uuid)
            },
            Plan::DataFrameScan {
                selection,
                projection,
                ..
            } => {
                let projected_uuid = match projection {
                    Some(projection) => early_projection(arena, active_df_uuid, &projection),
                    None => Ok(active_df_uuid),
                }?;

                match selection {
                    Some(s) => {
                        let expr = {
                            let expr_arena = rwlock_unlock!(arena.expr_arena, read);
                            expr_arena.get(s)?.clone()
                        };

                        check_expressions(arena, projected_uuid, &[expr], true, udfs)?;

                        Ok(projected_uuid)
                    },
                    None => Ok(projected_uuid),
                }
            },

            Plan::Aggregation {
                keys,
                aggs,
                gb_proxy,
                output_schema,
                ..
            } => {
                let expr_arena = rwlock_unlock!(arena.expr_arena, read);
                let keys = keys
                    .into_iter()
                    .map(|e| expr_arena.get(e).cloned())
                    .collect::<PicachvResult<Vec<_>>>()?;
                let aggs = aggs
                    .into_iter()
                    .map(|e| expr_arena.get(e).cloned())
                    .collect::<PicachvResult<Vec<_>>>()?;

                // There are two steps for the check:
                //
                // 1. We need to evaluate the group by things to make sure that the group by
                //    does not violate any security policies.
                // 2. We also need to "fold" on the group according to `gb_proxy` to make sure
                //    that the aggregation does not violate any security policies.
                //
                // See the semantics for `eval_aggregate` as well as `eval_groupby_having` and
                // `apply_fold_in_groups` in `semantics.v`.
                let first_part = {
                    let df_arena = rwlock_unlock!(arena.df_arena, read);
                    let df = df_arena.get(&active_df_uuid)?;
                    let df = do_check_expressions(arena, df, &keys, udfs)?;

                    aggregate_keys(&df, gb_proxy)
                }?;
                // This is in fact `apply_fold_on_groups`, but for the sake of naming consistency
                // we use `check_expressions_agg`.
                let second_part =
                    check_expressions_agg(arena, active_df_uuid, &aggs, gb_proxy, udfs)?;

                // Combine the two parts.
                let mut df_arena = rwlock_unlock!(arena.df_arena, write);
                let second_part = df_arena.get(&second_part)?;
                let mut new_df = PolicyGuardedDataFrame::stitch(&first_part, second_part)?;
                new_df.schema = output_schema.clone();

                df_arena.insert(new_df)
            },

            Plan::Hstack {
                cse_expressions,
                expressions,
            } => do_hstack(arena, active_df_uuid, cse_expressions, expressions, udfs),
        }
    }
}

/// Do the horizontal stack operation.
///
/// A horizontal stack operation is used to append new columns directly to the existing dataframe
/// without any join operation. There might be common subexpressions in the expressions that are
/// evaluated before the actual expressions are evaluated. We split it into two parts: we first
/// append the common subexpressions to the dataframe and then we evaluate the actual expressions.
/// The final result is the dataframe with the actual expressions appended.
#[tracing::instrument]
fn do_hstack(
    arena: &Arenas,
    active_df_uuid: Uuid,
    cse_expressions: &[Uuid],
    expressions: &[Uuid],
    udfs: &HashMap<String, Udf>,
) -> PicachvResult<Uuid> {
    let mut df_arena = rwlock_unlock!(arena.df_arena, write);
    let expr_arena = rwlock_unlock!(arena.expr_arena, read);
    let df = df_arena.get_mut(&active_df_uuid)?;

    println!("do_stack: {df}");

    let cse_expressions = cse_expressions
        .into_iter()
        .map(|e| expr_arena.get(e).cloned())
        .collect::<PicachvResult<Vec<_>>>()?;
    let expressions = expressions
        .into_iter()
        .map(|e| expr_arena.get(e).cloned())
        .collect::<PicachvResult<Vec<_>>>()?;

    let new_df = if cse_expressions.is_empty() {
        do_check_expressions(arena, df, &expressions, udfs)?
    } else {
        // First let us collect the common subexpression part.
        let cse_part = do_check_expressions(arena, df, &cse_expressions, udfs)?;
        // We then stitch the common subexpression part with the dataframe.
        let cse_part = PolicyGuardedDataFrame::stitch(df, &cse_part)?;
        // We then evaluate the actual expressions.
        do_check_expressions(arena, &cse_part, &expressions, udfs)?
    };

    // We then add new columns.
    let new_df = PolicyGuardedDataFrame::stitch(&df, &new_df)?;

    match Arc::get_mut(df) {
        Some(df) => {
            *df = new_df;
            Ok(active_df_uuid)
        },
        None => df_arena.insert_arc(Arc::new(new_df)),
    }
}

fn convert_slice_to_idx(slice: &GroupBySlice) -> PicachvResult<GroupByIdx> {
    let mut group_map = HashMap::<u64, Vec<u64>>::new();

    for (idx, group) in slice.groups.iter().enumerate() {
        // check if the group map contains this group.
        match group_map.get_mut(group) {
            Some(v) => v.push(idx as _),
            None => {
                group_map.insert(*group, vec![idx as _]);
            },
        }
    }

    Ok(GroupByIdx {
        groups: group_map
            .into_iter()
            .map(|e| Groups {
                first: e.0,
                group: e.1,
            })
            .collect(),
    })
}

fn aggregate_keys(
    df: &PolicyGuardedDataFrame,
    gb_proxy: &GroupByProxy,
) -> PicachvResult<PolicyGuardedDataFrame> {
    tracing::debug!("aggregate_keys: df = {df:?}, gb_proxy = {gb_proxy:?}");

    let idx = match gb_proxy.group_by.as_ref() {
        Some(gb) => match gb {
            GroupBy::GroupByIdx(idx) => idx.clone(),
            GroupBy::GroupBySlice(slice) => convert_slice_to_idx(slice)?,
        },
        None => picachv_bail!(ComputeError: "The group by is empty."),
    };

    aggregate_keys_idx(df, &idx)
}

fn aggregate_keys_idx(
    df: &PolicyGuardedDataFrame,
    gb_proxy: &GroupByIdx,
) -> PicachvResult<PolicyGuardedDataFrame> {
    let mut columns = vec![];

    for col_idx in 0..df.shape().1 {
        let mut cur = vec![];
        for group in gb_proxy.groups.iter() {
            let mut policy = Policy::PolicyClean;

            for idx in group.group.iter() {
                let p = df.columns[col_idx].policies[*idx as usize].clone();
                policy = policy.join(&p)?;
            }

            cur.push(policy);
        }
        columns.push(PolicyGuardedColumn { policies: cur });
    }

    Ok(PolicyGuardedDataFrame {
        schema: df.schema.clone(),
        columns,
    })
}

/// This function is used to apply the projection on the dataframe.
pub fn early_projection(
    df_arena: &Arenas,
    active_df_uuid: Uuid,
    project_list: &[String],
) -> PicachvResult<Uuid> {
    let mut df_arena = rwlock_unlock!(df_arena.df_arena, write);
    let df = df_arena.get_mut(&active_df_uuid)?;

    match Arc::get_mut(df) {
        Some(df) => {
            df.projection(project_list)?;
            Ok(active_df_uuid)
        },
        None => {
            let mut df = (**df).clone();
            df.projection(project_list)?;
            df_arena.insert_arc(Arc::new(df))
        },
    }
}

pub fn early_projection_by_id(
    df_arena: &Arenas,
    active_df_uuid: Uuid,
    project_list: &[usize],
) -> PicachvResult<Uuid> {
    let mut df_arena = rwlock_unlock!(df_arena.df_arena, write);
    let df = df_arena.get_mut(&active_df_uuid)?;

    match Arc::get_mut(df) {
        Some(df) => {
            df.projection_by_id(project_list)?;
            Ok(active_df_uuid)
        },
        None => {
            let mut df = (**df).clone();
            df.projection_by_id(project_list)?;
            df_arena.insert_arc(Arc::new(df))
        },
    }
}

/// Thus function enforces the policy for the aggregation expressions.
///
/// Implements the semantic of `apply_fold_on_groups_once` in `semantics.v`.
fn check_policy_agg(
    expr: &Expr,
    ctx: &mut ExpressionEvalContext,
) -> PicachvResult<Policy<PolicyLabel>> {
    picachv_ensure!(
        ctx.in_agg,
        ComputeError: "The expression is not in an aggregation context."
    );

    let agg_expr = match expr {
        Expr::Agg { expr, .. } => expr,
        _ => {
            // We must ensure that the expression being checked is an aggregation expression.
            picachv_bail!(ComputeError: "The expression {expr:?} is not an aggregation expression.")
        },
    };

    let inner_expr = agg_expr.extract_expr(&ctx.arena.expr_arena)?;
    // We first check the policy enforcement for the inner expression.
    let inner = inner_expr.check_policy_in_group(ctx)?;
    // We then apply the `fold` thing on `inner`.
    fold_on_groups(&inner, agg_expr.as_groupby_method())
}

/// Performs the aggregation on the dataframe; see `apply_fold_on_groups` in `semantics.v`.
///
/// The logic of this function is rather simple: it simply iterates all the groups as speci-
/// fied by the `gb_proxy` and applies the aggregation functions on the groups.
fn check_expressions_agg(
    arena: &Arenas,
    active_df_uuid: Uuid,
    agg_list: &[Arc<Expr>],
    gb_proxy: &GroupByProxy,
    udfs: &HashMap<String, Udf>,
) -> PicachvResult<Uuid> {
    let idx = match gb_proxy.group_by.as_ref() {
        Some(gb) => match gb {
            GroupBy::GroupByIdx(idx) => idx.clone(),
            GroupBy::GroupBySlice(slice) => convert_slice_to_idx(slice)?,
        },
        None => picachv_bail!(ComputeError: "The group by is empty."),
    };

    tracing::debug!("the aggregation list is {agg_list:?} against the group by {idx:?}");

    check_expressions_agg_idx(arena, active_df_uuid, &idx, agg_list, udfs)
}

fn check_expressions_agg_idx(
    arena: &Arenas,
    active_df_uuid: Uuid,
    gb_proxy: &GroupByIdx,
    agg_list: &[Arc<Expr>],
    udfs: &HashMap<String, Udf>,
) -> Result<Uuid, picachv_error::PicachvError> {
    let mut df_arena = rwlock_unlock!(arena.df_arena, write);
    let df = df_arena.get(&active_df_uuid)?;

    let mut res = vec![];
    // The semantic requires us to first iterate over the aggregate list.
    for agg in agg_list.into_iter() {
        let mut group_res = vec![];
        // Then we need to iterate over the groups.
        for group in gb_proxy.groups.iter() {
            tracing::debug!("check_expressions_agg_idx: group = {group:?}  ");
            let mut ctx = ExpressionEvalContext::new(df.schema.clone(), df, true, udfs, arena);
            ctx.gb_proxy = Some(&group);

            // For each group we will get the result of the aggregation.
            group_res.push(check_policy_agg(agg, &mut ctx)?);
        }

        // Then for each expression, we add it to the result.
        res.push(group_res);
    }

    tracing::debug!("check_expressions_agg: res = {res:?}");

    // Let us now construct a new dataframe.
    let df = PolicyGuardedDataFrame {
        columns: res
            .into_iter()
            .map(|col| PolicyGuardedColumn { policies: col })
            .collect(),
        // We don't need the schema since it is anonymous.
        schema: Default::default(),
    };

    df_arena.insert(df)
}

fn do_check_expressions(
    arena: &Arenas,
    df: &PolicyGuardedDataFrame,
    expression: &[Arc<Expr>],
    udfs: &HashMap<String, Udf>,
) -> PicachvResult<PolicyGuardedDataFrame> {
    let rows = df.shape().0;
    let mut col = vec![];
    let mut ctx = ExpressionEvalContext::new(df.schema.clone(), df, false, udfs, arena);
    for expr in expression.into_iter() {
        let mut cur = vec![];
        for idx in 0..rows {
            let updated_label = expr.check_policy_in_row(&mut ctx, idx)?;
            cur.push(updated_label);
        }

        col.push(PolicyGuardedColumn { policies: cur });
    }

    Ok(PolicyGuardedDataFrame {
        columns: col,
        schema: df.schema.clone(),
    })
}

fn check_expressions(
    arena: &Arenas,
    active_df_uuid: Uuid,
    expression: &[Arc<Expr>],
    keep_old: bool, // Whether we need to alter the dataframe in the arena.
    udfs: &HashMap<String, Udf>,
) -> PicachvResult<Uuid> {
    let mut df_arena = rwlock_unlock!(arena.df_arena, write);
    let df = df_arena.get_mut(&active_df_uuid)?;
    let can_replace = Arc::strong_count(df) == 1 && !keep_old;

    let new_df = Arc::new(do_check_expressions(arena, df, expression, udfs)?);

    if can_replace {
        let _ = std::mem::replace(df, new_df);
        Ok(active_df_uuid)
    } else {
        if !keep_old {
            df_arena.insert_arc(new_df)
        } else {
            Ok(active_df_uuid)
        }
    }
}

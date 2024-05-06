pub mod builder;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use picachv_error::PicachvResult;
use picachv_message::GroupByProxy;
use uuid::Uuid;

use crate::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
use crate::expr::Expr;
use crate::policy::context::ExpressionEvalContext;
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
        expression: Vec<Uuid>,
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
    },

    DataFrameScan {
        schema: Vec<String>,
        projection: Option<Vec<String>>,
        selection: Option<Uuid>,
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
            Self::Projection { expression, .. } => {
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
        }
    }

    /// This is the function eventually called to check if a physical operator is
    /// allowed to be executed. The caller is required to call this function *before*
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
    ///     if (!this->check(state, this->active_df_uuid)) {
    ///         return nullptr;
    ///     }
    ///     return this->execute_impl(state);
    /// }
    /// ```
    pub fn check_executor(
        &self,
        arena: &Arenas,
        active_df_uuid: Uuid,
        udfs: &HashMap<String, Udf>,
    ) -> PicachvResult<Uuid> {
        log::debug!(
            "execute_prologue: checking {:?} with {active_df_uuid}",
            self
        );

        match self {
            // See the semantics for `apply_proj_in_relation`.
            Plan::Projection { expression, .. } => {
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
            } => match selection {
                Some(s) => {
                    let uuid = if let Some(projection) = projection {
                        early_projection(arena, active_df_uuid, projection)?
                    } else {
                        active_df_uuid
                    };

                    let expr = {
                        let expr_arena = rwlock_unlock!(arena.expr_arena, read);
                        expr_arena.get(s)?.clone()
                    };

                    check_expressions(arena, uuid, &[expr], true, udfs)?;

                    Ok(active_df_uuid)
                },
                None => Ok(active_df_uuid),
            },

            Plan::Aggregation {
                keys,
                aggs,
                gb_proxy,
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
                let first_part = check_expressions(arena, active_df_uuid, &keys, true, udfs)?;
                let second_part =
                    check_expressions_agg(arena, active_df_uuid, &aggs, gb_proxy, udfs)?;

                // Combine the two parts.
                todo!()
            },
        }
    }
}

/// This function is used to apply the projection on the dataframe.
fn early_projection(
    df_arena: &Arenas,
    active_df_uuid: Uuid,
    project_list: &[String],
) -> PicachvResult<Uuid> {
    let mut df_arena = rwlock_unlock!(df_arena.df_arena, write);
    let df = df_arena.get_mut(&active_df_uuid)?;

    match Arc::get_mut(df) {
        Some(df) => {
            df.early_projection(project_list)?;
            Ok(active_df_uuid)
        },
        None => {
            let mut df = (**df).clone();
            df.early_projection(project_list)?;
            df_arena.insert_arc(Arc::new(df))
        },
    }
}

fn check_expressions_agg(
    arena: &Arenas,
    active_df_uuid: Uuid,
    expression: &[Arc<Expr>],
    gb_proxy: &GroupByProxy,
    udfs: &HashMap<String, Udf>,
) -> PicachvResult<Uuid> {
    let mut df_arena = rwlock_unlock!(arena.df_arena, write);
    let df = df_arena.get_mut(&active_df_uuid)?;
    let group_num = gb_proxy.first.len();

    for group in 0..group_num {
        let mut ctx = ExpressionEvalContext::new(df.schema.clone(), df, true, udfs);
        ctx.gb_proxy = Some(&gb_proxy.groups[group]);

        for expr in expression.into_iter() {
            for idx in 0..df.shape().0 {
                let updated_label = expr.check_policy_agg(&mut ctx)?;
            }
        }
    }

    todo!()
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

    let rows = df.shape().0;
    let mut col = vec![];
    let mut ctx = ExpressionEvalContext::new(df.schema.clone(), df, false, udfs);
    for expr in expression.into_iter() {
        let mut cur = vec![];
        for idx in 0..rows {
            let updated_label = expr.check_policy_in_row(&mut ctx, idx)?;
            cur.push(updated_label);
        }

        col.push(PolicyGuardedColumn { policies: cur });
    }

    let new_df = Arc::new(PolicyGuardedDataFrame {
        columns: col,
        schema: df.schema.clone(),
    });

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

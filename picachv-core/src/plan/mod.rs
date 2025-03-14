pub mod builder;

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use picachv_error::{picachv_bail, picachv_ensure, PicachvResult};
use picachv_message::group_by_idx::Groups;
use picachv_message::group_by_proxy::GroupBy;
use picachv_message::{ContextOptions, GroupByIdx, GroupByIdxMultiple, GroupByProxy, GroupBySlice};
use rayon::prelude::*;
use uuid::Uuid;

use crate::dataframe::{
    idx_to_group_info_vec, Chunks, PolicyGuardedColumn, PolicyGuardedDataFrame,
};
use crate::expr::pexpr::PExpr;
use crate::expr::{fold_on_groups, AExpr};
use crate::policy::context::ExpressionEvalContext;
use crate::policy::Policy;
use crate::profiler::PROFILER;
use crate::thread_pool::THREAD_POOL;
use crate::udf::Udf;
use crate::{Arenas, GroupInformation};

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
///   to give the checker the *optimized* plan.
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
        /// The early projected list.
        projection: Option<Vec<usize>>,
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
    pub fn name(&self) -> &str {
        match self {
            Self::Select { .. } => "Select",
            Self::Projection { .. } => "Projection",
            Self::Aggregation { .. } => "Aggregation",
            Self::DataFrameScan { .. } => "DataFrameScan",
            Self::Hstack { .. } => "Hstack",
        }
    }

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
                projection,
                selection,
            } => {
                let mut n_columns: String = "*".to_string();
                if let Some(columns) = projection {
                    n_columns = format!("{}", columns.len());
                }
                let selection = match selection {
                    Some(s) => Cow::Owned(format!("{s:?}")),
                    None => Cow::Borrowed("None"),
                };
                write!(
                    f,
                    "{:indent$}DF CAN PROJECT {} COLUMNS; SELECTION: {:?}",
                    "", n_columns, selection,
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
        options: &ContextOptions,
    ) -> PicachvResult<Uuid> {
        #[cfg(feature = "trace")]
        tracing::debug!(
            "execute_prologue: checking {:?} with {active_df_uuid}",
            self
        );

        let f = || match self {
            // See the semantics for `apply_proj_in_relation`.
            Plan::Projection {
                expressions: expression,
                ..
            } => {
                let expr_arena = arena.expr_arena.read();
                let expression = expression
                    .par_iter()
                    .map(|e| expr_arena.get(e))
                    .collect::<PicachvResult<Vec<_>>>()?;
                check_expressions(arena, active_df_uuid, &expression, false, udfs, options)
            },
            Plan::Select { predicate, .. } => {
                let predicate = {
                    let expr_arena = arena.expr_arena.read();
                    expr_arena.get(predicate)?.clone()
                };

                check_expressions(arena, active_df_uuid, &[&predicate], true, udfs, options)?;

                Ok(active_df_uuid)
            },
            Plan::DataFrameScan {
                selection,
                projection,
            } => {
                let projected_uuid = match projection {
                    Some(projection) => early_projection(arena, active_df_uuid, projection),
                    None => Ok(active_df_uuid),
                }?;

                match selection {
                    Some(s) => {
                        let expr = {
                            let expr_arena = arena.expr_arena.read();
                            expr_arena.get(s)?.clone()
                        };

                        check_expressions(arena, projected_uuid, &[&expr], true, udfs, options)?;

                        Ok(projected_uuid)
                    },
                    None => Ok(projected_uuid),
                }
            },

            Plan::Aggregation {
                keys,
                aggs,
                gb_proxy,
                ..
            } => {
                let expr_arena = arena.expr_arena.read();
                let keys = keys
                    .par_iter()
                    .map(|e| expr_arena.get(e))
                    .collect::<PicachvResult<Vec<_>>>()?;
                let aggs = aggs
                    .par_iter()
                    .map(|e| expr_arena.get(e))
                    .collect::<PicachvResult<Vec<_>>>()?;

                let new_df = match &gb_proxy.group_by {
                    // This is a special case where ther are multiple chunks.
                    Some(GroupBy::GroupByIdxMultiple(gbm)) => {
                        groupby_multiple(arena, &keys, &aggs, udfs, options, &gbm)
                    },
                    Some(GroupBy::GroupByIdx(gbi)) => {
                        let gb_proxy = idx_to_group_info_vec(gbi);
                        let df = arena.df_arena.read().get(&active_df_uuid)?.clone();
                        groupby_single(arena, &df, &keys, udfs, options, &gb_proxy, &aggs)
                    },
                    Some(GroupBy::NoGroup(_)) => {
                        picachv_ensure!(
                            keys.is_empty(),
                            ComputeError: "The group by is emptt, but we've got non-empty keys."
                        );

                        let df = arena.df_arena.read().get(&active_df_uuid)?.clone();
                        let gi = vec![GroupInformation {
                            first: 0,
                            groups: (0..df.shape().0).collect(),
                            hash: None,
                        }];

                        groupby_single(arena, &df, &keys, udfs, options, &gi, &aggs)
                    },

                    _ => picachv_bail!(ComputeError: "By slice is not supported anymore"),
                }?;

                arena.df_arena.write().insert(new_df)
            },

            Plan::Hstack {
                cse_expressions,
                expressions,
            } => do_hstack(
                arena,
                active_df_uuid,
                cse_expressions,
                expressions,
                udfs,
                options,
            ),
        };

        if options.enable_profiling {
            PROFILER.profile(f, self.name().to_string().into())
        } else {
            f()
        }
    }
}

pub(crate) fn groupby_single(
    arena: &Arenas,
    df: &Arc<PolicyGuardedDataFrame>,
    keys: &[&Arc<AExpr>],
    udfs: &HashMap<String, Udf>,
    options: &ContextOptions,
    gi: &[GroupInformation],
    aggs: &[&Arc<AExpr>],
) -> PicachvResult<PolicyGuardedDataFrame> {
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
        let df = do_check_expressions(arena, df, &keys, udfs, options, "groupby")?;

        aggregate_keys(&df, gi, options)
    }?;
    // This is in fact `apply_fold_on_groups`, but for the sake of naming consistency
    // we use `check_expressions_agg`.
    let second_part = check_expressions_agg(arena, df, gi, &aggs, udfs, options)?;

    // Combine the two parts.
    let df_arena = arena.df_arena.read();
    let second_part = df_arena.get(&second_part)?;

    if options.enable_profiling {
        PROFILER.profile(
            || PolicyGuardedDataFrame::stitch(&first_part, &second_part),
            "groupby_single: stitch".into(),
        )
    } else {
        PolicyGuardedDataFrame::stitch(&first_part, &second_part)
    }
}

/// Do the horizontal stack operation.
///
/// A horizontal stack operation is used to append new columns directly to the existing dataframe
/// without any join operation. There might be common subexpressions in the expressions that are
/// evaluated before the actual expressions are evaluated. We split it into two parts: we first
/// append the common subexpressions to the dataframe and then we evaluate the actual expressions.
/// The final result is the dataframe with the actual expressions appended.
#[cfg_attr(not(feature = "coq"), tracing::instrument)]
fn do_hstack(
    arena: &Arenas,
    active_df_uuid: Uuid,
    cse_expressions: &[Uuid],
    expressions: &[Uuid],
    udfs: &HashMap<String, Udf>,
    options: &ContextOptions,
) -> PicachvResult<Uuid> {
    let mut df_arena = arena.df_arena.write();
    let expr_arena = arena.expr_arena.read();
    let df = df_arena.get_mut(&active_df_uuid)?;

    let cse_expressions = cse_expressions
        .par_iter()
        .map(|e| expr_arena.get(e))
        .collect::<PicachvResult<Vec<_>>>()?;
    let expressions = expressions
        .par_iter()
        .map(|e| expr_arena.get(e))
        .collect::<PicachvResult<Vec<_>>>()?;

    let f = || {
        let new_df = if cse_expressions.is_empty() {
            do_check_expressions(arena, df, &expressions, udfs, options, "non-agg")?
        } else {
            // First let us collect the common subexpression part.
            let cse_part =
                do_check_expressions(arena, df, &cse_expressions, udfs, options, "non-agg")?;
            // We then stitch the common subexpression part with the dataframe.
            let cse_part = PolicyGuardedDataFrame::stitch(df, &cse_part)?;
            // We then evaluate the actual expressions.
            do_check_expressions(arena, &cse_part, &expressions, udfs, options, "non-agg")?
        };

        // We then add new columns.
        PolicyGuardedDataFrame::stitch(df, &new_df)
    };

    let new_df = if options.enable_profiling {
        PROFILER.profile(f, "do_hstack".into())
    } else {
        f()
    }?;

    match Arc::get_mut(df) {
        Some(df) => {
            *df = new_df;
            Ok(active_df_uuid)
        },
        None => df_arena.insert_arc(Arc::new(new_df)),
    }
}

#[deprecated(note = "do not use this")]
#[allow(dead_code)]
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
            .into_par_iter()
            .map(|e| Groups {
                first: e.0,
                group: e.1,
            })
            .collect(),
    })
}

fn groupby_multiple(
    arena: &Arenas,
    keys: &[&Arc<AExpr>],
    aggs: &[&Arc<AExpr>],
    udfs: &HashMap<String, Udf>,
    options: &ContextOptions,
    gbm: &GroupByIdxMultiple,
) -> PicachvResult<PolicyGuardedDataFrame> {
    let chunks = Chunks::new_from_groupby_multiple(gbm)?;
    // Convert this to GroupedDataFrameWithHash
    chunks.check(arena, keys, aggs, udfs, options)
}

fn aggregate_keys(
    df: &PolicyGuardedDataFrame,
    gi: &[GroupInformation],
    options: &ContextOptions,
) -> PicachvResult<PolicyGuardedDataFrame> {
    let columns = THREAD_POOL.install(|| {
        (0..df.shape().1)
            .into_par_iter()
            .map(|col_idx| {
                let cur = || {
                    gi.into_par_iter()
                        .map(|group| {
                            group
                                .groups
                                .par_iter()
                                .fold(
                                    || Ok(Arc::new(Policy::PolicyClean)),
                                    |mut acc, idx| {
                                        let p = &df.columns[col_idx][*idx as usize];
                                        acc = Ok(Arc::new(acc?.join(p)?));
                                        acc
                                    },
                                )
                                .reduce(
                                    || Ok(Arc::new(Policy::PolicyClean)),
                                    |mut acc, next| {
                                        acc = Ok(Arc::new(acc?.join(next?.as_ref())?));
                                        acc
                                    },
                                )
                        })
                        .collect::<PicachvResult<Vec<_>>>()
                };

                let cur = if options.enable_profiling {
                    PROFILER.profile(cur, "aggregate: groupby".into())
                } else {
                    cur()
                }?;

                Ok(Arc::new({
                    let f = || PolicyGuardedColumn::new_from_iter(cur.par_iter());

                    if options.enable_profiling {
                        PROFILER.profile(f, "aggregate: process".into())
                    } else {
                        f()
                    }
                }?))
            })
            .collect::<PicachvResult<Vec<_>>>()
    })?;

    Ok(PolicyGuardedDataFrame {
        columns,
        ..Default::default()
    })
}

pub fn early_projection(
    df_arena: &Arenas,
    active_df_uuid: Uuid,
    project_list: &[usize],
) -> PicachvResult<Uuid> {
    let mut df_arena = df_arena.df_arena.write();
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
    expr: &AExpr,
    ctx: &ExpressionEvalContext,
    options: &ContextOptions,
) -> PicachvResult<Policy> {
    picachv_ensure!(
        ctx.in_agg,
        ComputeError: "The expression is not in an aggregation context."
    );

    let agg_expr = match expr {
        AExpr::Agg { expr, .. } => expr,
        AExpr::Count => return Ok(Policy::PolicyClean),
        _ => {
            // We must ensure that the expression being checked is an aggregation expression.
            picachv_bail!(ComputeError: "The expression {expr:?} is not an aggregation expression.")
        },
    };

    let inner_expr = agg_expr.extract_expr(&ctx.arena.expr_arena)?;
    let inner_expr = PExpr::new_from_aexpr(&inner_expr, &ctx.arena.expr_arena)?;
    // We first check the policy enforcement for the inner expression.
    let inner = inner_expr.check_policy_in_group(ctx, options)?;
    // We then apply the `fold` thing on `inner`.
    let f = || fold_on_groups(&inner, agg_expr.as_groupby_method());

    if options.enable_profiling {
        PROFILER.profile(f, "check_policy_agg: fold_on_groups".into())
    } else {
        f()
    }
}

/// Performs the aggregation on the dataframe; see `apply_fold_on_groups` in `semantics.v`.
///
/// The logic of this function is rather simple: it simply iterates all the groups as speci-
/// fied by the `gb_proxy` and applies the aggregation functions on the groups.
fn check_expressions_agg(
    arena: &Arenas,
    df: &Arc<PolicyGuardedDataFrame>,
    gi: &[GroupInformation],
    agg_list: &[&Arc<AExpr>],
    udfs: &HashMap<String, Udf>,
    options: &ContextOptions,
) -> PicachvResult<Uuid> {
    let f = || {
        THREAD_POOL.install(|| {
            agg_list
                .par_iter()
                .map(|agg| {
                    gi.par_iter()
                        .map(|group| {
                            let mut ctx = ExpressionEvalContext::new("agg", df, true, udfs, arena);
                            ctx.gi = Some(group);

                            Ok(Arc::new(check_policy_agg(agg, &ctx, options)?))
                        })
                        .collect::<PicachvResult<Vec<_>>>()
                })
                .collect::<PicachvResult<Vec<_>>>()
        })
    };

    let res = if options.enable_profiling {
        PROFILER.profile(f, "aggregate: policy_eval".into())
    } else {
        f()
    }?;

    #[cfg(feature = "trace")]
    tracing::debug!("check_expressions_agg: res = {res:?}");

    // Let us now construct a new dataframe.
    let columns = || {
        THREAD_POOL.install(|| {
            res.into_par_iter()
                .map(|col| {
                    Ok(Arc::new(PolicyGuardedColumn::new_from_iter(
                        col.par_iter(),
                    )?))
                })
                .collect::<PicachvResult<Vec<_>>>()
        })
    };

    let columns = if options.enable_profiling {
        PROFILER.profile(columns, "aggregate: process".into())
    } else {
        columns()
    }?;

    let df = PolicyGuardedDataFrame {
        columns,
        ..Default::default()
    };

    arena.df_arena.write().insert(df)
}

fn do_check_expressions(
    arena: &Arenas,
    df: &PolicyGuardedDataFrame,
    expression: &[&Arc<AExpr>],
    udfs: &HashMap<String, Udf>,
    options: &ContextOptions,
    name: &str,
) -> PicachvResult<PolicyGuardedDataFrame> {
    let rows = df.shape().0;
    let ctx = ExpressionEvalContext::new(name, df, false, udfs, arena);

    let physical_expressions = THREAD_POOL.install(|| {
        expression
            .into_par_iter()
            .map(|e| PExpr::new_from_aexpr(e, &arena.expr_arena))
            .collect::<PicachvResult<Vec<_>>>()
    })?;

    let f = || {
        THREAD_POOL.install(|| {
            physical_expressions
                .par_iter()
                .map(|expr| {
                    let cur = (0..rows)
                        .into_par_iter()
                        .map(|idx| expr.check_policy_in_row(&ctx, idx))
                        .collect::<PicachvResult<Vec<_>>>()?;
                    Ok(Arc::new({
                        let f = || PolicyGuardedColumn::new_from_iter(cur.par_iter());

                        if options.enable_profiling {
                            PROFILER.profile(f, format!("{name}: policy_eval").into())
                        } else {
                            f()
                        }?
                    }))
                })
                .collect::<PicachvResult<Vec<_>>>()
        })
    };

    let columns = if options.enable_profiling {
        PROFILER.profile(f, format!("{name}: process").into())
    } else {
        f()
    }?;

    Ok(PolicyGuardedDataFrame {
        columns,
        ..Default::default()
    })
}

fn check_expressions(
    arena: &Arenas,
    active_df_uuid: Uuid,
    expression: &[&Arc<AExpr>],
    keep_old: bool, // Whether we need to alter the dataframe in the arena.
    udfs: &HashMap<String, Udf>,
    options: &ContextOptions,
) -> PicachvResult<Uuid> {
    let new_df = {
        let df = arena.df_arena.read();
        let df = df.get(&active_df_uuid)?;
        Arc::new(do_check_expressions(
            arena, df, expression, udfs, options, "non-agg",
        )?)
    };

    let mut df_arena = arena.df_arena.write();

    let df = df_arena.get_mut(&active_df_uuid)?;
    let can_replace = Arc::strong_count(df) == 1 && !keep_old;

    if can_replace {
        let _ = std::mem::replace(df, new_df);
        Ok(active_df_uuid)
    } else if !keep_old {
        df_arena.insert_arc(new_df)
    } else {
        Ok(active_df_uuid)
    }
}

pub mod builder;

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use picachv_error::{picachv_bail, picachv_ensure, ErrorStateSync, PicachvResult};
use polars_core::schema::SchemaRef;
use uuid::Uuid;

use crate::arena::Arena;
use crate::constants::{JoinType, LogicalPlanType};
use crate::dataframe::PolicyGuardedDataFrame;
use crate::expr::Expr;
use crate::policy::context::ExpressionEvalContext;
use crate::{rwlock_unlock, Arenas};

pub type PlanArena = Arena<Plan>;

/// This struct describes a physical plan that the caller wants to perform on the
/// raw data. We do not use the [`LogicalPlan`] shipped with polars because it contains too
/// many unnecessary operations.
///
/// # Note
///
/// We only consider common and generic logical plans in this enum type and avoid adding
/// too implementation- or architecture-specific operations.
///
/// We check if the physical plan conforms to the prescribed privacy policy. It is recommended
/// to give the checker the *optimized* plan.
#[derive(Clone)]
pub enum Plan {
    /// Select with *filter conditions* that work on a [`Plan`].
    Select { input: Box<Plan>, predicate: Expr },

    /// The distinct expression.
    Distinct {
        input: Box<Plan>,
        // options: DistinctOptions,
    },

    /// Projection
    Projection {
        input: Box<Plan>,
        /// Column 'names' as we may apply some transformation on columns.
        expression: Vec<Expr>,
        schema: SchemaRef,
    },

    /// Aggregate and group by
    Aggregation {
        input: Box<Plan>,
        schema: SchemaRef,
        /// Group by `keys`.
        keys: Arc<Vec<Expr>>,
        aggs: Vec<Expr>,
        // apply: Option<Arc<dyn UserDefinedFunction>>,
        maintain_order: bool,
    },

    /// Join operation
    Join {
        input_left: Box<Plan>,
        input_right: Box<Plan>,
        schema: SchemaRef,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        options: JoinType,
    },

    Union {
        input_left: Box<Plan>,
        input_right: Box<Plan>,
        schema: SchemaRef,
    },

    /// Error that should be emitted later.
    Error {
        input: Option<Box<Plan>>,
        err: ErrorStateSync,
        // Should we add a span?
    },

    DataFrameScan {
        schema: SchemaRef,
        // schema of the projected file
        output_schema: Option<SchemaRef>,
        projection: Option<Vec<String>>,
        selection: Option<Expr>,
    },

    Other {
        inputs: Vec<Box<Plan>>,
        schema: SchemaRef,
    },
}

impl fmt::Debug for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format(f, 0)
    }
}

impl Plan {
    // pub(crate) fn callback(&self) -> &Caller {
    //     match self {
    //         Self::Select { cb, .. }
    //         | Self::Distinct { cb, .. }
    //         | Self::Projection { cb, .. }
    //         | Self::Aggregation { cb, .. }
    //         | Self::Join { cb, .. }
    //         | Self::Union { cb, .. }
    //         | Self::Error { cb, .. }
    //         | Self::DataFrameScan { cb, .. }
    //         | Self::Other { cb, .. } => cb,
    //     }
    // }

    // pub fn call(&self) -> PicachvResult<Vec<u8>> {
    //     self.callback().call()
    // }

    /// Formats the current physical plan according to the given `indent`.
    pub(crate) fn format(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let sub_indent = indent + 4;
        match self {
            Self::Select {
                input, predicate, ..
            } => {
                write!(f, "{:indent$}FILTER {predicate:?} FROM", "")?;
                input.format(f, indent)
            },
            Self::Projection {
                input, expression, ..
            } => {
                write!(f, "{:indent$} SELECT {expression:?} FROM", "")?;
                input.format(f, sub_indent)
            },
            Self::Distinct { input, .. } => {
                write!(f, "{:indent$}DISTINCT", "")?;
                input.format(f, sub_indent)
            },
            Self::Union {
                input_left,
                input_right,
                schema,
                ..
            } => {
                write!(f, "{:indent$}UNION", "")?;
                write!(f, "\n{:indent$}LEFT", "")?;
                input_left.format(f, sub_indent)?;
                write!(f, "\n{:indent$}RIGHT", "")?;
                input_right.format(f, sub_indent)?;
                write!(f, "\n{:indent$}OUTPUT SCHEMA: {schema:?}", "")
            },
            Self::DataFrameScan {
                schema,
                projection,
                selection,
                ..
            } => {
                let total_columns = schema.iter_fields().len();
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
                    schema
                        .iter_fields()
                        .map(|field| field.name.clone())
                        .take(4)
                        .collect::<Vec<_>>(),
                    n_columns,
                    total_columns,
                    selection,
                )
            },
            Self::Error { input, err, .. } => {
                write!(
                    f,
                    "Error occurred when constructing the logical plan: {err:?}\nThe previous output is {input:?}"
                )
            },
            Self::Aggregation {
                input, keys, aggs, ..
            } => {
                write!(f, "{:indent$}AGGREGATE", "")?;
                write!(f, "\n{:indent$}\t{aggs:?} GROUP BY {keys:?} FROM", "")?;
                write!(f, "\n{:indent$}\t{input:?}", "")
            },
            Self::Join {
                input_left,
                input_right,
                schema,
                left_on,
                right_on,
                options,
                ..
            } => {
                let fields = schema.iter_fields().collect::<Vec<_>>();
                write!(f, "{:indent$}{options:?} JOIN:", "")?;
                write!(f, "\n{:indent$}LEFT ON: {left_on:?}", "")?;
                write!(f, "\n{:sub_indent$}{input_left:?}", "")?;
                write!(f, "\n{:indent$}RIGHT ON: {right_on:?}", "")?;
                write!(f, "\n{:sub_indent$}{input_right:?}", "")?;
                write!(
                    f,
                    "\n{:indent$}OUTPUT SCHEMA:\n{:sub_indent$}{fields:?}",
                    "", ""
                )?;
                write!(f, "\n{:indent$}END {options:?} JOIN", "")
            },
            Self::Other { inputs, .. } => {
                write!(f, "OTHER: ")?;
                for input in inputs {
                    input.format(f, sub_indent)?;
                }
                Ok(())
            },
        }
    }

    /// Returns the schema of the current physical plan.
    pub fn schema(&self) -> PicachvResult<SchemaRef> {
        match self {
            Self::Distinct { input, .. } | Self::Select { input, .. } => input.schema(),
            Self::Projection { schema, .. } => Ok(schema.clone()),
            Self::Aggregation { schema, .. } => Ok(schema.clone()),
            Self::Join { schema, .. } => Ok(schema.clone()),
            Self::Union { schema, .. } => Ok(schema.clone()),
            Self::Error { err, .. } => Err(err.take()),
            Self::DataFrameScan { schema, .. } => Ok(schema.clone()),
            Self::Other { schema, .. } => Ok(schema.clone()),
        }
    }

    pub fn to_lp_type(&self) -> LogicalPlanType {
        match self {
            Self::Select { .. } => LogicalPlanType::Select,
            Self::Distinct { .. } => LogicalPlanType::Distinct,
            Self::Projection { .. } => LogicalPlanType::Projection,
            Self::Aggregation { .. } => LogicalPlanType::Aggregation,
            Self::Join { .. } => LogicalPlanType::Join,
            Self::Union { .. } => LogicalPlanType::Union,
            Self::DataFrameScan { .. } => LogicalPlanType::Scan,
            Self::Other { .. } => LogicalPlanType::Other,
            Self::Error { .. } => LogicalPlanType::Other,
        }
    }

    fn check_plan(
        &self,
        arena: &Arenas,
        active_df_uuid: Uuid,
        expression: &[Expr],
        in_agg: bool,
        keep_old: bool, // Whether we need to alter the dataframe in the arena.
    ) -> PicachvResult<Uuid> {
        let mut df_arena = rwlock_unlock!(arena.df_arena, write);
        let df = df_arena.get_mut(&active_df_uuid)?;
        let can_replace = Arc::strong_count(df) == 1;

        let mut rows = df.into_rows();
        // We need to check the policy for each row.
        for row in rows.iter_mut() {
            for expr in expression {
                log::debug!("Checking the policy for the expression {expr:?}");

                let mut ctx = ExpressionEvalContext::new(df.schema.clone(), row.clone(), in_agg);
                expr.check_policy_in_row(&mut ctx)?;
                *row = ctx.current_row;
            }
        }

        let mut new_df = PolicyGuardedDataFrame::from(rows);
        new_df.schema = df.schema.clone();
        let new_df = Arc::new(new_df);

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
    pub fn execute_prologue(&self, arena: &Arenas, active_df_uuid: Uuid) -> PicachvResult<Uuid> {
        log::debug!("execute_prologue: checking {:?}", self);

        match self {
            // See the semantics for `apply_proj_in_relation`.
            Plan::Projection { expression, .. } => {
                self.check_plan(arena, active_df_uuid, expression, false, false)
            },
            Plan::Select { predicate, .. } => {
                self.check_plan(arena, active_df_uuid, &vec![predicate.clone()], false, true)
            },
            Plan::Union {
                input_left,
                input_right,
                ..
            }
            | Plan::Join {
                input_left,
                input_right,
                ..
            } => {
                input_left.execute_prologue(arena, active_df_uuid)?;
                input_right.execute_prologue(arena, active_df_uuid)
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

                    self.check_plan(arena, uuid, &vec![s.clone()], false, true)
                },
                None => Ok(active_df_uuid),
            },
            Plan::Other { .. } => unimplemented!("Other"),
            _ => Ok(active_df_uuid),
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

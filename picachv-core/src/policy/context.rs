//! This module defines the context for the policy enforcement.
//!
//! Contexts are used in the evaluation of the expression, operator, and the enforcement of the policy.
//! It is of course possible to pass multiple arguments to a function but this is not ergonomic in the
//! sense that function signature change breaks the code. Instead, we can wrap all the arguments in a
//! context and pass the context to the function.

use std::collections::HashMap;

use picachv_message::group_by_proxy::Groups;

use crate::dataframe::PolicyGuardedDataFrame;
use crate::udf::Udf;
use crate::Arenas;

/// The `eval_env` type defined in the Coq file.
///
/// There are some changes:
/// - No `step` is required since there is no termination check, and
/// - Γ is not required here although we can add it (not necessary).
pub(crate) struct ExpressionEvalContext<'ctx> {
    /// The schema of the current expression.
    pub(crate) schema: Vec<String>,
    /// The current activate data frame.
    pub(crate) df: &'ctx PolicyGuardedDataFrame,
    /// Indicates whether the expression is in an aggregation context.
    pub(crate) in_agg: bool,
    /// The group by columns.
    pub(crate) gb_proxy: Option<&'ctx Groups>,
    /// Some UDF mappings.
    pub(crate) udfs: &'ctx HashMap<String, Udf>,
    /// The reference to the arena.
    pub(crate) arena: &'ctx Arenas,
}

impl<'ctx> ExpressionEvalContext<'ctx> {
    pub fn new(
        schema: Vec<String>,
        df: &'ctx PolicyGuardedDataFrame,
        in_agg: bool,
        udfs: &'ctx HashMap<String, Udf>,
        arena: &'ctx Arenas,
    ) -> Self {
        ExpressionEvalContext {
            schema,
            df,
            in_agg,
            gb_proxy: None,
            udfs,
            arena,
        }
    }

    pub fn get_udf(&self, name: &str) -> Option<&Udf> {
        self.udfs.get(name)
    }
}

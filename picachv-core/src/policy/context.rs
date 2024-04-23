//! This module defines the context for the policy enforcement.
//!
//! Contexts are used in the evaluation of the expression, operator, and the enforcement of the policy.
//! It is of course possible to pass multiple arguments to a function but this is not ergonomic in the
//! sense that function signature change breaks the code. Instead, we can wrap all the arguments in a
//! context and pass the context to the function.

use polars_core::schema::SchemaRef;

use super::{Policy, PolicyLabel};

/// The `eval_env` type defined in the Coq file.
///
/// There are some changes:
/// - No `step` is required since there is no termination check, and
/// - Γ is not required here although we can add it (not necessary).
#[derive(Debug, Default)]
pub(crate) struct ExpressionEvalContext {
    /// The schema of the current expression.
    pub(crate) schema: SchemaRef,
    /// The current row where this expression is current being evaluated.
    pub(crate) current_row: Vec<Policy<PolicyLabel>>,
    /// Indicates whether the expression is in an aggregation context.
    pub(crate) in_agg: bool,
    // TODO.
    group_by: Vec<usize>,
}

impl ExpressionEvalContext {
    pub fn new(schema: SchemaRef, current_row: Vec<Policy<PolicyLabel>>, in_agg: bool) -> Self {
        ExpressionEvalContext {
            schema,
            current_row,
            in_agg,
            ..Default::default()
        }
    }
}

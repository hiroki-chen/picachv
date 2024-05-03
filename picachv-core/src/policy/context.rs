//! This module defines the context for the policy enforcement.
//!
//! Contexts are used in the evaluation of the expression, operator, and the enforcement of the policy.
//! It is of course possible to pass multiple arguments to a function but this is not ergonomic in the
//! sense that function signature change breaks the code. Instead, we can wrap all the arguments in a
//! context and pass the context to the function.

use std::collections::HashMap;

use arrow_schema::SchemaRef;

use super::{Policy, PolicyLabel};
use crate::udf::Udf;

/// The `eval_env` type defined in the Coq file.
///
/// There are some changes:
/// - No `step` is required since there is no termination check, and
/// - Γ is not required here although we can add it (not necessary).
#[derive(Debug)]
pub(crate) struct ExpressionEvalContext<'ctx> {
    /// The schema of the current expression.
    pub(crate) schema: SchemaRef,
    /// The current row where this expression is current being evaluated.
    pub(crate) current_row: Vec<Policy<PolicyLabel>>,
    /// Indicates whether the expression is in an aggregation context.
    pub(crate) in_agg: bool,
    // TODO.
    group_by: Vec<usize>,
    udfs: &'ctx HashMap<String, Udf>,
}

impl<'ctx> ExpressionEvalContext<'ctx> {
    pub fn new(
        schema: SchemaRef,
        current_row: Vec<Policy<PolicyLabel>>,
        in_agg: bool,
        udfs: &'ctx HashMap<String, Udf>,
    ) -> Self {
        ExpressionEvalContext {
            schema,
            current_row,
            in_agg,
            group_by: vec![],
            udfs,
        }
    }

    pub fn get_udf(&self, name: &str) -> Option<&Udf> {
        self.udfs.get(name)
    }
}

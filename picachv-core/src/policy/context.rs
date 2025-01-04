//! This module defines the context for the policy enforcement.
//!
//! Contexts are used in the evaluation of the expression, operator, and the enforcement of the policy.
//! It is of course possible to pass multiple arguments to a function but this is not ergonomic in the
//! sense that function signature change breaks the code. Instead, we can wrap all the arguments in a
//! context and pass the context to the function.

use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use spin::RwLock;

use crate::dataframe::{PolicyGuardedDataFrame, PolicyRef};
use crate::udf::Udf;
use crate::{Arenas, GroupInformation};

/// The `eval_env` type defined in the Coq file.
///
/// There are some changes:
/// - No `step` is required since there is no termination check, and
/// - Γ is not required here although we can add it (not necessary).
#[allow(unused)]
pub struct ExpressionEvalContext<'ctx> {
    pub(crate) name: &'ctx str,
    /// The current activate data frame.
    pub(crate) df: &'ctx PolicyGuardedDataFrame,
    /// Indicates whether the expression is in an aggregation context.
    pub(crate) in_agg: bool,
    /// The group by columns.
    pub(crate) gi: Option<&'ctx GroupInformation>,
    /// Some UDF mappings.
    pub(crate) udfs: &'ctx HashMap<String, Udf>,
    /// The reference to the arena.
    pub(crate) arena: &'ctx Arenas,
    pub(crate) expr_cache: Arc<RwLock<HashMap<u64, PolicyRef>>>,
    pub(crate) group_expr_cache: Arc<RwLock<HashMap<u64, PolicyRef>>>,
}

impl<'ctx> ExpressionEvalContext<'ctx> {
    pub fn new(
        name: &'ctx str,
        df: &'ctx PolicyGuardedDataFrame,
        in_agg: bool,
        udfs: &'ctx HashMap<String, Udf>,
        arena: &'ctx Arenas,
    ) -> Self {
        ExpressionEvalContext {
            name,
            df,
            in_agg,
            gi: None,
            udfs,
            arena,
            expr_cache: Arc::new(RwLock::new(HashMap::new())),
            group_expr_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

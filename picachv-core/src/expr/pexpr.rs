use std::sync::Arc;

use picachv_error::PicachvResult;
use picachv_message::binary_operator;
use spin::RwLock;

use super::{AAggExpr, AExpr, ColumnIdent, ExprArena};
use crate::policy::types::ValueArrayRef;
use crate::policy::TransformType;
use crate::udf::Udf;

/// Similar to [`PExpr`], but this is a physical expression for aggregates.
#[derive(Clone, Hash, PartialEq)]
pub enum PAggExpr {
    Min {
        input: Arc<PExpr>,
        propagate_nans: bool,
    },
    Max {
        input: Arc<PExpr>,
        propagate_nans: bool,
    },
    Median(Arc<PExpr>),
    NUnique(Arc<PExpr>),
    First(Arc<PExpr>),
    Last(Arc<PExpr>),
    Mean(Arc<PExpr>),
    Implode(Arc<PExpr>),
    Count(Arc<PExpr>, bool),
    Quantile {
        expr: Arc<PExpr>,
        quantile: Arc<PExpr>,
    },
    Sum(Arc<PExpr>),
    AggGroups(Arc<PExpr>),
    Std(Arc<PExpr>, u8),
    Var(Arc<PExpr>, u8),
}

/// A `PExpr` is a physical expression that is reified by [`AExpr`] that is stored
/// in the arena. The purpose of this struct is to reduce the overhead when looking
/// up the expression in the arena as policy-check requires intensive lookups.
#[derive(Clone, Hash, PartialEq)]
pub enum PExpr {
    Agg {
        expr: Arc<PAggExpr>,
        values: Option<Arc<Vec<ValueArrayRef>>>,
    },
    Column(ColumnIdent),
    Count,
    Alias {
        expr: Arc<PExpr>,
        name: String,
    },
    Wildcard,
    Filter {
        input: Arc<PExpr>,
        filter: Arc<PExpr>,
    },
    BinaryExpr {
        left: Arc<PExpr>,
        op: binary_operator::Operator,
        right: Arc<PExpr>,
        values: Option<Arc<Vec<ValueArrayRef>>>,
    },
    UnaryExpr {
        arg: Arc<PExpr>,
        op: TransformType,
    },
    Literal,
    Apply {
        udf_desc: Udf,
        args: Vec<Arc<PExpr>>,
        values: Option<Arc<Vec<ValueArrayRef>>>,
    },
    Ternary {
        cond: Arc<PExpr>,
        cond_values: Option<Arc<Vec<bool>>>,
        then: Arc<PExpr>,
        otherwise: Arc<PExpr>,
    },
}

impl PAggExpr {
    pub(crate) fn new_from_aexpr(
        aagg: &AAggExpr,
        expr_arena: &Arc<RwLock<ExprArena>>,
    ) -> PicachvResult<Self> {
        match aagg {
            AAggExpr::Min {
                input,
                propagate_nans,
            } => {
                let input = expr_arena.read().get(input)?.clone();
                let input = PExpr::new_from_aexpr(input.as_ref(), expr_arena)?;
                Ok(PAggExpr::Min {
                    input: Arc::new(input),
                    propagate_nans: *propagate_nans,
                })
            },
            AAggExpr::Max {
                input,
                propagate_nans,
            } => {
                let input = expr_arena.read().get(input)?.clone();
                let input = PExpr::new_from_aexpr(input.as_ref(), expr_arena)?;
                Ok(PAggExpr::Max {
                    input: Arc::new(input),
                    propagate_nans: *propagate_nans,
                })
            },
            AAggExpr::Median(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Median(Arc::new(expr)))
            },
            AAggExpr::NUnique(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::NUnique(Arc::new(expr)))
            },
            AAggExpr::First(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::First(Arc::new(expr)))
            },
            AAggExpr::Last(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Last(Arc::new(expr)))
            },
            AAggExpr::Mean(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Mean(Arc::new(expr)))
            },
            AAggExpr::Implode(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Implode(Arc::new(expr)))
            },
            AAggExpr::Count(expr, distinct) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Count(Arc::new(expr), *distinct))
            },
            AAggExpr::Quantile { expr, quantile } => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                let quantile = expr_arena.read().get(quantile)?.clone();
                let quantile = PExpr::new_from_aexpr(quantile.as_ref(), expr_arena)?;
                Ok(PAggExpr::Quantile {
                    expr: Arc::new(expr),
                    quantile: Arc::new(quantile),
                })
            },
            AAggExpr::Sum(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Sum(Arc::new(expr)))
            },
            AAggExpr::AggGroups(expr) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::AggGroups(Arc::new(expr)))
            },
            AAggExpr::Std(expr, ddof) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Std(Arc::new(expr), *ddof))
            },
            AAggExpr::Var(expr, ddof) => {
                let expr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(expr.as_ref(), expr_arena)?;
                Ok(PAggExpr::Var(Arc::new(expr), *ddof))
            },
        }
    }
}

impl PExpr {
    /// Creates a new `PExpr` from an `AExpr`.
    pub(crate) fn new_from_aexpr(
        aexpr: &AExpr,
        expr_arena: &Arc<RwLock<ExprArena>>,
    ) -> PicachvResult<Self> {
        match aexpr {
            AExpr::Agg { expr, values } => Ok(PExpr::Agg {
                expr: Arc::new(PAggExpr::new_from_aexpr(expr, expr_arena)?),
                values: values.clone(),
            }),
            AExpr::Column(ident) => Ok(PExpr::Column(ident.clone())),
            AExpr::Count => Ok(PExpr::Count),
            AExpr::Alias { expr, name } => {
                let aexpr = expr_arena.read().get(expr)?.clone();
                let expr = PExpr::new_from_aexpr(aexpr.as_ref(), expr_arena)?;
                Ok(PExpr::Alias {
                    expr: Arc::new(expr),
                    name: name.clone(),
                })
            },
            AExpr::Wildcard => Ok(PExpr::Wildcard),
            AExpr::Filter { input, filter } => {
                let input = expr_arena.read().get(input)?.clone();
                let filter = expr_arena.read().get(filter)?.clone();
                let input = PExpr::new_from_aexpr(input.as_ref(), expr_arena)?;
                let filter = PExpr::new_from_aexpr(filter.as_ref(), expr_arena)?;
                Ok(PExpr::Filter {
                    input: Arc::new(input),
                    filter: Arc::new(filter),
                })
            },
            AExpr::BinaryExpr {
                left,
                op,
                right,
                values,
            } => {
                let left = expr_arena.read().get(left)?.clone();
                let right = expr_arena.read().get(right)?.clone();
                let left = PExpr::new_from_aexpr(left.as_ref(), expr_arena)?;
                let right = PExpr::new_from_aexpr(right.as_ref(), expr_arena)?;
                Ok(PExpr::BinaryExpr {
                    left: Arc::new(left),
                    op: op.clone(),
                    right: Arc::new(right),
                    values: values.clone(),
                })
            },
            AExpr::UnaryExpr { arg, op } => {
                let arg = expr_arena.read().get(arg)?.clone();
                let arg = PExpr::new_from_aexpr(arg.as_ref(), expr_arena)?;
                Ok(PExpr::UnaryExpr {
                    arg: Arc::new(arg),
                    op: op.clone(),
                })
            },
            AExpr::Apply {
                udf_desc,
                args,
                values,
            } => {
                let args = args
                    .iter()
                    .map(|arg| {
                        let arg = expr_arena.read().get(arg)?.clone();
                        PExpr::new_from_aexpr(arg.as_ref(), expr_arena)
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;
                Ok(PExpr::Apply {
                    udf_desc: udf_desc.clone(),
                    args: args.into_iter().map(Arc::new).collect(),
                    values: values.clone(),
                })
            },
            AExpr::Literal => Ok(PExpr::Literal),
            AExpr::Ternary {
                cond,
                cond_values,
                then,
                otherwise,
            } => {
                let cond = expr_arena.read().get(cond)?.clone();
                let then = expr_arena.read().get(then)?.clone();
                let otherwise = expr_arena.read().get(otherwise)?.clone();
                let cond = PExpr::new_from_aexpr(cond.as_ref(), expr_arena)?;
                let then = PExpr::new_from_aexpr(then.as_ref(), expr_arena)?;
                let otherwise = PExpr::new_from_aexpr(otherwise.as_ref(), expr_arena)?;
                Ok(PExpr::Ternary {
                    cond: Arc::new(cond),
                    cond_values: cond_values.clone(),
                    then: Arc::new(then),
                    otherwise: Arc::new(otherwise),
                })
            },
        }
    }
}

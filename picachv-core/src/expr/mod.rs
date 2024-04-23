use std::fmt;

use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::binary_operator;

use crate::arena::Arena;
use crate::build_unary_expr;
use crate::policy::context::ExpressionEvalContext;
use crate::policy::{Policy, PolicyLabel, TransformType};

pub mod builder;

/// Stores the expressions.
pub type ExprArena = Arena<Expr>;

#[derive(PartialEq, Clone)]
pub enum AggExpr {
    Min {
        input: Box<Expr>,
        propagate_nans: bool,
    },
    Max {
        input: Box<Expr>,
        propagate_nans: bool,
    },
    Median(Box<Expr>),
    NUnique(Box<Expr>),
    First(Box<Expr>),
    Last(Box<Expr>),
    Mean(Box<Expr>),
    Implode(Box<Expr>),
    // include_nulls
    Count(Box<Expr>, bool),
    Quantile {
        expr: Box<Expr>,
        quantile: Box<Expr>,
    },
    Sum(Box<Expr>),
    AggGroups(Box<Expr>),
    Std(Box<Expr>, u8),
    Var(Box<Expr>, u8),
}

/// An expression type for describing a node in the query.
#[derive(Clone, PartialEq)]
pub enum Expr {
    /// Aggregation.
    Agg(AggExpr),
    /// Select a column.
    Column(String),
    /// Count expression.
    Count,
    /// Making alias.
    Alias {
        expr: Box<Expr>,
        name: String,
    },
    /// "*".
    Wildcard,
    /// Filter.
    Filter {
        input: Box<Expr>,
        filter: Box<Expr>,
    },
    /// Binary operations
    BinaryExpr {
        left: Box<Expr>,
        op: binary_operator::Operator,
        right: Box<Expr>,
    },
    UnaryExpr {
        arg: Box<Expr>,
        op: TransformType,
    },
    Literal,
}

impl Expr {
    pub(crate) fn check_policy_within_agg(
        &self,
        ctx: &mut ExpressionEvalContext,
    ) -> PicachvResult<Policy<PolicyLabel>> {
        picachv_ensure!(
            ctx.in_agg,
            ComputeError: "The expression is not in an aggregation context."
        );
        match self {
            Expr::Agg(_) => Err(PicachvError::ComputeError(
                "Aggregation within aggregation is not allowed.".into(),
            )),

            _ => todo!(),
        }
    }

    /// This function checks the policy enforcement for the expression type.
    ///
    /// The formalized part is described in `pcd-proof/theories/expression.v`.
    /// Note that since the check occurs at the tuple level!
    pub(crate) fn check_policy_in_row(
        &self,
        ctx: &mut ExpressionEvalContext,
    ) -> PicachvResult<Policy<PolicyLabel>> {
        if !ctx.in_agg {
            match self {
                // A literal expression is always allowed because it does not
                // contain any sensitive information.
                Expr::Literal => Ok(Default::default()),
                Expr::BinaryExpr { left, right, .. } => {
                    left.check_policy_in_row(ctx)?;
                    right.check_policy_in_row(ctx)
                },
                // This is truly interesting.
                //
                // See `eval_unary_expression_in_cell`.
                Expr::UnaryExpr { arg, op } => {
                    let policy = arg.check_policy_in_row(ctx)?;
                    policy.downgrade(build_unary_expr!(op.clone()))
                },
                Expr::Column(idx) => {
                    let idx = ctx.schema.iter_names().position(|name| name == idx).ok_or(
                        PicachvError::ComputeError("The column does not exist.".into()),
                    )?;

                    // For column expression this is an interesting undecidable case
                    // where we cannot determine what operation it will be applied.
                    //
                    // We neverthelss approve this operation per evaluation semantics.
                    //
                    // See `EvalColumnNotAgg` in `expression.v`.
                    Ok(ctx.current_row[idx].clone())
                },
                Expr::Alias { expr, .. } => expr.check_policy_in_row(ctx),
                Expr::Filter { input, filter } => {
                    input.check_policy_in_row(ctx)?;
                    filter.check_policy_in_row(ctx)
                },
                Expr::Agg(agg_expr) => todo!(),
                // todo.
                _ => Ok(Default::default()),
            }
        } else {
            self.check_policy_within_agg(ctx)
        }
    }
}

impl fmt::Debug for AggExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Min {
                input,
                propagate_nans,
            } => {
                write!(f, "MIN({input:?}, propagate_nans={propagate_nans})")
            },
            Self::Max {
                input,
                propagate_nans,
            } => {
                write!(f, "MAX({input:?}, propagate_nans={propagate_nans})")
            },
            Self::Median(input) => write!(f, "MEDIAN({input:?})"),
            Self::NUnique(input) => write!(f, "NUNIQUE({input:?})"),
            Self::First(input) => write!(f, "FIRST({input:?})"),
            Self::Last(input) => write!(f, "LAST({input:?})"),
            Self::Mean(input) => write!(f, "MEAN({input:?})"),
            Self::Implode(input) => write!(f, "IMPLODE({input:?})"),
            Self::Count(input, include_nulls) => {
                write!(f, "COUNT({input:?}, include_nulls={include_nulls})")
            },
            Self::Quantile { expr, quantile } => {
                write!(f, "QUANTILE({expr:?}, {quantile:?})")
            },
            Self::Sum(input) => write!(f, "SUM({input:?})"),
            Self::AggGroups(input) => write!(f, "AGG_GROUPS({input:?})"),
            Self::Std(input, n) => write!(f, "STD({input:?}, n={n})"),
            Self::Var(input, n) => write!(f, "VAR({input:?}, n={n})"),
        }
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg) => write!(f, "{agg:?}"),
            Self::Column(column) => write!(f, "col({column})"),
            Self::Count => write!(f, "COUNT"),
            Self::Wildcard => write!(f, "*"),
            Self::Alias { expr, name } => write!(f, "ALIAS {expr:?} -> {name}"),
            Self::Filter {
                input: data,
                filter,
            } => write!(f, "{data:?} WHERE {filter:?}"),
            Self::BinaryExpr { left, op, right } => write!(f, "({left:?} {op:?} {right:?})"),
            Self::UnaryExpr { arg, op } => write!(f, "{op:?} {arg:?}"),
            Self::Literal => write!(f, "LITERAL"),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl ExprArena {}

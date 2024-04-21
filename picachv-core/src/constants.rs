use std::fmt;

use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum LogicalPlanType {
    Select,
    Distinct,
    Projection,
    Aggregation,
    Join,
    Union,
    Scan,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Hash)]
pub enum QuantileInterpolOptions {
    #[default]
    Nearest,
    Lower,
    Higher,
    Midpoint,
    Linear,
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    TryFromPrimitive,
    IntoPrimitive,
)]
#[repr(u8)]
pub enum JoinType {
    /// LEFT OUTER JOIN
    Left,
    /// RIGHT OUTER JOIN
    Right,
    /// FULL OUTER JOIN
    Full,
    #[default]
    /// INNER JOIN
    Inner,
}

/// Logical conjunctions like `and`, `or` used as a binary operator.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, TryFromPrimitive,
)]
#[repr(u8)]
pub enum LogicalBinaryOperator {
    And,
    Or,
}

/// Comparison operators like `==`, `!=`, `>`, `<`, `<=`, `>=`.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, TryFromPrimitive,
)]
#[repr(u8)]
pub enum ComparisonBinaryOperator {
    Eq,
    Neq,
    Gt,
    Lt,
    Le,
    Ge,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, TryFromPrimitive,
)]
#[repr(u8)]
pub enum ArithmeticBinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

/// A binary operator that can be applied to two expressions.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinOperator {
    Logical(LogicalBinaryOperator),
    Comparsion(ComparisonBinaryOperator),
    Arithmetic(ArithmeticBinaryOperator),
}

impl BinOperator {
    #[inline(always)]
    pub fn is_logical(&self) -> bool {
        matches!(self, BinOperator::Logical(_))
    }

    #[inline(always)]
    pub fn is_comparison(&self) -> bool {
        matches!(self, BinOperator::Comparsion(_))
    }

    #[inline(always)]
    pub fn is_arithmetic(&self) -> bool {
        matches!(self, BinOperator::Arithmetic(_))
    }
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, TryFromPrimitive,
)]
#[repr(u8)]
pub enum UnaryOperator {
    Identity,
    Redact,
    Substitute,
    Not,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum GroupByMethod {
    Min,
    NanMin,
    Max,
    NanMax,
    Median,
    Mean,
    First,
    Last,
    Sum,
    Groups,
    NUnique,
    Quantile(f64, QuantileInterpolOptions),
    Count { include_nulls: bool },
    Implode,
    Std(u8),
    Var(u8),
}

impl fmt::Display for LogicalBinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalBinaryOperator::And => write!(f, "&&"),
            LogicalBinaryOperator::Or => write!(f, "||"),
        }
    }
}

impl fmt::Display for ComparisonBinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonBinaryOperator::Eq => write!(f, "=="),
            ComparisonBinaryOperator::Neq => write!(f, "!="),
            ComparisonBinaryOperator::Gt => write!(f, ">"),
            ComparisonBinaryOperator::Lt => write!(f, "<"),
            ComparisonBinaryOperator::Le => write!(f, "<="),
            ComparisonBinaryOperator::Ge => write!(f, ">="),
        }
    }
}

impl fmt::Display for ArithmeticBinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArithmeticBinaryOperator::Add => write!(f, "+"),
            ArithmeticBinaryOperator::Sub => write!(f, "-"),
            ArithmeticBinaryOperator::Mul => write!(f, "*"),
            ArithmeticBinaryOperator::Div => write!(f, "/"),
            ArithmeticBinaryOperator::Mod => write!(f, "%"),
            ArithmeticBinaryOperator::Pow => write!(f, "^"),
        }
    }
}

impl fmt::Display for BinOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinOperator::Logical(op) => write!(f, "{}", op),
            BinOperator::Comparsion(op) => write!(f, "{}", op),
            BinOperator::Arithmetic(op) => write!(f, "{}", op),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Identity => write!(f, ""),
            UnaryOperator::Redact => write!(f, "redact"),
            UnaryOperator::Substitute => write!(f, "substitute"),
            UnaryOperator::Not => write!(f, "not"),
        }
    }
}

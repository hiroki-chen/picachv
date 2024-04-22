use std::{collections::HashSet, fmt, hash::Hash, ops::Range};

use ordered_float::OrderedFloat;
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};

use crate::constants::GroupByMethod;

use super::lattice::Lattice;
use super::types::DpParam;

/// Denotes the privacy schemes that should be applied to the result and/or the dataset.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum PrivacyScheme {
    /// Differential privacy with a given set of parameters (`epsilon`, `delta`).
    DifferentialPrivacy(DpParam),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TransformType {
    /// An identity transform.
    Identify,
    /// Redact
    Redact { range: Range<usize> },
    /// Generalize
    Generalize { range: Range<usize> },
    /// Replace
    Replace,
    /// Shift by days
    Shift { by: i64 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggType(GroupByMethod);

impl Hash for AggType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self.0 {
            GroupByMethod::Median => "median".hash(state),
            GroupByMethod::Mean => "mean".hash(state),
            GroupByMethod::Sum => "sum".hash(state),
            GroupByMethod::Min => "min".hash(state),
            GroupByMethod::Max => "max".hash(state),
            GroupByMethod::First => "first".hash(state),
            GroupByMethod::Last => "last".hash(state),
            GroupByMethod::NUnique => "nunique".hash(state),
            GroupByMethod::Groups => "groups".hash(state),
            GroupByMethod::NanMax => "nanmax".hash(state),
            GroupByMethod::NanMin => "nanmin".hash(state),
            GroupByMethod::Count { include_nulls } => {
                if include_nulls {
                    "count".hash(state);
                } else {
                    "count-nonnull".hash(state);
                }
            },
            GroupByMethod::Quantile(percentage, op) => {
                "quantile".hash(state);
                OrderedFloat(percentage).hash(state);
                op.hash(state);
            },
            GroupByMethod::Std(v) => {
                "std".hash(state);
                v.hash(state);
            },
            GroupByMethod::Var(v) => {
                "var".hash(state);
                v.hash(state);
            },
            GroupByMethod::Implode => "implode".hash(state),
        }
    }
}

impl PartialOrd for AggType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match &(self.0, other.0) {
            _ => todo!("implement partial_cmp for AggType"),
        }
    }
}
impl Eq for AggType {}

pub trait SetLike {
    fn is_subset(&self, other: &Self) -> bool;
    fn union(&self, other: &Self) -> Self;
    fn intersection(&self, other: &Self) -> Self;
    fn set_eq(&self, other: &Self) -> bool {
        self.is_subset(other) && other.is_subset(self)
    }
}

#[derive(Debug, Clone)]
pub struct TransformOps(pub HashSet<TransformType>);
#[derive(Debug, Clone)]
pub struct AggOps(pub HashSet<AggType>);
#[derive(Debug, Clone)]
pub struct PrivacyOp(pub PrivacyScheme);

impl SetLike for TransformOps {
    fn is_subset(&self, other: &Self) -> bool {
        self.0.iter().all(|op| other.0.contains(op))
    }

    fn intersection(&self, other: &Self) -> Self {
        TransformOps(self.0.intersection(&other.0).cloned().collect())
    }

    fn union(&self, other: &Self) -> Self {
        TransformOps(self.0.union(&other.0).cloned().collect())
    }
}

impl SetLike for AggOps {
    fn is_subset(&self, other: &Self) -> bool {
        self.0.iter().all(|op| other.0.contains(op))
    }

    fn intersection(&self, other: &Self) -> Self {
        AggOps(self.0.intersection(&other.0).cloned().collect())
    }

    fn union(&self, other: &Self) -> Self {
        AggOps(self.0.union(&other.0).cloned().collect())
    }
}

impl SetLike for PrivacyOp {
    fn is_subset(&self, other: &Self) -> bool {
        self.0 <= other.0
    }

    fn intersection(&self, other: &Self) -> Self {
        PrivacyOp(match (&self.0, &other.0) {
            (PrivacyScheme::DifferentialPrivacy(lhs), PrivacyScheme::DifferentialPrivacy(rhs)) => {
                PrivacyScheme::DifferentialPrivacy(*lhs.min(rhs))
            },
        })
    }

    fn union(&self, other: &Self) -> Self {
        PrivacyOp(match (&self.0, &other.0) {
            (PrivacyScheme::DifferentialPrivacy(lhs), PrivacyScheme::DifferentialPrivacy(rhs)) => {
                PrivacyScheme::DifferentialPrivacy(*lhs.max(rhs))
            },
        })
    }
}

/// The full-fledged policy label with downgrading operators attached.
#[derive(Clone, Debug)]
pub enum PolicyLabel {
    PolicyBot,
    PolicyTransform { ops: TransformOps },
    PolicyAgg { ops: AggOps },
    PolicyNoise { ops: PrivacyOp },
    PolicyTop,
}

/// Denotes the policy that is applied to each individual cell.
#[derive(Clone, Debug)]
pub enum Policy<T>
where
    T: Lattice,
{
    /// No policy is applied.
    PolicyClean,
    /// A declassfiication policy is applied.
    PolicyDeclassify {
        /// The label of the policy.
        label: T,
        /// The next policy in the chain.
        next: Box<Self>,
    },
}

impl PolicyLabel {
    /// Checks if the current label `ℓ` can be downgraded to the label `ℓ'`.
    pub fn can_declassify(&self, other: &Self) -> bool {
        match &(self, other) {
            (PolicyLabel::PolicyBot, _) => true,
            (
                PolicyLabel::PolicyTransform { ops: lhs },
                PolicyLabel::PolicyTransform { ops: rhs },
            ) => rhs.is_subset(lhs),
            (PolicyLabel::PolicyAgg { ops: lhs }, PolicyLabel::PolicyAgg { ops: rhs }) => {
                rhs.is_subset(lhs)
            },
            (PolicyLabel::PolicyNoise { ops: lhs }, PolicyLabel::PolicyNoise { ops: rhs }) => {
                rhs.is_subset(lhs)
            },
            (lhs, rhs) => lhs == rhs,
        }
    }

    /// The implementation for the `policy_base_label_eq` function.
    pub fn base_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PolicyLabel::PolicyBot, PolicyLabel::PolicyBot)
            | (PolicyLabel::PolicyTransform { .. }, PolicyLabel::PolicyTransform { .. })
            | (PolicyLabel::PolicyAgg { .. }, PolicyLabel::PolicyAgg { .. })
            | (PolicyLabel::PolicyNoise { .. }, PolicyLabel::PolicyNoise { .. })
            | (PolicyLabel::PolicyTop, PolicyLabel::PolicyTop) => true,
            _ => false,
        }
    }
}

impl fmt::Display for PolicyLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyLabel::PolicyBot => write!(f, "⊥"),
            PolicyLabel::PolicyTransform { ops } => write!(f, "Transform({:?})", ops),
            PolicyLabel::PolicyAgg { ops } => write!(f, "Agg({:?})", ops),
            PolicyLabel::PolicyNoise { ops } => write!(f, "Noise({:?})", ops),
            PolicyLabel::PolicyTop => write!(f, "⊤"),
        }
    }
}

impl PartialEq for PolicyLabel {
    /// The implementation for the `policy_label_eq` function.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PolicyLabel::PolicyBot, PolicyLabel::PolicyBot)
            | (PolicyLabel::PolicyTop, PolicyLabel::PolicyTop) => true,
            (
                PolicyLabel::PolicyTransform { ops: lhs },
                PolicyLabel::PolicyTransform { ops: rhs },
            ) => lhs.set_eq(rhs),
            (PolicyLabel::PolicyAgg { ops: lhs }, PolicyLabel::PolicyAgg { ops: rhs }) => {
                lhs.set_eq(rhs)
            },
            (PolicyLabel::PolicyNoise { ops: lhs }, PolicyLabel::PolicyNoise { ops: rhs }) => {
                lhs.set_eq(rhs)
            },
            _ => false,
        }
    }
}

impl Lattice for PolicyLabel {
    fn join(&self, other: &Self) -> Self {
        match (self, other) {
            (PolicyLabel::PolicyBot, _) => other.clone(),
            (PolicyLabel::PolicyTransform { .. }, PolicyLabel::PolicyBot) => self.clone(),
            (
                PolicyLabel::PolicyTransform { ops: lhs },
                PolicyLabel::PolicyTransform { ops: rhs },
            ) => PolicyLabel::PolicyTransform {
                ops: lhs.intersection(rhs),
            },
            (PolicyLabel::PolicyTransform { .. }, _) => other.clone(),
            (
                PolicyLabel::PolicyAgg { .. },
                PolicyLabel::PolicyBot | PolicyLabel::PolicyTransform { .. },
            ) => self.clone(),
            (PolicyLabel::PolicyAgg { ops: lhs }, PolicyLabel::PolicyAgg { ops: rhs }) => {
                PolicyLabel::PolicyAgg {
                    ops: lhs.intersection(rhs),
                }
            },
            (PolicyLabel::PolicyAgg { .. }, _) => other.clone(),
            (
                PolicyLabel::PolicyNoise { .. },
                PolicyLabel::PolicyBot
                | PolicyLabel::PolicyTransform { .. }
                | PolicyLabel::PolicyAgg { .. },
            ) => self.clone(),
            (PolicyLabel::PolicyNoise { ops: lhs }, PolicyLabel::PolicyNoise { ops: rhs }) => {
                PolicyLabel::PolicyNoise {
                    ops: lhs.intersection(rhs),
                }
            },
            (PolicyLabel::PolicyNoise { .. }, _) => other.clone(),
            (PolicyLabel::PolicyTop, _) => self.clone(),
        }
    }

    fn meet(&self, other: &Self) -> Self {
        match (self, other) {
            (PolicyLabel::PolicyBot, _) => self.clone(),
            (PolicyLabel::PolicyTransform { .. }, PolicyLabel::PolicyBot) => other.clone(),
            (
                PolicyLabel::PolicyTransform { ops: lhs },
                PolicyLabel::PolicyTransform { ops: rhs },
            ) => PolicyLabel::PolicyTransform {
                ops: lhs.union(rhs),
            },
            (PolicyLabel::PolicyTransform { .. }, _) => self.clone(),
            (
                PolicyLabel::PolicyAgg { .. },
                PolicyLabel::PolicyBot | PolicyLabel::PolicyTransform { .. },
            ) => self.clone(),
            (PolicyLabel::PolicyAgg { ops: lhs }, PolicyLabel::PolicyAgg { ops: rhs }) => {
                PolicyLabel::PolicyAgg {
                    ops: lhs.union(rhs),
                }
            },
            (PolicyLabel::PolicyAgg { .. }, _) => self.clone(),
            (
                PolicyLabel::PolicyNoise { .. },
                PolicyLabel::PolicyBot
                | PolicyLabel::PolicyTransform { .. }
                | PolicyLabel::PolicyAgg { .. },
            ) => self.clone(),
            (PolicyLabel::PolicyNoise { ops: lhs }, PolicyLabel::PolicyNoise { ops: rhs }) => {
                PolicyLabel::PolicyNoise {
                    ops: lhs.union(rhs),
                }
            },
            (PolicyLabel::PolicyNoise { .. }, _) => self.clone(),
            (PolicyLabel::PolicyTop, _) => other.clone(),
        }
    }

    fn top() -> Self {
        Self::PolicyTop
    }

    fn bottom() -> Self {
        Self::PolicyBot
    }
}

impl<T> Default for Policy<T>
where
    T: Lattice,
{
    fn default() -> Self {
        Self::PolicyClean
    }
}

impl<T> Policy<T>
where
    T: Lattice,
{
    /// Constructs a new policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// For a policy to be valid, it must be "downgrading".
    pub fn valid(&self) -> bool {
        todo!()
    }

    /// Constructs the policy chain.
    ///
    /// # Example
    ///
    /// ```
    /// use polars_policy::policy::{Policy, PolicyLabel};
    ///
    /// let policy = Policy::new().cons(PolicyLabel::Top).and_then(|p| p.cons(PolicyLabel::Bottom));
    /// ```
    ///
    /// As the above example shows, the policy chain is constructed from top to bottom.
    pub fn cons(self, label: T) -> PicachvResult<Self> {
        match &self {
            Policy::PolicyClean => Ok(Self::PolicyDeclassify {
                label,
                next: Box::new(self),
            }),
            Policy::PolicyDeclassify {
                label: cur,
                next: p,
            } => match cur.flowsto(&label) {
                false => Ok(Self::PolicyDeclassify {
                    label: cur.clone(),
                    next: Box::new(Self::PolicyDeclassify {
                        label,
                        next: p.clone(),
                    }),
                }),
                true => Err(PicachvError::InvalidOperation(
                    "policy label is not ordered correctly".into(),
                )),
            },
        }
    }

    /// The implementation for the `policy_lt` inductive relation.
    pub fn le(&self, other: &Self) -> PicachvResult<bool> {
        picachv_ensure!(self.valid() && other.valid(),
            ComputeError: "trying to compare invalid policies");

        Ok(match (self, other) {
            (Policy::PolicyClean, _) => true,
            (
                Policy::PolicyDeclassify {
                    label: l1,
                    next: n1,
                },
                Policy::PolicyDeclassify {
                    label: l2,
                    next: n2,
                },
            ) => l1.flowsto(l2) && n1.le(n2),
            _ => false,
        })
    }

    /// The implementation for the `policy_join` inductive relation.
    pub fn join(&self, other: &Self) -> PicachvResult<Self> {
        picachv_ensure!(self.valid() && other.valid(),
            ComputeError: "trying to join invalid policies");

        match (self, other) {
            (Policy::PolicyClean, _) => Ok(other.clone()),
            (_, Policy::PolicyClean) => Ok(self.clone()),
            (
                Policy::PolicyDeclassify {
                    label: label1,
                    next: next1,
                },
                Policy::PolicyDeclassify {
                    label: label2,
                    next: next2,
                },
            ) => {
                let (lbl, p3) = match label1.flowsto(label2) {
                    true => (label2, self.join(next2)?),
                    false => (label1, next1.join(other)?),
                };

                Ok(Policy::PolicyDeclassify {
                    label: lbl.clone(),
                    next: Box::new(p3),
                })
            },
        }
    }
}

impl<T> PartialEq for Policy<T>
where
    T: Lattice,
{
    fn eq(&self, other: &Self) -> bool {
        match (self.le(other), other.le(self)) {
            (Ok(true), Ok(true)) => true,
            _ => false,
        }
    }
}

impl<T> PartialOrd for Policy<T>
where
    T: Lattice,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self.le(other), other.le(self)) {
            (Ok(true), Ok(true)) => Some(std::cmp::Ordering::Equal),
            (Ok(true), Ok(false)) => Some(std::cmp::Ordering::Less),
            (Ok(false), Ok(true)) => Some(std::cmp::Ordering::Greater),
            _ => None,
        }
    }
}

impl<T: fmt::Display + Lattice> fmt::Display for Policy<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Policy::PolicyClean => write!(f, "∅"),
            Policy::PolicyDeclassify { label, next } => {
                write!(f, "{label} ⇝ {next}")
            },
        }
    }
}

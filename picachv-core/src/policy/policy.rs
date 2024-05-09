use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::sync::OnceLock;

use ordered_float::OrderedFloat;
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::lattice::Lattice;
use super::types::{AnyValue, DpParam};
use crate::build_policy;
use crate::constants::GroupByMethod;

pub const P_CLEAN: Policy<PolicyLabel> = Policy::PolicyClean;

pub fn p_bot() -> &'static Policy<PolicyLabel> {
    static POLICY: OnceLock<Policy<PolicyLabel>> = OnceLock::new();
    POLICY.get_or_init(|| Policy::PolicyDeclassify {
        label: PolicyLabel::PolicyBot,
        next: Box::new(Policy::PolicyClean),
    })
}

#[inline(always)]
pub fn policy_ok(p: &Policy<PolicyLabel>) -> bool {
    p == &P_CLEAN || p == p_bot()
}

/// Denotes the privacy schemes that should be applied to the result and/or the dataset.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PrivacyScheme {
    /// Differential privacy with a given set of parameters (`epsilon`, `delta`).
    DifferentialPrivacy(DpParam),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryTransformType {
    /// An identity transform.
    Identity,
    /// Redact: completely hides the data.
    Redact,
    /// Length
    Length,
    /// Other custom types.
    Others { name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BinaryTransformType {
    pub name: String,
    // What about the type??
    pub arg: AnyValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransformType {
    Unary(UnaryTransformType),
    Binary(BinaryTransformType),
    // TODO: Not sure what it looks like.
    Others,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggType {
    pub how: GroupByMethod,
    /// The size of the group.
    pub group_size: usize,
}

impl Hash for AggType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self.how {
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
        match self.how == other.how {
            true => self.group_size.partial_cmp(&other.group_size),
            false => None,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOps(pub HashSet<TransformType>);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggOps(pub HashSet<AggType>);
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PolicyLabel {
    PolicyBot,
    PolicyTransform { ops: TransformOps },
    PolicyAgg { ops: AggOps },
    PolicyNoise { ops: PrivacyOp },
    PolicyTop,
}

/// Denotes the policy that is applied to each individual cell.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Policy<T>
where
    T: Lattice + Serialize,
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
    T: Lattice + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::PolicyClean
    }
}

impl<T> Policy<T>
where
    T: Lattice + Serialize + DeserializeOwned,
{
    /// Constructs a new policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// For a policy to be valid, it must be "downgrading".
    pub fn valid(&self) -> bool {
        match self {
            Policy::PolicyClean => true,
            Policy::PolicyDeclassify { label, next } => {
                next.valid()
                    && match next.as_ref() {
                        Policy::PolicyClean => true,
                        Policy::PolicyDeclassify {
                            label: next_label, ..
                        } => next_label.flowsto(label),
                    }
            },
        }
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
                    next: Box::new(p.clone().cons(label)?),
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

        let res = match (self, other) {
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
        };

        Ok(res)
    }
}

impl Policy<PolicyLabel> {
    /// Since this function is called only after we have decided that p_cur ⪯ p_f which means that
    /// the current policy is less or equal to the operation we are about to apply, we can safely
    /// assume that the operation is allowed. So, this function's logic is simple as there are
    /// only two possible cases:
    /// - The current policy is less stricter, then the new policy is the current policy.
    /// - The current policy can be declassified, then the new policy is the declassified policy.
    ///
    ///   In other words, ℓ ⇝ p ⪯ ∘ (op) ==> p_new = p.
    fn do_downgrade(&self, by: &PolicyLabel) -> PicachvResult<Self> {
        match &self {
            // The current policy is less stricter.
            Policy::PolicyClean => Ok(self.clone()),
            Policy::PolicyDeclassify { label, next } => match label.can_declassify(by) {
                true => Ok(next.as_ref().clone()),
                false => Ok(self.clone()),
            },
        }
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
                if label1.base_eq(&label2) {
                    return Ok(
                        Policy::PolicyDeclassify {
                            label: label1.join(label2),
                            next: Box::new(next1.join(next2)?),
                        }
                    )
                }

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

    /// Checks and downgrades the policy by a given label.
    pub fn downgrade(&self, by: PolicyLabel) -> PicachvResult<Self> {
        let p = build_policy!(by.clone())?;
        log::debug!("in downgrade: constructed policy: {p:?}");

        match self.le(&p) {
            Ok(b) => {
                picachv_ensure!(b, PrivacyError: "trying to downgrade by an operation that is not allowed");
                self.do_downgrade(&by)
            },
            Err(e) => Err(e),
        }
    }
}

impl<T> PartialEq for Policy<T>
where
    T: Lattice + Serialize + DeserializeOwned,
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
    T: Lattice + Serialize + DeserializeOwned,
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

impl<T> fmt::Display for Policy<T>
where
    T: fmt::Display + Lattice + Serialize + DeserializeOwned,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Policy::PolicyClean => write!(f, "∅"),
            Policy::PolicyDeclassify { label, next } => {
                write!(f, "{label} ⇝ {next}")
            },
        }
    }
}

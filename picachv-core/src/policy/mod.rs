pub mod context;
pub mod error;
pub mod lattice;
pub mod policy;
pub mod types;

pub use policy::*;

/// A convenience macro for building a policy.
#[macro_export]
macro_rules! build_policy {
    () => { $crate::policy::Policy::default() };
    ($label:expr) => {
        $crate::policy::Policy::new().cons($label)
    };
    ($label:expr $(=> $rest:path)*) => {{
        let mut policy = $crate::policy::Policy::new().cons($label);

        $(
            policy = policy.and_then(|p| p.cons($rest));
        )*

        policy
    }};
}

#[macro_export]
macro_rules! build_unary_expr {
    () => { $crate::policy::PolicyLabel::PolicyTransform {
        ops: Default::default(),
    } };
    ($($ops:expr),*) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(std::collections::HashSet::from_iter(vec![$($ops),*].into_iter())),
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::policy::{Policy, PolicyLabel, TransformOps, TransformType};

    #[test]
    fn test_build_policy() {
        let policy = build_policy!(PolicyLabel::PolicyTransform {
            ops: TransformOps(HashSet::from_iter(vec![TransformType::Shift {by: 1}].into_iter()))
        } => PolicyLabel::PolicyBot);
        assert!(policy.is_ok());
        let policy = build_policy!(PolicyLabel::PolicyTop => PolicyLabel::PolicyBot => PolicyLabel::PolicyTop);
        assert!(policy.is_err());
    }

    #[test]
    fn test_serde_policy() {
        let prev = build_policy!(PolicyLabel::PolicyTransform {
            ops: TransformOps(HashSet::from_iter(vec![TransformType::Shift {by: 1}].into_iter()))
        } => PolicyLabel::PolicyBot)
        .unwrap();
        let policy_str = serde_json::to_string(&prev).unwrap();
        let cur: Policy<PolicyLabel> = serde_json::from_str(&policy_str).unwrap();
        assert_eq!(prev, cur);
    }
}

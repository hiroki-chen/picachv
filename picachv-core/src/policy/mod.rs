pub mod context;
pub mod lattice;
pub mod policy;
pub mod types;

pub use policy::*;

/// A convenience macro for building a policy.
#[macro_export]
macro_rules! build_policy {
    () => { $crate::policy::Policy::default() };
    ($label:expr) => {
        $crate::policy::Policy::new().cons($label.into())
    };
    ($label:expr $(=> $rest:path)*) => {{
        let policy = $crate::policy::Policy::new().cons($label.into());

        $(
            let policy = policy.and_then(|p| p.cons($rest.into()));
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
            ops: $crate::policy::TransformOps(vec![$($ops),*]),
        }
    };
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::constants::GroupByMethod;
    use crate::policy::types::AnyValue;
    use crate::policy::{BinaryTransformType, Policy, PolicyLabel, TransformOps, TransformType};
    use crate::{policy_agg_label, policy_binary_transform_label};

    #[test]
    fn test_build_policy() {
        let policy = build_policy!(PolicyLabel::PolicyTransform {
            ops: TransformOps(vec![TransformType::Binary(BinaryTransformType{name: "dt.offset_by".into(), arg: Arc::new(AnyValue::Duration(Duration::new(5, 0))) })])
        } => PolicyLabel::PolicyBot);
        assert!(policy.is_ok());
        let policy = build_policy!(PolicyLabel::PolicyTop => PolicyLabel::PolicyBot => PolicyLabel::PolicyTop);
        assert!(policy.is_err());
    }

    #[test]
    fn test_serde_policy() {
        let prev = build_policy!(PolicyLabel::PolicyTransform {
            ops: TransformOps(vec![TransformType::Binary(BinaryTransformType{name: "dt.offset_by".into(), arg: Arc::new(AnyValue::Duration(Duration::new(5, 0))) })])
        } => PolicyLabel::PolicyBot)
        .unwrap();
        let policy_str = serde_json::to_string(&prev).unwrap();
        let cur: Policy = serde_json::from_str(&policy_str).unwrap();
        assert_eq!(prev, cur);
    }

    #[test]
    fn test_policy_join() {
        let policy_lhs = build_policy!(PolicyLabel::PolicyTop).unwrap();
        let policy_rhs = policy_binary_transform_label!(
            "dt.offset_by",
            Arc::new(AnyValue::Duration(Duration::new(5, 0)))
        );
        let polich_rhs = build_policy!(policy_rhs.clone()).unwrap();
        let policy_res = build_policy!(PolicyLabel::PolicyTop => policy_rhs).unwrap();

        let res = policy_lhs.join(&polich_rhs);
        assert!(res.is_ok_and(|res| res == policy_res));
    }

    #[test]
    fn test_policy_cmp() {
        let agg1 = build_policy!(policy_agg_label!(GroupByMethod::Sum, 2)).unwrap();
        let agg2 = build_policy!(policy_agg_label!(GroupByMethod::Sum, 5)).unwrap();

        assert!(agg1.le(&agg2).is_ok_and(|b| b));
        assert!(agg2.le(&agg1).is_ok_and(|b| !b));
    }
}

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

#[cfg(test)]
mod tests {
    use crate::policy::PolicyLabel;

    #[test]
    fn test_build_policy() {
        let policy = build_policy!(PolicyLabel::PolicyTop => PolicyLabel::PolicyBot);
        assert!(policy.is_ok());
        let policy = build_policy!(PolicyLabel::PolicyTop=> PolicyLabel::PolicyBot => PolicyLabel::PolicyTop);
        assert!(policy.is_err());
    }
}

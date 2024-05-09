use picachv_core::constants::GroupByMethod;
use picachv_core::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
use picachv_core::io::JsonIO;
use picachv_core::policy::Policy;
use picachv_core::{build_policy, policy_agg_label};

fn main() {
    let mut p1 = vec![];
    let mut p2 = vec![];
    for _ in 0..5 {
        let policy1 = build_policy!(policy_agg_label!(GroupByMethod::Sum, 2)).unwrap();
        let policy2 = Policy::PolicyClean;
        p1.push(policy1);
        p2.push(policy2);
    }
    let col_a = PolicyGuardedColumn::new(p1);
    let col_b = PolicyGuardedColumn::new(p2);

    let policy_df = PolicyGuardedDataFrame::new(vec!["a".into(), "b".into()], vec![col_a, col_b]);

    policy_df.to_json("data/simple_policy2.json").unwrap();
}

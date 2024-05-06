#![allow(unused)]

use std::time::Duration;

use anyhow::{anyhow, Result};
use polars::lazy::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
use polars::lazy::dsl::{col, lit};
use polars::lazy::io::JsonIO;
use polars::lazy::native::{init_monitor, open_new, register_policy_dataframe};
use polars::lazy::policy::types::AnyValue;
use polars::lazy::policy::{Policy, TransformOps};
use polars::lazy::prelude::*;
use polars::lazy::{build_policy, policy_binary_transform_label, PicachvError};
use polars::prelude::*;

fn get_example_df() -> PolicyGuardedDataFrame {
    let df = df!(
        "a" => &[1, 2, 3, 4, 5],
        "b" => &[5, 4, 3, 2, 1],
    )
    .unwrap();

    let mut p1 = vec![];
    let mut p2 = vec![];
    for i in 0..5 {
        let policy1 = build_policy!(policy_binary_transform_label!(
            "dt.offset_by".to_string(),
            AnyValue::Duration(Duration::new(5, 0))
        ))
        .unwrap();
        let policy2 = Policy::PolicyClean;
        p1.push(policy1);
        p2.push(policy2);
    }
    let col_a = PolicyGuardedColumn::new(p1);
    let col_b = PolicyGuardedColumn::new(p2);

    PolicyGuardedDataFrame::new(
        df.schema()
            .get_names()
            .into_iter()
            .map(|e| e.to_owned())
            .collect::<Vec<_>>(),
        vec![col_a, col_b],
    )
}

fn example1(policy: &PolicyGuardedDataFrame) -> Result<DataFrame> {
    let mut df = df! {
        "a" => &[1, 2, 3, 4, 5],
        "b" => &[5, 4, 3, 2, 1]
    }?;

    let ctx_id = open_new()?;
    let uuid = register_policy_dataframe(ctx_id, policy.clone())?;
    df.set_uuid(uuid);

    // This query is equivalent to
    //
    // SELECT b FROM df WHERE 1 < a
    let out = df
        .lazy()
        .select([col("a"), col("b")])
        .filter(lit(1).lt(col("a")))
        .group_by([col("b")])
        .agg(vec![col("a").sum()])
        .set_ctx_id(ctx_id);
    // Error: invalid operation: Possible policy breach detected; abort early.
    // Because we do not apply the policy on the column "b".
    out.collect().map_err(|e| anyhow!(e))
}

fn example2(policy: &PolicyGuardedDataFrame) -> Result<DataFrame> {
    let mut df = df! {
        "a" => &[2, 3, 2, 3, 2],
        "b" => &[5, 4, 3, 2, 1]
    }?;

    let ctx_id = open_new()?;
    let uuid = register_policy_dataframe(ctx_id, policy.clone())?;
    df.set_uuid(uuid);

    // This query is equivalent to
    //
    // SELECT a FROM df WHERE 1 < a
    let out = df
        .lazy()
        .filter(lit(1).lt(col("b")))
        .select([
            col("a").cast(DataType::Date).dt().offset_by(lit("5s")),
            col("b"),
        ])
        .group_by([col("a")])
        .agg(vec![col("b").sum()])
        .set_ctx_id(ctx_id);
    println!("plan: {:?}", out.explain(true));
    // OK.
    out.collect().map_err(|e| anyhow!(e))
}

fn example3(policy: &PolicyGuardedDataFrame) -> Result<DataFrame> {
    let mut df1 = df! {
        "a" => &[1, 2, 3, 4, 5],
        "b" => &[5, 4, 3, 2, 1]
    }?;
    let mut df2 = df! {
        "a" => &[1, 2, 3, 4, 5],
        "b" => &[5, 4, 3, 2, 1]
    }?;

    let ctx_id = open_new()?;
    let uuid1 = register_policy_dataframe(ctx_id, policy.clone())?;
    let uuid2 = register_policy_dataframe(ctx_id, policy.clone())?;
    df1.set_uuid(uuid1);
    df2.set_uuid(uuid2);

    concat([df1.lazy(), df2.lazy()], Default::default())?
        .set_ctx_id(ctx_id)
        .collect()
        .map_err(|e| anyhow!(e))
}

fn main() -> Result<()> {
    env_logger::init();

    get_example_df().to_json("./data/simple_policy.json")?;

    if let Err(e) = init_monitor() {
        match e {
            PicachvError::Already(_) => {},
            _ => return Err(anyhow!("error")),
        }
    }

    let policy = PolicyGuardedDataFrame::from_json("./data/simple_policy.json")?;
    // match example1(&policy) {
    //     Ok(_) => unreachable!("Should not occur!"),
    //     Err(e) => log::error!("Error: {}", e),
    // }
    match example2(&policy) {
        Ok(df) => log::info!("Result: {}", df),
        Err(e) => unreachable!("Error: {}", e),
    }
    // match example3(&policy) {
    //     Ok(df) => unreachable!("Result: {}", df),
    //     Err(e) => log::error!("Error: {}", e),
    // }

    Ok(())
}

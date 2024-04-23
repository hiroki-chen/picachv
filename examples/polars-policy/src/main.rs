use anyhow::{anyhow, Result};
use polars::lazy::dataframe::PolicyGuardedDataFrame;
use polars::lazy::dsl::{col, lit};
use polars::lazy::io::JsonIO;
use polars::lazy::native::{init_monitor, open_new, register_policy_dataframe};
use polars::lazy::prelude::*;
use polars::lazy::PicachvError;
use polars::prelude::*;

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
        .set_ctx_id(ctx_id);
    // Error: invalid operation: Possible policy breach detected; abort early.
    // Because we do not apply the policy on the column "b".
    out.collect().map_err(|e| anyhow!(e))
}

fn example2(policy: &PolicyGuardedDataFrame) -> Result<DataFrame> {
    let mut df = df! {
        "a" => &[1, 2, 3, 4, 5],
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
        .select([col("b")])
        .filter(lit(1).lt(col("b")))
        .set_ctx_id(ctx_id);
    // Error: invalid operation: Possible policy breach detected; abort early.
    out.collect().map_err(|e| anyhow!(e))
}

fn main() -> Result<()> {
    env_logger::init();

    if let Err(e) = init_monitor() {
        match e {
            PicachvError::Already(_) => {},
            _ => return Err(anyhow!("error")),
        }
    }

    let policy = PolicyGuardedDataFrame::from_json("./data/simple_policy.json")?;
    match example1(&policy) {
        Ok(_) => unreachable!("Should not occur!"),
        Err(e) => log::error!("Error: {}", e),
    }
    match example2(&policy) {
        Ok(df) => log::info!("Result: {}", df),
        Err(e) => log::error!("Error: {}", e),
    }

    Ok(())
}

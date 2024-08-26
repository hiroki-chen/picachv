use std::time::Duration;

use polars::frame::DataFrame;
use polars::lazy::native::{enable_profiling, enable_tracing, open_new};
use polars::prelude::{col, lit, LazyFrame, ScanArgsParquet};

fn timer(f: impl FnOnce() -> DataFrame) -> (Duration, DataFrame) {
    let begin = std::time::Instant::now();
    let res = f();
    let end = std::time::Instant::now();

    (end - begin, res)
}

fn main() {
    let sel_wo_policy = timer(|| select(false)).0;
    let sel_w_policy = timer(|| select(true)).0;

    println!("Selection without policy: {:?}", sel_wo_policy);
    println!("Selection with policy: {:?}", sel_w_policy);

    let proj_wo_policy = timer(|| projection(false)).0;
    // let proj_w_policy = timer(|| projection(true)).0;
}

fn select(with_policy: bool) -> DataFrame {
    let path = "../../data/tables/lineitem.parquet";
    let mut scan_arg = ScanArgsParquet {
        ..Default::default()
    };

    if with_policy {
        scan_arg.with_policy =
            Some("../../data/policies/micro/lineitem.parquet.policy.parquet".into());
    }
    let df = LazyFrame::scan_parquet(path, scan_arg).unwrap();
    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, false).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = df
        .filter(col("l_quantity").gt(lit(40)))
        .set_policy_checking(with_policy)
        .set_ctx_id(ctx_id);

    df.collect().unwrap()
}

fn projection(with_policy: bool) -> DataFrame {
    let path = "../../data/tables/lineitem.parquet";
    let mut scan_arg = ScanArgsParquet {
        ..Default::default()
    };

    if with_policy {
        scan_arg.with_policy =
            Some("../../data/policies/micro/lineitem.parquet.policy.parquet".into());
    }
    let df = LazyFrame::scan_parquet(path, scan_arg).unwrap();
    let ctx_id = open_new().unwrap();

    let df = df
        .select(&[col("l_quantity") + lit(1)])
        .set_policy_checking(with_policy)
        .set_ctx_id(ctx_id);

    df.collect().unwrap()
}

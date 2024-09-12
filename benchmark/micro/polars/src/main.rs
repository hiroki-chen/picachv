#![allow(dead_code)]

use std::time::Duration;

use chrono::NaiveDate;
use polars::frame::DataFrame;
use polars::lazy::dsl::*;
use polars::lazy::native::{enable_profiling, enable_tracing, open_new};
use polars::prelude::*;

fn timer(f: impl FnOnce() -> DataFrame) -> (Duration, DataFrame) {
    let begin = std::time::Instant::now();
    let res = f();
    let end = std::time::Instant::now();

    (end - begin, res)
}

fn main() {
    let df = q3(true);
    println!("{:?}", df);
}

fn q3(with_policy: bool) -> DataFrame {
    let lineitem_path = "../../data/tables/lineitem.parquet";
    let orders_path = "../../data/tables/orders.parquet";
    let customer_path = "../../data/tables/customer.parquet";

    let mut lineitem_scan_arg = ScanArgsParquet {
        ..Default::default()
    };
    let mut orders_scan_arg = ScanArgsParquet {
        ..Default::default()
    };
    let mut customer_scan_arg = ScanArgsParquet {
        ..Default::default()
    };

    if with_policy {
        lineitem_scan_arg.with_policy =
            Some("../../data/policies/micro/lineitem.parquet.policy.parquet".into());
        orders_scan_arg.with_policy =
            Some("../../data/policies/micro/orders.parquet.policy.parquet".into());
        customer_scan_arg.with_policy =
            Some("../../data/policies/micro/customer.parquet.policy.parquet".into());
    }

    let lineitem = LazyFrame::scan_parquet(lineitem_path, lineitem_scan_arg).unwrap();
    let orders = LazyFrame::scan_parquet(orders_path, orders_scan_arg).unwrap();
    let customer = LazyFrame::scan_parquet(customer_path, customer_scan_arg).unwrap();

    let lineitem = concat([lineitem.clone(), lineitem], Default::default()).unwrap();

    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let segment = lit("BUILDING");
    let date = lit(NaiveDate::from_ymd_opt(1995, 3, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap());

    let df = customer
        .filter(col("c_mktsegment").eq(segment))
        .join(
            orders,
            [col("c_custkey")],
            [col("o_custkey")],
            JoinArgs::default(),
        )
        .join(
            lineitem,
            [col("o_orderkey")],
            [col("l_orderkey")],
            JoinArgs::default(),
        )
        .filter(col("o_orderdate").lt(date.clone()))
        .filter(col("l_shipdate").gt(date))
        .with_columns([(col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("revenue")])
        .group_by([col("o_orderkey"), col("o_orderdate"), col("o_shippriority")])
        .agg([sum("revenue")])
        .select(&[
            col("o_orderkey").alias("l_orderkey"),
            col("revenue"),
            col("o_orderdate"),
            col("o_shippriority"),
        ])
        .sort(
            ["revenue", "o_orderdate"],
            SortMultipleOptions::new().with_order_descendings([true, false]),
        )
        .set_ctx_id(ctx_id)
        .set_policy_checking(with_policy);

    println!("{}", df.explain(true).unwrap());

    let df = df.collect().unwrap();

    println!("{:?}", df);

    df
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
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = df
        .filter(col("l_quantity").gt(lit(40)))
        .set_policy_checking(with_policy)
        .set_ctx_id(ctx_id);

    df.collect().unwrap_or_default()
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
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = df
        .select(&[col("l_quantity") + lit(1)])
        .set_policy_checking(with_policy)
        .set_ctx_id(ctx_id);

    df.collect().unwrap()
}

fn join(with_policy: bool) -> DataFrame {
    let lineitem_path = "../../data/tables/lineitem.parquet";
    let orders_path = "../../data/tables/orders.parquet";

    let mut lineitem_scan_arg = ScanArgsParquet {
        ..Default::default()
    };
    let mut orders_scan_arg = ScanArgsParquet {
        ..Default::default()
    };

    if with_policy {
        orders_scan_arg.with_policy =
            Some("../../data/policies/micro/lineitem.parquet.policy.parquet".into());
        lineitem_scan_arg.with_policy =
            Some("../../data/policies/micro/orders.parquet.policy.parquet".into());
    }

    let lineitem = LazyFrame::scan_parquet(lineitem_path, lineitem_scan_arg).unwrap();
    let orders = LazyFrame::scan_parquet(orders_path, orders_scan_arg).unwrap();

    let ctx_id = open_new().unwrap();
    enable_profiling(ctx_id, true).unwrap();
    enable_tracing(ctx_id, false).unwrap();

    let df = lineitem
        .select(&[col("l_orderkey")])
        .join(
            orders,
            [col("l_orderkey")],
            [col("o_orderkey")],
            Default::default(),
        )
        .set_policy_checking(with_policy)
        .set_ctx_id(ctx_id);

    df.collect().unwrap()
}

//! This file defines the TPC-H equivalent queries using polars native APIs.

use chrono::NaiveDate;
use polars::lazy::dsl::*;
use polars::prelude::*;

const TABLE_PATH: &str = "../../data/tables/";

pub fn q1() -> PolarsResult<LazyFrame> {
    let df = LazyFrame::scan_parquet(format!("{TABLE_PATH}/lineitem.parquet"), Default::default())?;
    let var_1 = lit(NaiveDate::from_ymd_opt(1998, 9, 2)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap());

    let df = df
        .filter(col("l_shipdate").lt_eq(var_1))
        .group_by([cols(["l_returnflag", "l_linestatus"])])
        .agg([
            sum("l_quantity").alias("sum_qty"),
            sum("l_extendedprice").alias("sum_base_price"),
            (col("l_extendedprice") * (lit(1) - col("l_discount")))
                .sum()
                .alias("sum_disc_price"),
            (col("l_extendedprice") * (lit(1.0) - col("l_discount")) * (lit(1.0) + col("l_tax")))
                .sum()
                .alias("sum_charge"),
            mean("l_quantity").alias("avg_qty"),
            mean("l_extendedprice").alias("avg_price"),
            mean("l_discount").alias("avg_disc"),
            cols(["l_returnflag", "l_linestatus"])
                .count()
                .alias("count_order"),
        ])
        .sort_by_exprs([cols(["l_returnflag", "l_linestatus"])], Default::default());

    Ok(df)
}

pub fn q2() -> PolarsResult<LazyFrame> {
    let part = LazyFrame::scan_parquet(format!("{TABLE_PATH}/part.parquet"), Default::default())?;
    let supplier =
        LazyFrame::scan_parquet(format!("{TABLE_PATH}/supplier.parquet"), Default::default())?;
    let partsupp =
        LazyFrame::scan_parquet(format!("{TABLE_PATH}/partsupp.parquet"), Default::default())?;
    let nation =
        LazyFrame::scan_parquet(format!("{TABLE_PATH}/nation.parquet"), Default::default())?;
    let region =
        LazyFrame::scan_parquet(format!("{TABLE_PATH}/region.parquet"), Default::default())?;

    let size = lit(15);
    let ty = lit("BRASS");
    let region_name = lit("EUROPE");

    // Join all tables
    let df = part
        .join(
            partsupp,
            [col("p_partkey")],
            [col("ps_partkey")],
            JoinArgs::default(),
        )
        .join(
            supplier,
            [col("ps_suppkey")],
            [col("s_suppkey")],
            JoinArgs::default(),
        )
        .join(
            nation,
            [col("s_nationkey")],
            [col("n_nationkey")],
            JoinArgs::default(),
        )
        .join(
            region,
            [col("n_regionkey")],
            [col("r_regionkey")],
            JoinArgs::default(),
        )
        .filter(col("p_size").eq(size))
        .filter(col("p_type").str().ends_with(ty))
        .filter(col("r_name").eq(region_name));

    let df = df
        .clone()
        .group_by([col("p_partkey")])
        .agg([min("ps_supplycost").alias("min_supplycost")])
        .join(
            df,
            [col("p_partkey"), col("ps_supplycost")],
            [col("p_partkey"), col("ps_supplycost")],
            JoinArgs::default(),
        )
        .select(&[
            col("s_acctbal"),
            col("s_name"),
            col("n_name"),
            col("p_partkey"),
            col("p_mfgr"),
            col("s_address"),
            col("s_phone"),
            col("s_comment"),
        ])
        .sort_by_exprs(
            [
                col("s_acctbal"),
                col("n_name"),
                col("s_name"),
                col("p_partkey"),
            ],
            Default::default(), // todo.
        )
        .limit(100);

    Ok(df)
}

//! This file defines the TPC-H equivalent queries using polars native APIs.

use std::collections::HashMap;
use std::path::Path;

use chrono::NaiveDate;
use polars::lazy::dsl::*;
use polars::prelude::*;

/// TPC-H Tables.
const TABLE_NAMES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "supplier", "partsupp", "region",
];

pub struct QueryFactory {
    /// The dataframes registry.
    df_registry: HashMap<String, LazyFrame>,
}

impl QueryFactory {
    pub fn new<P: AsRef<Path>>(table_path: P, with_policy: Option<P>) -> PolarsResult<Self> {
        let mut df_registry = HashMap::new();

        for table in TABLE_NAMES.iter() {
            let path = table_path.as_ref().join(format!("{}.parquet", table));
            let mut scan_arg = ScanArgsParquet::default();
            scan_arg.with_policy = with_policy.as_ref().map(|p| {
                p.as_ref()
                    .to_path_buf()
                    .join(format!("{}.parquet.json", table))
            });

            let df = LazyFrame::scan_parquet(path, scan_arg)?;
            df_registry.insert(table.to_string(), df);
        }

        Ok(Self { df_registry })
    }

    pub fn q1(&self) -> PolarsResult<LazyFrame> {
        let df = self.df_registry.get("lineitem").cloned().unwrap();
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
                (col("l_extendedprice")
                    * (lit(1.0) - col("l_discount"))
                    * (lit(1.0) + col("l_tax")))
                .sum()
                .alias("sum_charge"),
                mean("l_quantity").alias("avg_qty"),
                mean("l_extendedprice").alias("avg_price"),
                mean("l_discount").alias("avg_disc"),
                cols(["l_returnflag", "l_linestatus"])
                    .len()
                    .alias("count_order"),
            ])
            .sort_by_exprs([cols(["l_returnflag", "l_linestatus"])], Default::default());

        Ok(df)
    }

    pub fn q2(&self) -> PolarsResult<LazyFrame> {
        let part = self.df_registry.get("part").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();
        let partsupp = self.df_registry.get("partsupp").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let region = self.df_registry.get("region").cloned().unwrap();

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
                [col("p_partkey"), col("min_supplycost")],
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
                SortMultipleOptions::new().with_order_descendings([true, false, false, false]),
            )
            .limit(100);

        Ok(df)
    }

    pub fn q3(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();

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
            .group_by([col("o_orderkey"), col("o_orderdate"), col("o_shippriority")])
            .agg([(col("l_extendedprice") * (lit(1) - col("l_discount")))
                .alias("revenue")
                .sum()])
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
            .limit(10);

        Ok(df)
    }

    pub fn q4(&self) -> PolarsResult<LazyFrame> {
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();

        let date = lit(NaiveDate::from_ymd_opt(1993, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1993, 10, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());

        let df = orders
            .join(
                lineitem.filter(col("l_commitdate").lt(col("l_receiptdate"))),
                [col("o_orderkey")],
                [col("l_orderkey")],
                JoinArgs::new(JoinType::Semi), // EXISTS
            )
            .filter(col("o_orderdate").gt_eq(date))
            .filter(col("o_orderdate").lt(date2))
            .group_by([col("o_orderpriority")])
            .agg([len().alias("order_count")])
            .sort(["o_orderpriority"], Default::default());

        Ok(df)
    }

    pub fn q5(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let region = self.df_registry.get("region").cloned().unwrap();

        let date1 = lit(NaiveDate::from_ymd_opt(1994, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1995, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let asia = lit("ASIA");

        let df = region
            .join(
                nation,
                [col("r_regionkey")],
                [col("n_regionkey")],
                JoinArgs::default(),
            )
            .join(
                customer,
                [col("n_nationkey")],
                [col("c_nationkey")],
                JoinArgs::default(),
            )
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
            .join(
                supplier,
                [col("l_suppkey"), col("n_nationkey")],
                [col("s_suppkey"), col("s_nationkey")],
                JoinArgs::default(),
            )
            .filter(col("r_name").eq(asia))
            .filter(col("l_shipdate").gt(date1))
            .filter(col("o_orderdate").lt(date2))
            .group_by([col("n_name")])
            .agg([(col("l_extendedprice") * (lit(1) - col("l_discount")))
                .sum()
                .alias("revenue")])
            .sort(
                ["revenue"],
                SortMultipleOptions::new().with_order_descending(true),
            );

        Ok(df)
    }

    pub fn q6(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();

        let date1 = lit(NaiveDate::from_ymd_opt(1994, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1995, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let discount1 = lit(0.05);
        let discount2 = lit(0.07);
        let quantity = lit(24);

        let df = lineitem.filter(col("l_shipdate").gt_eq(date1));
        // .filter(col("l_shipdate").lt(date2))
        // .filter(col("l_discount").gt_eq(discount1))
        // .filter(col("l_discount").lt(discount2))
        // .filter(col("l_quantity").lt(quantity));
        // .with_columns([(col("l_extendedprice") * col("l_discount")).alias("revenue")])
        // .select([sum("revenue")]);

        Ok(df)
    }

    pub fn q7(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();

        let from = lit("FRANCE");
        let to = lit("GERMANY");
        let date1 = lit(NaiveDate::from_ymd_opt(1995, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1996, 12, 31)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());

        let n1 = nation.clone().filter(col("n_name").eq(from));
        let n2 = nation.filter(col("n_name").eq(to));

        let q1 = customer
            .clone()
            .join(
                n1.clone(),
                [col("c_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .join(
                orders.clone(),
                [col("c_custkey")],
                [col("o_custkey")],
                JoinArgs::default(),
            )
            .rename(["n_name"], ["cust_nation"])
            .join(
                lineitem.clone(),
                [col("o_orderkey")],
                [col("l_orderkey")],
                JoinArgs::default(),
            )
            .join(
                supplier.clone(),
                [col("l_suppkey")],
                [col("s_suppkey")],
                JoinArgs::default(),
            )
            .join(
                n2.clone(),
                [col("s_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .rename(["n_name"], ["supp_nation"]);

        let q2 = customer
            .join(
                n2,
                [col("c_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .join(
                orders,
                [col("c_custkey")],
                [col("o_custkey")],
                JoinArgs::default(),
            )
            .rename(["n_name"], ["cust_nation"])
            .join(
                lineitem,
                [col("o_orderkey")],
                [col("l_orderkey")],
                JoinArgs::default(),
            )
            .join(
                supplier,
                [col("l_suppkey")],
                [col("s_suppkey")],
                JoinArgs::default(),
            )
            .join(
                n1,
                [col("s_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .rename(["n_name"], ["supp_nation"]);

        let df = concat([q1, q2], UnionArgs::default())
            .unwrap()
            .filter(col("l_shipdate").gt_eq(date1))
            .filter(col("l_shipdate").lt(date2))
            .with_columns([
                (col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("volume"),
                col("l_shipdate").dt().year().alias("l_year"),
            ])
            .group_by([col("supp_nation"), col("cust_nation"), col("l_year")])
            .agg([sum("volume").alias("revenue")])
            .sort(
                ["supp_nation", "cust_nation", "l_year"],
                SortMultipleOptions::default(),
            );

        Ok(df)
    }
}

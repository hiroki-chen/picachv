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
            let scan_arg = ScanArgsParquet {
                with_policy: with_policy.as_ref().map(|p| {
                    p.as_ref()
                        .to_path_buf()
                        .join(format!("{}.parquet.policy.parquet", table))
                }),
                ..Default::default()
            };

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
            .with_columns(
                [(col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("revenue")],
            )
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

        let df = lineitem
            .filter(col("l_shipdate").gt_eq(date1))
            .filter(col("l_shipdate").lt(date2))
            .filter(col("l_discount").gt_eq(discount1))
            .filter(col("l_discount").lt(discount2))
            .filter(col("l_quantity").lt(quantity))
            .with_columns([
                (col("l_extendedprice") * col("l_discount")).alias("revenue"),
                lit(1).alias("_tmp"),
            ])
            // a workaround solution for anonymous function.
            .group_by(["_tmp"])
            .agg([sum("revenue")]);

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

    pub fn q8(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let part = self.df_registry.get("part").cloned().unwrap();
        let region = self.df_registry.get("region").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();

        let brazil = lit("BRAZIL");
        let america = lit("AMERICA");
        let eas = lit("ECONOMY ANODIZED STEEL");
        let date1 = lit(NaiveDate::from_ymd_opt(1995, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1996, 12, 31)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());

        let n1 = nation
            .clone()
            .select([col("n_nationkey"), col("n_regionkey")]);
        let n2 = nation.select([col("n_nationkey"), col("n_name")]);

        let df = part
            .join(
                lineitem,
                [col("p_partkey")],
                [col("l_partkey")],
                JoinArgs::default(),
            )
            .join(
                supplier,
                [col("l_suppkey")],
                [col("s_suppkey")],
                JoinArgs::default(),
            )
            .join(
                orders,
                [col("l_orderkey")],
                [col("o_orderkey")],
                JoinArgs::default(),
            )
            .join(
                customer,
                [col("o_custkey")],
                [col("c_custkey")],
                JoinArgs::default(),
            )
            .join(
                n1,
                [col("c_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .join(
                region,
                [col("n_regionkey")],
                [col("r_regionkey")],
                JoinArgs::default(),
            )
            .filter(col("r_name").eq(america))
            .join(
                n2,
                [col("s_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .filter(col("o_orderdate").gt_eq(date1))
            .filter(col("o_orderdate").gt(date2))
            .filter(col("p_type").eq(eas))
            .select([
                col("o_orderdate").dt().year().alias("o_year"),
                (col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("volume"),
                col("n_name").alias("nation"),
            ])
            .with_columns([when(col("nation").eq(brazil))
                .then(lit(1))
                .otherwise(lit(0))
                .alias("_tmp")])
            .group_by([col("o_year")])
            .agg([sum("_tmp").alias("a"), sum("volume").alias("b")]);

        let df = df.select([(col("a") / col("b")).round(2).alias("mkt_share")]);

        Ok(df)
    }

    pub fn q9(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let part = self.df_registry.get("part").cloned().unwrap();
        let partsupp = self.df_registry.get("partsupp").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();

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
                lineitem,
                [col("p_partkey"), col("ps_suppkey")],
                [col("l_partkey"), col("l_suppkey")],
                JoinArgs::default(),
            )
            .join(
                orders,
                [col("l_orderkey")],
                [col("o_orderkey")],
                JoinArgs::default(),
            )
            .join(
                nation,
                [col("s_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .filter(col("p_name").str().contains(lit("green"), true))
            .select([
                col("n_name").alias("nation"),
                col("o_orderdate").dt().year().alias("o_year"),
                (col("l_extendedprice") * (lit(1) - col("l_discount"))
                    - col("ps_supplycost") * col("l_quantity"))
                .alias("amount"),
            ])
            .group_by([col("nation"), col("o_year")])
            .agg([sum("amount").alias("sum_profit")])
            .sort(
                ["nation", "o_year"],
                SortMultipleOptions::new().with_order_descendings([false, true]),
            );

        Ok(df)
    }

    pub fn q10(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();

        let date1 = lit(NaiveDate::from_ymd_opt(1993, 10, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1994, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());

        let df = customer
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
                nation,
                [col("c_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .filter(col("o_orderdate").lt(date2))
            .filter(col("o_orderdate").gt_eq(date1))
            .filter(col("l_returnflag").eq(lit("R")))
            .group_by([
                col("c_custkey"),
                col("c_name"),
                col("c_acctbal"),
                col("c_phone"),
                col("n_name"),
                col("c_address"),
                col("c_comment"),
            ])
            .agg([(col("l_extendedprice") - (lit(1) - col("l_discount")))
                .sum()
                .alias("revenue")])
            .select([col("*")])
            .sort(["revenue"], Default::default())
            .limit(20);

        Ok(df)
    }

    pub fn q11(&self) -> PolarsResult<LazyFrame> {
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let partsupp = self.df_registry.get("partsupp").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();

        let germany = lit("GERMANY");
        let per = lit(0.0001);

        let df1 = partsupp
            .join(
                supplier,
                [col("ps_suppkey")],
                [col("s_suppkey")],
                JoinArgs::default(),
            )
            .join(
                nation.filter(col("n_name").eq(germany)),
                [col("s_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            );

        let df = df1
            .clone()
            .with_columns([lit(1).alias("_tmp")])
            .group_by(["_tmp"])
            .agg([(col("ps_supplycost") * col("ps_availqty"))
                .sum()
                .alias("tmp")]);

        let df = df1
            .group_by(["ps_partkey"])
            .agg([(col("ps_supplycost") * col("ps_availqty"))
                .sum()
                .alias("value")])
            .cross_join(df)
            .filter(col("value").gt(col("tmp") * per))
            .select([col("ps_partkey"), col("value")])
            .sort(["value"], Default::default());

        Ok(df)
    }

    pub fn q12(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();

        let mail = lit("MAIL");
        let ship = lit("SHIP");
        let date1 = lit(NaiveDate::from_ymd_opt(1994, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1995, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let high = lit(Series::from_iter(vec![
            String::from("1-URGENT"),
            String::from("2-HIGH"),
        ]));

        let df = orders
            .join(
                lineitem,
                [col("o_orderkey")],
                [col("l_orderkey")],
                JoinArgs::default(),
            )
            .filter((col("l_shipmode").eq(mail)).or(col("l_shipmode").eq(ship)))
            .filter(col("l_commitdate").lt(col("l_receiptdate")))
            .filter(col("l_shipdate").lt(col("l_commitdate")))
            .filter(col("l_receiptdate").gt_eq(date1))
            .filter(col("l_receiptdate").lt(date2))
            .with_columns([
                when(col("o_orderpriority").is_in(high.clone()))
                    .then(lit(1))
                    .otherwise(lit(0))
                    .alias("high_line_count"),
                when(not(col("o_orderpriority").is_in(high)))
                    .then(lit(1))
                    .otherwise(lit(0))
                    .alias("low_line_count"),
            ])
            .group_by(["l_shipmode"])
            .agg([sum("high_line_count"), sum("low_line_count")])
            .sort(["l_shipmode"], Default::default());

        Ok(df)
    }

    pub fn q13(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();

        let orders = orders.filter(
            col("o_comment")
                .str()
                .contains(lit("special.*reequests"), true),
        );
        let df = customer
            .join(
                orders,
                [col("c_custkey")],
                [col("o_custkey")],
                JoinArgs::new(JoinType::Inner), //todo. should be left outer join.
            )
            .group_by(["c_custkey"])
            .agg([col("o_orderkey").count().alias("c_count")])
            .group_by(["c_count"])
            .agg([len()])
            .select([col("c_count"), col("len").alias("custdist")])
            .sort(
                ["custdist", "c_count"],
                SortMultipleOptions::new().with_order_descendings([true, true]),
            );

        Ok(df)
    }

    pub fn q14(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let part = self.df_registry.get("part").cloned().unwrap();

        let date1 = lit(NaiveDate::from_ymd_opt(1995, 9, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1995, 10, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());

        let df = lineitem
            .join(
                part,
                [col("l_partkey")],
                [col("p_partkey")],
                JoinArgs::default(),
            )
            .filter(col("l_shipdate").lt(date2))
            .filter(col("l_shipdate").gt_eq(date1))
            .select([
                (lit(100.00)
                    * when(col("p_type").str().contains(lit("PROMO*"), false))
                        .then(col("l_extendedprice") * (lit(1) - col("l_discount")))
                        .otherwise(lit(0)))
                .alias("a"),
                (col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("b"),
            ]);

        let df = df
            .group_by(["a", "b"])
            .agg([sum("a").alias("c"), sum("b").alias("d")]);
        let df = df.select([(col("c") / col("d")).alias("promo_revenue")]);

        Ok(df)
    }

    pub fn q15(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();

        let date1 = lit(NaiveDate::from_ymd_opt(1996, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());
        let date2 = lit(NaiveDate::from_ymd_opt(1996, 4, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap());

        let revenue = lineitem
            .filter(col("l_shipdate").gt_eq(date1))
            .filter(col("l_shipdate").lt(date2))
            .group_by(["l_suppkey"])
            .agg([(col("l_extendedprice") * (lit(1) - col("l_discount")))
                .sum()
                .alias("total_revenue")])
            .select([col("l_suppkey").alias("supplier_no"), col("total_revenue")]);

        let df = supplier
            .join(
                revenue,
                [col("s_suppkey")],
                [col("supplier_no")],
                JoinArgs::default(),
            )
            .with_columns([col("total_revenue").round(2)])
            .select([
                col("s_suppkey"),
                col("s_name"),
                col("s_address"),
                col("s_phone"),
                col("total_revenue"),
            ])
            .sort(["s_suppkey"], Default::default());

        Ok(df)
    }

    pub fn q18(&self) -> PolarsResult<LazyFrame> {
        let customer = self.df_registry.get("customer").cloned().unwrap();
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();

        let var = lit(300);

        let df = lineitem
            .clone()
            .group_by(["l_orderkey"])
            .agg([sum("l_quantity").alias("sum_quantity")])
            .filter(col("sum_quantity").gt(var));

        let df = orders
            .join(
                df,
                [col("o_orderkey")],
                [col("l_orderkey")],
                JoinArgs::new(JoinType::Semi),
            )
            .join(
                customer,
                [col("o_custkey")],
                [col("c_custkey")],
                JoinArgs::default(),
            )
            .join(
                lineitem,
                [col("o_orderkey")],
                [col("l_orderkey")],
                JoinArgs::default(),
            )
            .group_by([
                "c_name",
                "o_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice",
            ])
            .agg([col("l_quantity").sum().alias("col6")])
            .select([
                col("c_name"),
                col("o_custkey").alias("c_custkey"),
                col("o_orderkey"),
                col("o_orderdate").alias("o_orderdat"),
                col("o_totalprice"),
                col("col6"),
            ])
            .sort(
                ["o_totalprice", "o_orderdat"],
                SortMultipleOptions::new().with_order_descendings([true, false]),
            )
            .limit(100);

        Ok(df)
    }

    pub fn q19(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let part = self.df_registry.get("part").cloned().unwrap();

        let brand1 = lit("Brand#12");
        let brand2 = lit("Brand#23");
        let brand3 = lit("Brand#34");
        let container1 = lit(Series::from_iter(vec![
            String::from("SM CASE"),
            String::from("SM BOX"),
            String::from("SM PACK"),
            String::from("SM PKG"),
        ]));
        let container2 = lit(Series::from_iter(vec![
            String::from("MED BAG"),
            String::from("MED BOX"),
            String::from("MED PKG"),
            String::from("MED PACK"),
        ]));
        let container3 = lit(Series::from_iter(vec![
            String::from("LG CASE"),
            String::from("LG BOX"),
            String::from("LG PACK"),
            String::from("LG PKG"),
        ]));
        let shipmode = lit(Series::from_iter(vec![
            String::from("AIR"),
            String::from("AIR REG"),
        ]));
        let shipinstruct = lit("DELIVER IN PERSON");

        let df = part
            .join(
                lineitem,
                [col("p_partkey")],
                [col("l_partkey")],
                JoinArgs::default(),
            )
            .filter(col("l_shipmode").is_in(shipmode))
            .filter(col("l_shipinstruct").eq(shipinstruct));
        let df = df
            .filter(
                ((col("p_brand").eq(brand1))
                    .and(col("p_container").is_in(container1))
                    .and(col("l_quantity").gt(lit(0)))
                    .and(col("l_quantity").lt(lit(10))))
                .or((col("p_brand").eq(brand2))
                    .and(col("p_container").is_in(container2))
                    .and(col("l_quantity").gt(lit(10)))
                    .and(col("l_quantity").lt(lit(20))))
                .or((col("p_brand").eq(brand3))
                    .and(col("p_container").is_in(container3))
                    .and(col("l_quantity").gt(lit(20)))
                    .and(col("l_quantity").lt(lit(30)))),
            )
            .with_columns([lit(1).alias("gb")])
            .group_by(["gb"])
            .agg([(col("l_extendedprice") * (lit(1) - col("l_discount")))
                .sum()
                .alias("revenue")])
            .select([col("revenue")]);

        Ok(df)
    }

    pub fn q21(&self) -> PolarsResult<LazyFrame> {
        let lineitem = self.df_registry.get("lineitem").cloned().unwrap();
        let nation = self.df_registry.get("nation").cloned().unwrap();
        let orders = self.df_registry.get("orders").cloned().unwrap();
        let supplier = self.df_registry.get("supplier").cloned().unwrap();

        let sa = lit("SAUDI ARABIA");

        let df = lineitem
            .clone()
            .group_by(["l_orderkey"])
            .agg([col("l_suppkey").len().alias("n_supp_by_order")])
            .filter(col("n_supp_by_order").gt(lit(1)))
            .join(
                lineitem.filter(col("l_receiptdate").gt(col("l_commitdate"))),
                [col("l_orderkey")],
                [col("l_orderkey")],
                JoinArgs::default(),
            );

        let df = df
            .clone()
            .group_by(["l_orderkey"])
            .agg([col("l_suppkey").len().alias("n_supp_by_order")])
            .join(
                df,
                [col("l_orderkey")],
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
                nation,
                [col("s_nationkey")],
                [col("n_nationkey")],
                JoinArgs::default(),
            )
            .join(
                orders,
                [col("l_orderkey")],
                [col("o_orderkey")],
                JoinArgs::default(),
            )
            .filter(col("n_supp_by_order").eq(lit(1)))
            .filter(col("n_name").eq(sa))
            .filter(col("o_orderstatus").eq(lit("F")))
            .group_by(["s_name"])
            .agg([len().alias("numwait")])
            .sort(
                ["numwait", "s_name"],
                SortMultipleOptions::new().with_order_descendings([true, false]),
            )
            .limit(100);

        Ok(df)
    }
}

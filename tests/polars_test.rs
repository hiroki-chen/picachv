use polars::lazy::dataframe::PolicyGuardedDataFrame;
use polars::lazy::io::JsonIO;
use polars::lazy::native::{init_monitor, open_new, register_policy_dataframe};
use polars::lazy::uuid::Uuid;
use polars::lazy::PicachvError;
use polars::prelude::*;

// FIXME: The json is broken. Regenerate them

fn example_df1() -> DataFrame {
    df! {
        "a" => &[2, 3, 3, 1, 5],
        "b" => &[5, 5, 4, 4, 1]
    }
    .unwrap()
}

fn example_df2() -> DataFrame {
    df! {
        "a" => &["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"],
        "b" => &[1, 2, 3, 4, 5]
    }
    .unwrap()
}

fn example_df3() -> DataFrame {
    df! {
        "a" => &[2, 3, 3, 1, 5],
        "b" => &[5, 5, 5, 5, 5]
    }
    .unwrap()
}

fn get_df(ctx_id: Uuid, get_df: fn() -> DataFrame, path: &str) -> DataFrame {
    let policy = PolicyGuardedDataFrame::from_json(path).unwrap();
    let mut df = get_df();
    let uuid = register_policy_dataframe(ctx_id, policy).unwrap();
    df.set_uuid(uuid);

    df
}

fn init() -> Uuid {
    let _ = env_logger::builder().is_test(true).try_init();

    match init_monitor() {
        Ok(_) | Err(PicachvError::Already(_)) => (),
        Err(e) => panic!("Error: {:?}", e),
    };

    open_new().unwrap()
}

fn prepare(df: fn() -> DataFrame, path: &str) -> (Uuid, DataFrame) {
    let ctx_id = init();
    let df = get_df(ctx_id, df, path);

    (ctx_id, df)
}

#[cfg(test)]
mod polars_tests {
    use polars::lazy::dsl::col;

    use super::*;

    #[test]
    fn test_polars_query_fail() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        // This query is equivalent to
        //
        // SELECT a, b FROM df WHERE 1 < a GROUP BY b AGG SUM(a)
        let out = df
            .lazy()
            .select([col("a"), col("b")])
            .filter(lit(1).lt(col("a")))
            .group_by([col("b")])
            .agg(vec![col("a").sum()])
            .set_ctx_id(ctx_id)
            .set_policy_checking(true)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_query_ok() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        // This query is equivalent to
        //
        // SELECT offset(a, 5), b FROM df WHERE 1 < a GROUP BY a AGG SUM(b)
        let out = df
            .lazy()
            .filter(lit(1).lt(col("b")))
            .select([
                col("a")
                    .cast(DataType::Date)
                    .dt()
                    .offset_by(lit("5s"))
                    .alias("a_ok"),
                col("b"),
            ])
            .group_by([col("a_ok")])
            .agg(vec![col("b").sum()])
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_non_single() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        let lazy = df.lazy();

        fn do_something(mut lazy: LazyFrame) -> LazyFrame {
            for i in 3..5 {
                lazy = lazy.filter(col("a").lt(lit(i)))
            }

            lazy
        }

        let out = do_something(lazy);

        let out = out
            .select([
                col("a").cast(DataType::Date).dt().offset_by(lit("5s")),
                col("b"),
            ])
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        println!("{:?}", out);

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_expr_in_agg_ok() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        let out = df
            .lazy()
            .group_by([col("a").cast(DataType::Date).dt().offset_by(lit("5s"))])
            .agg(vec![col("b").sum()])
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_expr_in_agg_fail() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        // Error: the argument to `offset` is incorrect.
        let out = df
            .clone()
            .lazy()
            .group_by([col("a").cast(DataType::Date).dt().offset_by(lit("4s"))])
            .agg(vec![col("a").sum()])
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());

        // Error: No transform function is applied.
        let out = df
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a").sum()]) // Cannot do it.
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_expr_within_agg_ok() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        let out = df
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a")
                .cast(DataType::Date)
                .dt()
                .offset_by(lit("5s"))
                .sum()])
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        if let Err(ref e) = out {
            eprintln!("{}", e);
        }

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_union_ok() {
        let ctx_id = init();
        let df1 = get_df(ctx_id, example_df1, "../data/simple_policy.json");
        let df2 = get_df(ctx_id, example_df1, "../data/simple_policy.json");

        let out1 = df1.clone().lazy();
        let out2 = df2.clone().lazy();

        let out1 = out1
            .filter(lit(1).lt(col("b")))
            .select([
                col("a").cast(DataType::Date).dt().offset_by(lit("5s")),
                col("b"),
            ])
            .group_by([col("a")])
            .agg(vec![col("b").sum()]);
        let out2 = out2
            .filter(lit(1).lt(col("b")))
            .select([
                col("a").cast(DataType::Date).dt().offset_by(lit("5s")),
                col("b"),
            ])
            .group_by([col("a")])
            .agg(vec![col("b").sum()]);

        let out = concat([out1, out2], Default::default())
            .unwrap()
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        println!("{:?}", out);
        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_union_fail() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        let out1 = df.clone().lazy();
        let out2 = df.clone().lazy().filter(lit(1).lt(col("b")));

        let out = concat([out1, out2], Default::default())
            .unwrap()
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_join_fail() {
        let ctx_id = init();
        let df1 = get_df(ctx_id, example_df1, "../data/simple_policy.json");
        let df2 = get_df(ctx_id, example_df1, "../data/simple_policy.json");

        let out1 = df1.clone().lazy();
        let out2 = df2.clone().lazy();

        let out = out1
            .join(out2, [col("a")], [col("a")], JoinArgs::new(JoinType::Inner))
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        println!("{:?}", out);
        assert!(out.is_err());
    }

    #[test]
    fn test_polars_agg_groupsize_fail() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy2.json");

        let out = df
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a").sum()]) // <- Because the groupsize is ok.
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_agg_groupsize_ok() {
        let (ctx_id, df) = prepare(example_df3, "../data/simple_policy2.json");

        let out = df
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a").sum()]) // <- Because the groupsize is too small.
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();
        println!("{:?}", out);

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_self_join() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        let out = df
            .clone()
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a").sum().alias("sum_a")])
            .join(
                df.lazy(),
                [col("sum_a")],
                [col("a")],
                JoinArgs::new(JoinType::Inner),
            )
            .set_policy_checking(true)
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }
}

#[cfg(test)]
mod polars_sql_tests {
    use polars::sql::SQLContext;

    use super::*;

    #[test]
    fn test_polars_sql_fail() {
        let (ctx_id, df) = prepare(example_df1, "../data/simple_policy.json");

        let sql = r#"
            SELECT a, b + 1
            FROM df
            WHERE 1 < a
        "#;

        let mut sql_ctx = SQLContext::new();
        sql_ctx.register("df", df.lazy());
        let out = sql_ctx.execute(sql);

        assert!(out.is_ok());

        let out = out
            .unwrap()
            .set_ctx_id(ctx_id)
            .set_policy_checking(true)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_sql_ok() {
        let (ctx_id, df) = prepare(example_df2, "../data/simple_policy.json");

        let sql = r#"
            SELECT a, b + 1
            FROM df
            WHERE 1 < b
        "#;

        let mut sql_ctx = SQLContext::new();
        sql_ctx.register("df", df.lazy());
        let out = sql_ctx.execute(sql);

        if let Err(ref e) = out {
            eprintln!("{}", e);
        }

        assert!(out.is_ok());

        let out = out
            .unwrap()
            .set_ctx_id(ctx_id)
            .set_policy_checking(true)
            .collect();
        println!("{:?}", out);
    }
}

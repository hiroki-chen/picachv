#[cfg(test)]
mod polars_tests {
    use polars::lazy::dataframe::PolicyGuardedDataFrame;
    use polars::lazy::dsl::col;
    use polars::lazy::io::JsonIO;
    use polars::lazy::native::{init_monitor, open_new, register_policy_dataframe};
    use polars::lazy::uuid::Uuid;
    use polars::lazy::PicachvError;
    use polars::prelude::*;

    fn get_df(ctx_id: Uuid) -> DataFrame {
        let policy = PolicyGuardedDataFrame::from_json("../data/simple_policy.json").unwrap();
        let mut df = df! {
            "a" => &[2, 3, 3, 1, 5],
            "b" => &[5, 4, 3, 2, 1]
        }
        .unwrap();
        let uuid = register_policy_dataframe(ctx_id, policy).unwrap();
        df.set_uuid(uuid);

        df
    }

    fn init() -> Uuid {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init();

        match init_monitor() {
            Ok(_) | Err(PicachvError::Already(_)) => (),
            Err(e) => panic!("Error: {:?}", e),
        };

        open_new().unwrap()
    }

    fn prepare() -> (Uuid, DataFrame) {
        let ctx_id = init();
        let df = get_df(ctx_id);

        (ctx_id, df)
    }

    #[test]
    fn test_polars_query_fail() {
        let (ctx_id, df) = prepare();

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
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_query_ok() {
        let (ctx_id, df) = prepare();

        // This query is equivalent to
        //
        // SELECT offset(a, 5), b FROM df WHERE 1 < a GROUP BY a AGG SUM(b)
        let out = df
            .lazy()
            .filter(lit(1).lt(col("b")))
            .select([
                col("a").cast(DataType::Date).dt().offset_by(lit("5s")),
                col("b"),
            ])
            .group_by([col("a")])
            .agg(vec![col("b").sum()])
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_expr_in_agg_ok() {
        let (ctx_id, df) = prepare();

        let out = df
            .lazy()
            .group_by([col("a").cast(DataType::Date).dt().offset_by(lit("5s"))])
            .agg(vec![col("b").sum()])
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_expr_in_agg_fail() {
        let (ctx_id, df) = prepare();

        // Error: the argument to `offset` is incorrect.
        let out = df
            .clone()
            .lazy()
            .group_by([col("a").cast(DataType::Date).dt().offset_by(lit("4s"))])
            .agg(vec![col("a").sum()])
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());

        // Error: No transform function is applied.
        let out = df
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a").sum()]) // Cannot do it.
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_expr_within_agg_ok() {
        let (ctx_id, df) = prepare();

        let out = df
            .lazy()
            .group_by([col("b")])
            .agg(vec![col("a")
                .cast(DataType::Date)
                .dt()
                .offset_by(lit("5s"))
                .sum()])
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
        let df1 = get_df(ctx_id);
        let df2 = get_df(ctx_id);

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
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_ok());
    }

    #[test]
    fn test_polars_union_fail() {
        let (ctx_id, df) = prepare();

        let out1 = df.clone().lazy();
        let out2 = df.clone().lazy().filter(lit(1).lt(col("b")));

        let out = concat([out1, out2], Default::default())
            .unwrap()
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }

    #[test]
    fn test_polars_join_fail() {
        let ctx_id = init();
        let df1 = get_df(ctx_id);
        let df2 = get_df(ctx_id);

        let out1 = df1.clone().lazy();
        let out2 = df2.clone().lazy();

        let out = out1
            .join(out2, [col("a")], [col("a")], JoinArgs::new(JoinType::Inner))
            .set_ctx_id(ctx_id)
            .collect();

        assert!(out.is_err());
    }
}

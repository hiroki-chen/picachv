use polars::lazy::dsl::col;
use polars::prelude::*;

fn main() {
    let df = df! {
        "a" => &[1, 1, 1, 1, 1],
        "b" => &[1, 2, 3, 4, 5]
    }
    .unwrap();

    let out = df
        .lazy()
        .group_by([col("a").cast(DataType::Date).dt().offset_by(lit("5s"))])
        .agg(vec![col("b").sum()]);

    println!("plan logical query:\n{}", out.explain(true).unwrap());

    let out = out.collect().unwrap();

    println!("{:?}", out);
}

use polars::lazy::dsl::col;
use polars::prelude::*;

fn main() {
    let df = df! {
        "a" => &[1, 2, 3, 4, 5],
        "b" => &["2020-08-21", "2020-08-22", "2020-08-23", "2020-08-24", "2020-08-25"]
    }
    .unwrap();

    let out = df
        .lazy()
        .filter(lit(1).lt(col("a")))
        .select([col("b").cast(DataType::Date).dt().offset_by(lit("5mo"))])
        .collect()
        .unwrap();

    println!("{:?}", out);
}

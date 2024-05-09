use polars::lazy::dsl::col;
use polars::prelude::*;

fn main() {
    let df = df! {
        "a" => &[1, 2, 3, 4, 5],
        "b" => &[1, 2, 3, 4, 5]
    }
    .unwrap();

    let out = df
        .clone()
        .lazy()
        .join(
            df.clone().lazy(),
            [col("a")],
            [col("a")],
            JoinArgs::new(JoinType::Inner),
        )
        .collect()
        .unwrap();

    println!("{}", out);
}

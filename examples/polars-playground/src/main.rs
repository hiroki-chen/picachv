use polars::prelude::*;

fn main() {
    let df1 = df![
        "a" => [1,2,3],
        "b" => ["l", "m", "n"]
    ]
    .unwrap();
    let df2 = df![
        "a" => [4,2,3],
        "b" => ["x", "y", "z"]
    ]
    .unwrap();

    let df1 = df1.lazy();
    let df2 = df2.lazy();

    let actual = concat(
        vec![df1.clone(), df2.clone()],
        UnionArgs {
            parallel: true,
            rechunk: true,
            to_supertypes: true,
        },
    )
    .unwrap()
    .collect()
    .unwrap();

    println!("{:?}", actual);
}

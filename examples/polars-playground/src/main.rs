use polars::prelude::*;

fn main() {
    let df = LazyFrame::scan_parquet(
        "./data/tables/lineitem.parquet",
        ScanArgsParquet::default(),
    )
    .unwrap();

    println!("{:?}", df.count().collect().unwrap());
}

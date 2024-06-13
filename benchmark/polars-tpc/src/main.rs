use std::time::Duration;

use clap::Parser;
use polars::error::{PolarsError, PolarsResult};
use polars::frame::DataFrame;
use polars::lazy::native::{init_monitor, open_new};
use polars::lazy::PicachvError;

pub mod queries;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(short, long, default_value = "1", help = "The query number to run")]
    query: usize,

    #[clap(short, long, help = "Whether to enable policy checking", action)]
    with_policy_checking: bool,
}

fn timer(f: impl FnOnce() -> DataFrame) -> (Duration, DataFrame) {
    let begin = std::time::Instant::now();
    let res = f();
    let end = std::time::Instant::now();

    (end - begin, res)
}

fn main() -> PolarsResult<()> {
    let args = Args::parse();

    let query = match args.query {
        1 => queries::q1(),
        2 => queries::q2(),
        3 => queries::q3(),
        4 => queries::q4(),
        5 => queries::q5(),
        6 => queries::q6(),
        7 => queries::q7(),
        8..=15 => {
            panic!("Query not implemented");
        },
        _ => panic!("Invalid query number"),
    }?;

    let query = if args.with_policy_checking {
        match init_monitor() {
            Ok(_) | Err(PicachvError::Already(_)) => (),
            Err(e) => panic!("Error: {:?}", e),
        };

        let ctx_id = open_new().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        query.set_policy_checking(true).set_ctx_id(ctx_id)
    } else {
        query
    };

    let (elapsed_time, df) = timer(|| query.collect().unwrap());

    println!("{}", df);
    println!("Elapsed time: {:?}", elapsed_time);

    Ok(())
}

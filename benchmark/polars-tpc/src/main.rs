use std::time::Duration;

use clap::Parser;
use polars::error::{PolarsError, PolarsResult};
use polars::frame::DataFrame;
use polars::lazy::native::{init_monitor, open_new};
use polars::lazy::PicachvError;
use queries::QueryFactory;

pub mod queries;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(short, long, default_value = "1", help = "The query number to run")]
    query: usize,

    #[clap(
        short,
        long,
        default_value = "../../data/tables",
        help = "The path to the tables"
    )]
    tables_path: String,

    #[clap(short, long, help = "The policy path")]
    policy_path: Option<String>,
}

fn timer(f: impl FnOnce() -> DataFrame) -> (Duration, DataFrame) {
    let begin = std::time::Instant::now();
    let res = f();
    let end = std::time::Instant::now();

    (end - begin, res)
}

fn main() -> PolarsResult<()> {
    let args = Args::parse();
    let qf = QueryFactory::new(&args.tables_path, args.policy_path.as_ref())?;

    let query = match args.query {
        1 => qf.q1(),
        2 => qf.q2(),
        3 => qf.q3(),
        4 => qf.q4(),
        5 => qf.q5(),
        6 => qf.q6(),
        7 => qf.q7(),
        8 => qf.q8(),
        9 => qf.q9(),
        10 => qf.q10(),
        11 => qf.q11(),
        12 => qf.q12(),
        13 => qf.q13(),
        14 => qf.q14(),
        15 => qf.q15(),
        // 16 => qf.q16(),
        16..=22 => {
            panic!("Query not implemented");
        },
        _ => panic!("Invalid query number"),
    }?;

    let query = if args.policy_path.is_some() {
        match init_monitor() {
            Ok(_) | Err(PicachvError::Already(_)) => (),
            Err(e) => panic!("Error: {:?}", e),
        };

        let ctx_id = open_new().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        query.set_policy_checking(true).set_ctx_id(ctx_id)
    } else {
        query
    };

    println!("{}", query.explain(true)?);

    let (elapsed_time, df) = timer(|| query.collect().unwrap());

    println!("{}", df);
    println!("Elapsed time: {:?}", elapsed_time);

    Ok(())
}

use std::time::Duration;

use clap::Parser;
use polars::error::{PolarsError, PolarsResult};
use polars::frame::DataFrame;
use polars::lazy::native::{enable_profiling, enable_tracing, open_new};
use queries::{QueryFactory, QUERY_NUM};

pub mod queries;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(short, long, default_value = "1", help = "The query number to run")]
    query: usize,

    #[clap(
        short,
        long,
        default_value = "false",
        help = "Run all queries in sequence"
    )]
    run_all: bool,

    #[clap(
        short,
        long,
        default_value = "../../data/tables",
        help = "The path to the tables"
    )]
    tables_path: String,

    #[clap(short, long, help = "The policy path")]
    policy_path: Option<String>,

    #[clap(long, help = "Enable tracing")]
    enable_tracing: bool,

    #[clap(long, help = "Enable profiling")]
    enable_profiling: bool,
}

fn timer(f: impl FnOnce() -> DataFrame) -> (Duration, DataFrame) {
    let begin = std::time::Instant::now();
    let res = f();
    let end = std::time::Instant::now();

    (end - begin, res)
}

fn do_query(qf: &QueryFactory, qn: usize, args: &Args) -> PolarsResult<Duration> {
    let query = match qn {
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
        18 => qf.q18(),
        19 => qf.q19(),
        21 => qf.q21(),
        // 16 => qf.q16(),
        16..=22 => {
            panic!("Query not implemented");
        },
        _ => panic!("Invalid query number"),
    }?;

    let query = if args.policy_path.is_some() {
        let ctx_id = open_new().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        enable_tracing(ctx_id, args.enable_tracing).unwrap();
        enable_profiling(ctx_id, args.enable_profiling).unwrap();

        query.set_policy_checking(true).set_ctx_id(ctx_id)
    } else {
        query
    };

    let (elapsed_time, _) = timer(|| query.collect().unwrap());

    Ok(elapsed_time)
}

fn main() -> PolarsResult<()> {
    let args = Args::parse();
    let qf = QueryFactory::new(&args.tables_path, args.policy_path.as_ref())?;

    if args.run_all {
        for qn in QUERY_NUM {
            let elapsed_time = do_query(&qf, qn, &args)?;
            println!("Query {} took {:?}", qn, elapsed_time);
        }
    } else {
        let elapsed_time = do_query(&qf, args.query, &args)?;
        println!("Query {} took {:?}", args.query, elapsed_time);
    }

    Ok(())
}

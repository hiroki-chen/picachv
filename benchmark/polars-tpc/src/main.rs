use std::time::Duration;

use clap::{Parser, ArgAction};

pub mod queries;

#[derive(Parser)]
pub struct Args {
    #[clap(short, long, default_value = "1", help = "The query number to run")]
    query: usize,

    #[clap(
        short,
        long,
        action=ArgAction::SetFalse,
        help = "Whether to enable policy checking"
    )]
    with_policy_checking: bool,
}

fn timer(f: impl FnOnce()) -> Duration {
    let begin = std::time::Instant::now();
    f();
    let end = std::time::Instant::now();

    end - begin
}

fn main() {
    let args = Args::parse();

    let elapsed_time = match args.query {
        1 => {
            timer(|| {
                let _ = queries::q1().unwrap().collect().unwrap();
            })
        },
        2 => {
            timer(|| {
                let _ = queries::q2().unwrap().collect().unwrap();
            })
        },
        3 => {
            timer(|| {
                let _ = queries::q3().unwrap().collect().unwrap();
            })
        },
        4 => {
            timer(|| {
                let _ = queries::q4().unwrap().collect().unwrap();
            })
        },
        5 => {
            timer(|| {
                let _ = queries::q5().unwrap().collect().unwrap();
            })
        },
        6..=15 => {
            panic!("Query not implemented");
        },
        _ => panic!("Invalid query number"),
    };

    println!("Elapsed time: {:?}", elapsed_time);
}

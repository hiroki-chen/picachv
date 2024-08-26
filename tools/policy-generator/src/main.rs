use std::error::Error;
use std::fmt::Display;
use std::fs::{self, File};
use std::result::Result;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use picachv_core::constants::GroupByMethod;
use picachv_core::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
use picachv_core::io::{BinIo, JsonIO};
use picachv_core::policy::types::AnyValue;
use picachv_core::policy::{AggType, BinaryTransformType, Policy, PolicyLabel, TransformType};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Format {
    Json,
    Bin,
    Parquet,
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Format::Json => write!(f, "json"),
            Format::Bin => write!(f, "bin"),
            Format::Parquet => write!(f, "parquet"),
        }
    }
}

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(
        short,
        long,
        default_value = ".",
        help = "The output path of the policy"
    )]
    output_path: String,
    #[clap(
        short,
        long,
        default_value = "../../data/tables",
        help = "The base input path of the parquet file"
    )]
    input_path: String,
    #[clap(
        short,
        long,
        help = "The table name of the parquet file. Leaving empty means all tables in the directory will be processed."
    )]
    table_name: Option<String>,
    #[clap(
        short,
        long,
        default_value = "Json",
        help = "The format of the policy file"
    )]
    format: Format,
    #[clap(long)]
    is_micro: bool,
}

/// A simple generator that produces dummy policies for testing.
pub struct PolicyGenerator {
    args: Args,
}

impl PolicyGenerator {
    pub fn new(args: Args) -> Self {
        Self { args }
    }

    pub fn generate_policy(&self) -> Result<(), Box<dyn Error>> {
        match self.args.table_name.as_ref() {
            Some(table) => {
                let filename = format!("{}/{}.parquet", self.args.input_path, table);
                let df = self.generate_policy_single(&filename)?;

                // Write the policy to a file
                println!("Writing policy to file: {}", table);
                let output_path = format!(
                    "{}/{}.policy.{}",
                    self.args.output_path, table, self.args.format
                );

                match self.args.format {
                    Format::Json => df.to_json(&output_path)?,
                    Format::Bin => df.to_bytes(&output_path)?,
                    Format::Parquet => df.to_parquet(&output_path)?,
                }

                Ok(())
            },
            None => {
                let paths = fs::read_dir(&self.args.input_path)?;
                for path in paths {
                    let path = path?;

                    // Check the file extension
                    if path.path().extension().unwrap() != "parquet" {
                        continue;
                    }
                    let filename = path.path().display().to_string();
                    let df = self.generate_policy_single(&filename)?;

                    // Write the policy to a file
                    println!(
                        "Writing policy to file: {}",
                        path.file_name().to_str().unwrap()
                    );
                    let output_path = format!(
                        "{}/{}.policy.{}",
                        self.args.output_path,
                        path.file_name().to_str().unwrap(),
                        self.args.format
                    );

                    match self.args.format {
                        Format::Json => df.to_json(&output_path)?,
                        Format::Bin => df.to_bytes(&output_path)?,
                        Format::Parquet => df.to_parquet(&output_path)?,
                    }
                }

                Ok(())
            },
        }
    }

    fn generate_policy_single(
        &self,
        filename: &str,
    ) -> Result<PolicyGuardedDataFrame, Box<dyn Error>> {
        println!("Processing file: {}", filename);

        let f = File::open(filename)?;
        let pr = ParquetRecordBatchReaderBuilder::try_new(f)?;
        let col_num = pr.schema().fields.len();
        let row_num = pr.metadata().file_metadata().num_rows();

        println!("Column number: {}, row number: {}", col_num, row_num);

        let mut columns = vec![];
        let mut names = vec![];
        for col in pr.schema().fields.iter() {
            let mut c = vec![];

            let pb = ProgressBar::new(row_num as _);
            pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap().progress_chars("#>-"));

            for _ in 0..row_num {
                let p = if self.args.is_micro {
                    if col.name() == "l_quantity" {
                        Policy::PolicyDeclassify {
                            label: PolicyLabel::PolicyTransform {
                                ops: picachv_core::policy::TransformOps(vec![
                                    TransformType::Binary(BinaryTransformType {
                                        name: "+".into(),
                                        arg: AnyValue::UInt64(1).into(),
                                    }),
                                ]),
                            }
                            .into(),
                            next: Policy::PolicyClean.into(),
                        }
                    } else if col.name() == "l_discount" {
                        Policy::PolicyDeclassify {
                            label: PolicyLabel::PolicyAgg {
                                ops: picachv_core::policy::AggOps(vec![AggType {
                                    how: GroupByMethod::Max,
                                    group_size: 5,
                                }]),
                            }
                            .into(),
                            next: Policy::PolicyClean.into(),
                        }
                    } else {
                        Policy::PolicyClean
                    }
                } else {
                    Policy::PolicyClean
                };

                c.push(p.into());
                pb.inc(1);
            }
            pb.finish_with_message("done");

            columns.push(Arc::new(PolicyGuardedColumn::new_from_iter(c.iter())?));
            names.push(col.name().to_string());
        }

        Ok(PolicyGuardedDataFrame::new(columns))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let pg = PolicyGenerator::new(args);
    pg.generate_policy()
}

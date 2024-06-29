use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{BinaryArray, BooleanArray, RecordBatch};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, RowSelection};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::dataframe::PolicyGuardedDataFrame;
use crate::thread_pool::THREAD_POOL;

#[cfg(feature = "json")]
pub trait JsonIO: Serialize + DeserializeOwned {
    fn to_json<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let file = File::create(path)?;
        serde_json::to_writer(file, self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write JSON: {}", e).into())
        })?;
        Ok(())
    }

    fn to_json_bytes(&self) -> PicachvResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write JSON: {}", e).into())
        })
    }

    fn from_json<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        let now = std::time::Instant::now();
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let result = serde_json::from_reader(reader).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read JSON: {}", e).into())
        })?;
        println!("Time to read JSON: {:?}", now.elapsed());
        Ok(result)
    }

    fn from_json_bytes(bytes: &[u8]) -> PicachvResult<Self> {
        serde_json::from_slice(bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read JSON: {}", e).into())
        })
    }
}

#[cfg(feature = "json")]
impl<T> JsonIO for T where T: Serialize + DeserializeOwned {}

#[cfg(feature = "parquet")]
pub trait ParquetIO: Sized {
    fn to_parquet<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()>;
    fn from_parquet<P: AsRef<Path>>(
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
    ) -> PicachvResult<Self>;
}

#[cfg(feature = "fast_bin")]
pub trait BinIo: Serialize + DeserializeOwned {
    fn to_bytes<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let mut file = File::create(path)?;
        let bytes = bincode::serialize(self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to serialize binary: {}", e).into())
        })?;
        file.write_all(&bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write binary: {}", e).into())
        })?;
        Ok(())
    }

    fn to_byte_array(&self) -> PicachvResult<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to serialize binary: {}", e).into())
        })
    }

    fn from_bytes<P: AsRef<Path>>(path: P) -> PicachvResult<Self> {
        let now = std::time::Instant::now();
        let bytes = fs::read(path).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read binary: {}", e).into())
        })?;
        let res = bincode::deserialize::<Self>(&bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to deserialize binary: {}", e).into())
        })?;

        println!("Time to read binary: {:?}", now.elapsed());
        Ok(res)
    }

    fn from_byte_array(bytes: &[u8]) -> PicachvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to deserialize binary: {}", e).into())
        })
    }
}

#[cfg(feature = "fast_bin")]
impl<T> BinIo for T where T: Serialize + DeserializeOwned {}

#[cfg(all(feature = "parquet", feature = "fast_bin"))]
impl ParquetIO for PolicyGuardedDataFrame {
    fn from_parquet<P: AsRef<Path>>(
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
    ) -> PicachvResult<Self> {
        let file = File::open(path).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to open file: {}", e).into())
        })?;

        let mut builder = ArrowReaderBuilder::try_new(file)
            .map_err(|e| {
                PicachvError::InvalidOperation(
                    format!("Failed to create Parquet reader: {}", e).into(),
                )
            })?
            .with_batch_size(8192);
        let file_metadata = builder.metadata().file_metadata().clone();
        let proj_mask = ProjectionMask::roots(file_metadata.schema_descr(), projection.to_vec());

        builder = builder.with_projection(proj_mask);

        // Do a predicate pushdown.
        if let Some(selection) = selection {
            picachv_ensure!(selection.len() == file_metadata.num_rows() as usize,
                InvalidOperation: "The selection array is not equal to the number of rows in the file"
            );

            // Make selection array as an array of boolean arrays.
            let row_groups = builder.metadata().row_groups();
            let row_groups = THREAD_POOL.install(|| {
                row_groups
                    .par_iter()
                    .map(|rg| rg.num_rows() as usize)
                    .collect::<Vec<_>>()
            });

            // Split the selection array into arrays whose size is the number of rows in each row group.
            let mut selections = vec![];
            for (rg, num_rows) in row_groups.iter().enumerate() {
                let start = rg * num_rows;
                let end = start + num_rows;
                let cur = BooleanArray::from(selection[start..end].to_vec());
                selections.push(cur);
            }

            builder = builder.with_row_selection(RowSelection::from_filters(&selections));
        }

        let mut reader = builder.build().map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to build Parquet reader: {}", e).into())
        })?;

        let rb = reader.try_collect::<Vec<_>>().map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to read Parquet file: {}", e).into())
        })?;

        let rb = arrow_select::concat::concat_batches(&rb[0].schema(), &rb).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to concat batches: {}", e).into())
        })?;

        PolicyGuardedDataFrame::new_from_record_batch(rb)
    }

    fn to_parquet<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
        let bin = THREAD_POOL.install(|| {
            self.columns
                .par_iter()
                .enumerate()
                .map(|(idx, col)| {
                    let policies = col
                        .policies
                        .par_iter()
                        .map(|p| {
                            p.to_byte_array()
                                .map_err(|e| PicachvError::InvalidOperation(e.to_string().into()))
                        })
                        .collect::<PicachvResult<Vec<_>>>()?;
                    let policies = Arc::new(BinaryArray::from_vec(
                        policies.iter().map(|e| e.as_ref()).collect(),
                    )) as _;
                    Ok((format!("col_{idx}"), policies))
                })
                .collect::<PicachvResult<Vec<_>>>()
        })?;

        picachv_ensure!(
            bin.len() == self.columns.len(),
            ComputeError: "The number of columns and the number of binary data are not equal"
        );

        let rb = RecordBatch::try_from_iter(bin).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to create record batch. {e}").into())
        })?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, rb.schema(), None).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to create Arrow writer. {e}").into())
        })?;

        writer.write(&rb).map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to write record batch. {e}").into())
        })?;
        writer.close().map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to close Arrow writer. {e}").into())
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;
    use crate::dataframe::{PolicyGuardedColumn, PolicyGuardedDataFrame};
    use crate::policy::{Policy, PolicyLabel};

    fn test_df() -> PolicyGuardedDataFrame {
        let df = PolicyGuardedColumn::new(vec![
            Arc::new(Policy::PolicyClean),
            Arc::new(Policy::PolicyDeclassify {
                label: PolicyLabel::PolicyTop.into(),
                next: Policy::PolicyClean.into(),
            }),
        ]);
        PolicyGuardedDataFrame::new(vec![Arc::new(df)])
    }

    #[test]
    fn test_parquet_roundabout() {
        let df = test_df();
        let path = env::temp_dir().join("test.parquet");
        let res = df.to_parquet(&path);
        assert!(res.is_ok());

        let df2 = PolicyGuardedDataFrame::from_parquet(&path, &[0], None);
        assert!(df2.is_ok());
        let df2 = df2.unwrap();
        assert_eq!(df, df2);
    }

    #[test]
    fn test_bin_roundabout() {
        let df = test_df();
        let path = env::temp_dir().join("test.bin");
        let res = df.to_bytes(&path);
        assert!(res.is_ok());

        let df2 = PolicyGuardedDataFrame::from_bytes(&path);
        assert!(df2.is_ok());
        let df2 = df2.unwrap();
        assert_eq!(df, df2);
    }

    #[test]
    fn test_json_roundabout() {
        let df = test_df();
        let path = env::temp_dir().join("test.json");
        let res = df.to_json(&path);
        assert!(res.is_ok());

        let df2 = PolicyGuardedDataFrame::from_json(&path);
        assert!(df2.is_ok());
        let df2 = df2.unwrap();
        assert_eq!(df, df2);
    }
}

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{BinaryArray, BooleanArray, RecordBatch};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, RowSelection, SyncReader};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use rayon::prelude::*;

use crate::dataframe::PolicyGuardedDataFrame;
use crate::io::BinIo;
use crate::thread_pool::THREAD_POOL;

/// This constant defines the default size of a row group in a Parquet file.
/// The default vector size of DuckDB is set to this value which we believe
/// is an efficient size for the row group.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 2048;

impl PolicyGuardedDataFrame {
    /// Reads policies from the parquet file.
    ///
    /// # Warnings
    ///
    /// This function can cause subtle problems if not properly used. This is because the policy
    /// dataframe's structure is a sheer mirror of the original dataframe. This fact implies that
    /// the *logical layout*s of these two dataframes must be the same. However, as we store these
    /// two objects in separate Parquet file, there is no guarantee that the file organizations
    /// will always be the same. Thus, the arguments provided to this function should refer to the
    /// loical layout and never to the physical one.
    ///
    /// A typical case would be the size of row groups. By default the size is configured to 8KiB,
    /// but since the sizes of the objects of data and policies are different, the row groups will
    /// have different numbers of rows. Subtle errors will occur if the selection array is not
    /// properly constructed. For example, if the caller is referring to row group #1 and tries to
    /// select directly the row group #1 from the selection array, it will possibly read more rows
    /// than it should; or less.
    ///
    /// For these use cases, please use the [`PolicyGuardedDataFrame::from_parquet_row_group`] method.
    ///
    /// # Links
    ///
    /// [Apache Parquet File Format](https://parquet.apache.org/docs/file-format/)
    pub fn from_parquet<P: AsRef<Path>>(
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
    ) -> PicachvResult<Self> {
        let mut builder = get_initial_builder(path, projection)?;
        let file_metadata = builder.metadata().file_metadata();

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
            // TODO: Parallelize this step.
            // We can first construct a "range" vector and then use it to split the selection array.
            let mut selections = vec![];
            let mut start = 0usize;
            for num_rows in row_groups.iter() {
                let end = start + num_rows;
                let cur = BooleanArray::from(selection[start..end].to_vec());
                selections.push(cur);
                start = end;
            }

            builder = builder.with_row_selection(RowSelection::from_filters(&selections));
        }

        collect_reader(builder)
    }

    /// Reads the policy dataframe from a specific row group with a specified number of rows.
    ///
    /// # Warnings
    ///
    /// Use of this method requires the size of the row group to be [`DEFAULT_ROW_GROUP_SIZE`]
    /// for both the data side and the policy side.
    ///
    /// By default the argument `num_row_read` should be set to this value. However, this value
    /// might be *smaller* than this value which occurs when the last row group num < 2048.
    pub fn from_parquet_row_group<P: AsRef<Path>>(
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
        row_group_index: usize,
    ) -> PicachvResult<Self> {
        let mut builder = get_initial_builder(path, projection)?;
        let metadata = builder.metadata().clone();

        picachv_ensure!(
            row_group_index < builder.metadata().num_row_groups(),
            InvalidOperation: "The row group index {} is out of bound {}",
            row_group_index,
            metadata.num_row_groups(),
        );

        let row_group_meta = metadata.row_group(row_group_index);
        picachv_ensure!(
            row_group_meta.num_rows() as usize <= DEFAULT_ROW_GROUP_SIZE,
            InvalidOperation: "The number of rows in the row group is greater than the default row group size"
        );

        builder = builder.with_row_groups(vec![row_group_index]);
        if let Some(selection) = selection {
            picachv_ensure!(
                selection.len() == row_group_meta.num_rows() as usize,
                InvalidOperation: "The selection array length {} is not equal to `num_rows` {}",
                selection.len(),
                row_group_meta.num_rows()
            );

            builder =
                builder.with_row_selection(RowSelection::from_filters(&[BooleanArray::from(
                    selection.to_vec(),
                )]));
        }

        collect_reader(builder)
    }

    pub fn to_parquet<P: AsRef<Path>>(&self, path: P) -> PicachvResult<()> {
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

        let writer_prop = WriterProperties::builder()
            .set_max_row_group_size(DEFAULT_ROW_GROUP_SIZE)
            // .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
            .build();
        let mut writer =
            ArrowWriter::try_new(file, rb.schema(), Some(writer_prop)).map_err(|e| {
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

fn collect_reader(
    builder: ArrowReaderBuilder<SyncReader<File>>,
) -> Result<PolicyGuardedDataFrame, PicachvError> {
    let mut reader = builder.build().map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to build Parquet reader: {}", e).into())
    })?;

    let rb = reader.try_collect::<Vec<_>>().map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to read Parquet file: {}", e).into())
    })?;

    if rb.is_empty() {
        return Ok(PolicyGuardedDataFrame::new(vec![]));
    }

    let rb = arrow_select::concat::concat_batches(&rb[0].schema(), &rb).map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to concat batches: {}", e).into())
    })?;

    PolicyGuardedDataFrame::new_from_record_batch(rb)
}

/// Gets the initial builder for the Parquet reader with columns projected.
fn get_initial_builder<P: AsRef<Path>>(
    path: P,
    projection: &[usize],
) -> PicachvResult<ArrowReaderBuilder<SyncReader<File>>> {
    let file = File::open(path).map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to open file: {}", e).into())
    })?;

    let mut builder = ArrowReaderBuilder::try_new(file)
        .map_err(|e| {
            PicachvError::InvalidOperation(format!("Failed to create Parquet reader: {}", e).into())
        })?
        .with_batch_size(DEFAULT_ROW_GROUP_SIZE);
    let file_metadata = builder.metadata().file_metadata().clone();
    let proj_mask = ProjectionMask::roots(file_metadata.schema_descr(), projection.to_vec());

    builder = builder.with_projection(proj_mask);

    Ok(builder)
}

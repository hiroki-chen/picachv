#![cfg_attr(feature = "coq", feature(lazy_cell))]
#![cfg_attr(not(feature = "coq"), feature(duration_constructors))]
#![feature(iterator_try_collect)]
#![allow(clippy::module_inception)]

use std::sync::Arc;

use arena::Arena;
pub use arrow_array::{Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use dataframe::DfArena;
use expr::{Expr, ExprArena};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::ExprArgument;
use spin::RwLock;
use uuid::Uuid;

pub mod arena;
pub mod cast;
pub mod constants;
pub mod dataframe;
pub mod expr;
pub mod io;
pub mod macros;
pub mod plan;
pub mod policy;
pub mod profiler;
pub mod thread_pool;
pub mod udf;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// A unified group information that is used to tell Picachv how groups are formed.
///
/// For example, let us consider the following dataframe:
///
/// ```text
/// +---+---+---+
/// | A | B | C |
/// +---+---+---+
/// | 1 | 2 | 3 |
/// | 1 | 2 | 4 |
/// | 1 | 3 | 5 |
/// | 2 | 3 | 6 |
/// +---+---+---+
/// ```
///
/// The group information for the above dataframe is (suppose we group by column A):
///
/// ```text
/// GroupInformation {
///    first: 0,
///    groups: vec![0, 1, 2],
/// },
/// GroupInformation {
///   first: 3,
///   groups: vec![3],
/// }
/// ```
/// 
/// Conceptually, this struct is just a group.
#[derive(Debug, Clone)]
pub struct GroupInformation {
    /// The index of the representative row in the group.
    pub first: usize,
    /// All the indices of the rows of this group.
    pub groups: Vec<usize>,
    /// Optional: the hash identification.
    pub hash: Option<u64>,
}

#[derive(Debug)]
pub struct Arenas {
    pub expr_arena: Arc<RwLock<ExprArena>>,
    pub df_arena: Arc<RwLock<DfArena>>,
}

impl Default for Arenas {
    fn default() -> Self {
        Self::new()
    }
}

impl Arenas {
    pub fn new() -> Self {
        Arenas {
            expr_arena: Arc::new(RwLock::new(ExprArena::new("expr_arena".into()))),
            df_arena: Arc::new(RwLock::new(Arena::new("df_arena".into()))),
        }
    }

    pub fn build_expr(&self, arg: ExprArgument) -> PicachvResult<Uuid> {
        let arg = arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let expr = Expr::from_args(self, arg)?;

        let mut lock = self.expr_arena.write();
        lock.insert(expr)
    }
}

pub fn get_new_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Encode a vector of type-erased arrays into bytes.
pub fn arrays_into_bytes(arrays: Vec<Arc<dyn Array>>) -> PicachvResult<Vec<u8>> {
    let arrays = arrays.into_iter().map(|e| ("", e)).collect::<Vec<_>>();
    let rb = RecordBatch::try_from_iter(arrays).map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to create record batch. {e}").into())
    })?;

    let mut ipc_writer = StreamWriter::try_new(vec![], &rb.schema()).map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to create IPC writer. {e}").into())
    })?;
    ipc_writer.write(&rb).map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to write record batch. {e}").into())
    })?;
    ipc_writer.finish().map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to finish IPC writer. {e}").into())
    })?;

    Ok(ipc_writer.get_ref().to_vec())
}

/// Decode the bytes into the record batch.
pub fn record_batch_from_bytes(value: &[u8]) -> PicachvResult<RecordBatch> {
    let ipc_reader = StreamReader::try_new(value, None).map_err(|e| {
        PicachvError::InvalidOperation(format!("Failed to create IPC reader. {e}").into())
    })?;

    let schema = ipc_reader.schema();

    let rb = ipc_reader
        .map(|e| e.map_err(|e| PicachvError::ComputeError(e.to_string().into())))
        .collect::<PicachvResult<Vec<_>>>()?;

    arrow_select::concat::concat_batches(&schema, &rb)
        .map_err(|e| PicachvError::ComputeError(format!("Failed to concat batches. {e}").into()))
}

#[cfg(test)]
mod tests {}

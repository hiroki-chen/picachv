use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use lazy_static::lazy_static;
use picachv_core::dataframe::DataFrame;
use picachv_error::{PicachvError, PicachvResult};

/// Parameters for the monitor initialization.
pub struct InitParams {}

/// An activate context for the data analysis.
pub struct Context {
    /// The context ID.
    id: usize,
    /// The dataframes to be analyzed.
    dataframes: Vec<Arc<DataFrame>>,
}

impl Context {
    pub fn new(id: usize) -> Self {
        Context {
            id,
            dataframes: Vec::new(),
        }
    }

    pub fn register_dataframe(&mut self, df: DataFrame) {
        self.dataframes.push(Arc::new(df));
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    /// The context map.
    pub(crate) ctx: RwLock<HashMap<usize, Arc<Context>>>,
}

impl PicachvMonitor {
    pub fn new() -> Self {
        PicachvMonitor {
            ctx: RwLock::new(HashMap::new()),
        }
    }

    /// Opens a new context.
    pub fn open_new(&self, params: InitParams) -> PicachvResult<()> {
        let new_id = self
            .ctx
            .read()
            .map_err(|e| PicachvError::ComputeError(e.to_string().into()))?
            .len();
        let ctx = Context::new(new_id);
        todo!()
    }
}

lazy_static! {
    pub static ref MONITOR: PicachvMonitor = PicachvMonitor::new();
}

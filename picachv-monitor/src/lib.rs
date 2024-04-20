use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use picachv_core::{
    dataframe::{DataFrameRegistry, PolicyGuardedDataFrame},
    get_new_uuid, Arenas,
};
use picachv_error::{PicachvError, PicachvResult};
use uuid::Uuid;

/// Parameters for the monitor initialization.
pub struct InitParams {}

/// An activate context for the data analysis.
pub struct Context {
    /// The context ID.
    id: Uuid,
    /// The dataframes to be analyzed.
    df_registry: DataFrameRegistry,
    arena: Arenas,
}

impl Context {
    pub fn new(id: Uuid) -> Self {
        Context {
            id,
            df_registry: DataFrameRegistry::new(),
            arena: Arenas::new(),
        }
    }

    pub fn register_dataframe(&mut self, df: PolicyGuardedDataFrame) -> PicachvResult<Uuid> {
        let uuid = get_new_uuid();
        self.df_registry.insert(uuid, Arc::new(df));

        Ok(uuid)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    /// The context map.
    pub(crate) ctx: RwLock<HashMap<Uuid, Arc<Context>>>,
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
        let ctx = Context::new(get_new_uuid());
        todo!()
    }
}

pub static MONITOR_INSTANCE: OnceLock<PicachvMonitor> = OnceLock::new();

use std::{collections::HashMap, fmt, sync::Arc};

use picachv_error::{PicachvError, PicachvResult};
use uuid::Uuid;

pub type ArenaType<T> = HashMap<Uuid, Arc<T>>;

/// Stores a collection of objects looked up by UUID.
#[derive(Clone, Debug)]
pub struct Arena<T>
where
    T: Clone + fmt::Debug,
{
    pub(crate) inner: ArenaType<T>,
    pub(crate) name: String,
}

impl<T> Arena<T>
where
    T: Clone + fmt::Debug,
{
    /// Inserts a new object into the arena.
    pub fn insert(&mut self, plan: T) -> PicachvResult<Uuid> {
        let uuid = Uuid::new_v4();

        self.inner.insert(uuid, Arc::new(plan));
        Ok(uuid)
    }

    pub fn get(&self, uuid: &Uuid) -> PicachvResult<&Arc<T>> {
        match self.inner.get(uuid) {
            Some(plan) => Ok(plan),
            None => Err(PicachvError::InvalidOperation(
                format!(
                    "The object {uuid} does not exist in the arena {}.",
                    self.name
                )
                .into(),
            )),
        }
    }

    pub fn get_mut(&mut self, uuid: &Uuid) -> PicachvResult<&mut Arc<T>> {
        match self.inner.get_mut(uuid) {
            Some(plan) => Ok(plan),
            None => Err(PicachvError::InvalidOperation(
                format!(
                    "The object {uuid} does not exist in the arena {}.",
                    self.name
                )
                .into(),
            )),
        }
    }

    /// Returns the logical plan with the given `uuid`.
    pub fn new(name: String) -> Self {
        Self {
            inner: HashMap::new(),
            name,
        }
    }
}

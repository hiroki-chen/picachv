use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

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
    #[inline]
    pub fn insert(&mut self, object: T) -> PicachvResult<Uuid> {
        let uuid = Uuid::new_v4();

        self.inner.insert(uuid, Arc::new(object));
        Ok(uuid)
    }

    #[inline]
    pub fn insert_arc(&mut self, plan: Arc<T>) -> PicachvResult<Uuid> {
        let uuid = Uuid::new_v4();

        self.inner.insert(uuid, plan);
        Ok(uuid)
    }

    #[inline]
    pub fn contains_key(&self, uuid: &Uuid) -> bool {
        self.inner.contains_key(uuid)
    }

    pub fn get(&self, uuid: &Uuid) -> PicachvResult<&Arc<T>> {
        match self.inner.get(uuid) {
            Some(plan) => Ok(plan),
            None => Err(PicachvError::InvalidOperation(
                format!(
                    "get: the object {uuid} does not exist in the arena {}.",
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
                    "get_mut: the object {uuid} does not exist in the arena {}.",
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

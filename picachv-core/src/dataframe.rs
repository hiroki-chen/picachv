use std::{collections::HashMap, sync::Arc};

use picachv_error::{PicachvError, PicachvResult};
use polars_core::series::Series;
use uuid::Uuid;

use crate::policy::{Policy, PolicyLabel};

pub type DataFrameRegistry = HashMap<Uuid, Arc<DataFrame>>;

/// A column in a [`DataFrame`] that is guarded by a vector of policies.
///
/// # Design considereration
///
/// Some might think it is more efficient to store the policies within each data
/// cell. However, this is not a good idea because it will make the data structure
/// more complex and harder to maintain. Worse still, some traits that we must
/// implement are private, which means if we really want to implement custom type
/// that carries policy, then we need to fork `polars` and make a lot of changes.
pub struct PolicyGuardedColumn {
    /// The original column which is a type-erased vector.
    pub(crate) column: Series,
    /// Policies for the column.
    pub(crate) policies: Vec<Policy<PolicyLabel>>,
}

impl PolicyGuardedColumn {
    /// Checks the sanity of the column.
    ///
    /// This is very important as it ensures that the column and the policies are in sync.
    pub fn sanity_check(&self) -> PicachvResult<()> {
        match self.column.len() == self.policies.len() {
            true => Ok(()),
            false => Err(PicachvError::InvalidOperation(
                "Column and policies are not in sync!".into(),
            )),
        }
    }
}

/// A contiguous growable collection of `Series` that have the same length.
///
/// This [`DataFrame`] is slightly modified to let the policy into the play: we just
/// add [`polars_core::series::Series`] and policies to it.
pub struct DataFrame {
    pub(crate) columns: Vec<PolicyGuardedColumn>,
}

impl DataFrame {
    pub fn sanity_check(&self) -> PicachvResult<()> {
        self.columns.iter().map(|c| c.sanity_check()).collect()
    }

    /// Gets the names of all columns in this dataframe.
    pub fn get_column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|s| s.column.name()).collect()
    }

    /// Get (height, width) of the [`DataFrame`].
    pub fn shape(&self) -> (usize, usize) {
        match self.columns.as_slice() {
            &[] => (0, 0),
            v => (v[0].column.len(), v.len()),
        }
    }
}

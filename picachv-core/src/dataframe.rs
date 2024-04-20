use std::{collections::HashMap, sync::Arc};

use picachv_error::{PicachvError, PicachvResult};
use polars_core::schema::SchemaRef;
use uuid::Uuid;

use crate::policy::{Policy, PolicyLabel};

pub type DataFrameRegistry = HashMap<Uuid, Arc<PolicyGuardedDataFrame>>;

/// A column in a [`DataFrame`] that is guarded by a vector of policies.
///
/// # Design considereration
///
/// Some might think it is more efficient to store the policies within each data
/// cell. However, this is not a good idea because it will make the data structure
/// more complex and harder to maintain.
///
/// It is thus more efficient to keep policies as a separate vector and ensure that
/// the column and the policies are in sync.
pub struct PolicyGuardedColumn {
    pub(crate) policies: Vec<Policy<PolicyLabel>>,
}

/// A contiguous growable collection of `Series` that have the same length.
///
/// This [`PolicyGuardedDataFrame`] is just a conceptual wrapper around a vector of
/// [`PolicyGuardedColumn`]s. It is not a real data structure; it does not contain
/// any data. It is just a way to group columns together.
pub struct PolicyGuardedDataFrame {
    /// The schema
    pub(crate) schema: SchemaRef,
    /// Policies for the column.
    pub(crate) columns: Vec<PolicyGuardedColumn>,
}

impl PolicyGuardedDataFrame {
    pub fn sanity_check(&self) -> PicachvResult<()> {
        if self.columns.len() != self.schema.len() {
            return Err(PicachvError::InvalidOperation(
                "The number of columns does not match the schema.".into(),
            ));
        }

        Ok(())
    }

    /// Gets the names of all columns in this dataframe.
    pub fn get_column_names(&self) -> Vec<String> {
        self.schema
            .iter_fields()
            .map(|f| f.name().to_string())
            .collect()
    }

    /// Get (height, width) of the [`DataFrame`].
    pub fn shape(&self) -> (usize, usize) {
        match self.columns.as_slice() {
            &[] => (0, 0),
            v => (v[0].policies.len(), v.len()),
        }
    }
}

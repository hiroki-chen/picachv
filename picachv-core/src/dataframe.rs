use std::collections::HashSet;

use picachv_error::{PicachvError, PicachvResult};
use polars_core::{df, schema::SchemaRef};

use crate::{
    build_policy,
    policy::{Policy, PolicyLabel, TransformOps, TransformType},
};

pub fn get_example_df() -> PolicyGuardedDataFrame {
    let df = df!(
        "a" => &[1, 2, 3, 4, 5],
        "b" => &[5, 4, 3, 2, 1],
    )
    .unwrap();

    let policy = build_policy!(PolicyLabel::PolicyTransform {
        ops: TransformOps(HashSet::from_iter(vec![TransformType::Shift {by: 1}].into_iter()))
    } => PolicyLabel::PolicyBot)
    .unwrap();
    let col_a = PolicyGuardedColumn {
        policies: vec![policy; 5],
    };
    let col_b = PolicyGuardedColumn {
        policies: vec![Policy::PolicyClean; 5],
    };

    PolicyGuardedDataFrame {
        schema: df.schema().into(),
        columns: vec![col_a, col_b],
    }
}

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
///
/// # TODOs
///
/// - Sometimes the cell-level policies can be "sparse" which means there is plentiful
///     space for us to optimize. For example, we can "fold" the policy and "expand" it
///     whenever it is needed.
/// - Perhaps we can even make the policy guarded data frame a bitmap or something.
#[derive(Clone, Debug, Default)]
pub struct PolicyGuardedColumn {
    pub(crate) policies: Vec<Policy<PolicyLabel>>,
}

/// A contiguous growable collection of `Series` that have the same length.
///
/// This [`PolicyGuardedDataFrame`] is just a conceptual wrapper around a vector of
/// [`PolicyGuardedColumn`]s. It is not a real data structure; it does not contain
/// any data. It is just a way to group columns together.
#[derive(Clone, Debug, Default)]
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

    /// This checks if we can safely release this dataframe.
    ///
    /// todo: Describe in more detail.
    pub fn finalize(&self) -> PicachvResult<()> {
        for c in self.columns.iter() {
            for p in c.policies.iter() {
                match p {
                    Policy::PolicyClean => continue,
                    _ => {
                        return Err(PicachvError::InvalidOperation(
                            "Possible policy braech detected. Abort early".into(),
                        ))
                    },
                }
            }
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

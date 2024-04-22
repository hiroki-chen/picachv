use std::{collections::HashSet, fmt};

use picachv_error::{PicachvError, PicachvResult};
use polars_core::{df, schema::SchemaRef};
use tabled::{
    builder::Builder,
    settings::{object::Rows, Alignment, Style},
};

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

    let mut p1 = vec![];
    let mut p2 = vec![];
    for i in 0..5 {
        let policy1 = build_policy!(PolicyLabel::PolicyTransform {
            ops: TransformOps(HashSet::from_iter(vec![TransformType::Shift {by: i}].into_iter()))
        } => PolicyLabel::PolicyBot)
        .unwrap();
        let policy2 = Policy::PolicyClean;
        p1.push(policy1);
        p2.push(policy2);
    }
    let col_a = PolicyGuardedColumn { policies: p1 };
    let col_b = PolicyGuardedColumn { policies: p2 };

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

impl fmt::Display for PolicyGuardedDataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = Builder::new();
        let mut header = self
            .schema
            .iter_names()
            .map(|e| e.to_string())
            .collect::<Vec<_>>();
        header.insert(0, "index".to_string());
        builder.push_record(header);

        for i in 0..self.shape().0 {
            let mut row = vec![i.to_string()];
            for j in 0..self.shape().1 {
                row.push(format!("{}", self.columns[j].policies[i]));
            }
            builder.push_record(row);
        }

        write!(
            f,
            "{}",
            builder
                .build()
                .with(Style::rounded())
                .modify(Rows::new(1..), Alignment::left())
                .to_string()
        )
    }
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
        log::debug!("finalizing\n{self}");

        for c in self.columns.iter() {
            for p in c.policies.iter() {
                match p {
                    Policy::PolicyClean => continue,
                    _ => {
                        return Err(PicachvError::InvalidOperation(
                            format!(
                                "Possible policy breach detected; abort early.\n\nThe required policy is\n{self}",
                            )
                            .into(),
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

    /// Apply the filter.
    pub fn filter(&mut self, pred: &[bool]) -> PicachvResult<()> {
        if pred.len() != self.shape().0 {
            return Err(PicachvError::InvalidOperation(
                "The length of the predicate does not match the dataframe.".into(),
            ));
        }

        let mut columns = vec![];
        for c in self.columns.iter() {
            let mut new_policies = vec![];
            c.policies.iter().zip(pred.iter()).for_each(|(p, b)| {
                if *b {
                    new_policies.push(p.clone());
                }
            });
            columns.push(PolicyGuardedColumn {
                policies: new_policies,
            });
        }
        self.columns = columns;

        Ok(())
    }
}

use std::fmt;
use std::sync::Arc;

use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::group_by_proxy::Groups;
use serde::{Deserialize, Serialize};
use tabled::builder::Builder;
use tabled::settings::object::Rows;
use tabled::settings::{Alignment, Style};

use crate::policy::{Policy, PolicyLabel};

pub type Row = Vec<Policy<PolicyLabel>>;

// pub fn get_example_df() -> PolicyGuardedDataFrame {
//     let df = df!(
//         "a" => &[1, 2, 3, 4, 5],
//         "b" => &[5, 4, 3, 2, 1],
//     )
//     .unwrap();

//     let mut p1 = vec![];
//     let mut p2 = vec![];
//     for i in 0..5 {
//         let policy1 = build_policy!(PolicyLabel::PolicyTransform {
//             ops: TransformOps(HashSet::from_iter(vec![TransformType::Binary(BinaryTransformType::ShiftBy {by: Duration::new(i, 0) })].into_iter()))
//         } => PolicyLabel::PolicyBot)
//         .unwrap();
//         let policy2 = Policy::PolicyClean;
//         p1.push(policy1);
//         p2.push(policy2);
//     }
//     let col_a = PolicyGuardedColumn { policies: p1 };
//     let col_b = PolicyGuardedColumn { policies: p2 };

//     PolicyGuardedDataFrame {
//         schema: df.schema().into(),
//         columns: vec![col_a, col_b],
//     }
// }

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
/// - In order to be consistent with the formal model, we should make it indexed by
///   identifiers like UUIDs?
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PolicyGuardedColumn {
    pub(crate) policies: Vec<Policy<PolicyLabel>>,
}

impl PolicyGuardedColumn {
    pub fn new(policies: Vec<Policy<PolicyLabel>>) -> Self {
        PolicyGuardedColumn { policies }
    }
}

/// A contiguous growable collection of `Series` that have the same length.
///
/// This [`PolicyGuardedDataFrame`] is just a conceptual wrapper around a vector of
/// [`PolicyGuardedColumn`]s. It is not a real data structure; it does not contain
/// any data. It is just a way to group columns together.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyGuardedDataFrame {
    /// The schema
    pub(crate) schema: Vec<String>,
    /// Policies for the column.
    pub(crate) columns: Vec<PolicyGuardedColumn>,
}

impl From<Vec<Row>> for PolicyGuardedDataFrame {
    fn from(value: Vec<Row>) -> Self {
        let mut columns = vec![];
        for i in 0..value[0].len() {
            let mut policies = vec![];
            for j in 0..value.len() {
                policies.push(value[j][i].clone());
            }
            columns.push(PolicyGuardedColumn { policies });
        }

        PolicyGuardedDataFrame {
            schema: Vec::new(),
            columns,
        }
    }
}

impl fmt::Display for PolicyGuardedDataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = Builder::new();
        let mut header = self.schema.clone();
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

    /// According to the `groups` struct, fetch the group of columns.
    pub fn groups(&self, groups: &Groups) -> PicachvResult<Self> {
        let mut col = vec![];
        for i in 0..self.columns.len() {
            let mut columns = vec![];

            for g in groups.group.iter() {
                columns.push(self.columns[i].policies[*g as usize].clone());
            }

            col.push(PolicyGuardedColumn { policies: columns });
        }

        Ok(PolicyGuardedDataFrame {
            schema: self.schema.clone(),
            columns: col,
        })
    }

    pub fn row(&self, idx: usize) -> PicachvResult<Vec<Policy<PolicyLabel>>> {
        picachv_ensure!(
            idx < self.shape().0,
            ComputeError: "The index is out of bound.",
        );

        let mut row = vec![];
        for i in 0..self.shape().1 {
            row.push(self.columns[i].policies[idx].clone());
        }

        Ok(row)
    }

    pub fn union(inputs: &[&Arc<Self>]) -> PicachvResult<Self> {
        // Ensures we are really doing unions.
        picachv_ensure!(
            !inputs.is_empty(),
            ComputeError: "Doing union on an empty vector of dataframes is meaningless.",
        );

        // Ensures that the schemas are the same.
        picachv_ensure!(
            inputs.iter().all(|df| df.schema == inputs[0].schema) &&
            inputs.iter().all(|df| df.columns.len() == inputs[0].columns.len()),
            ComputeError: "The schemas of the inputs must be the same.",
        );

        // Do unions.
        let mut columns = vec![];
        for i in 0..inputs[0].columns.len() {
            let mut policies = vec![];
            for j in 0..inputs.len() {
                policies.extend(inputs[j].columns[i].policies.clone());
            }
            columns.push(PolicyGuardedColumn { policies });
        }

        Ok(PolicyGuardedDataFrame {
            schema: inputs[0].schema.clone(),
            columns,
        })
    }

    pub fn new(schema: Vec<String>, columns: Vec<PolicyGuardedColumn>) -> Self {
        PolicyGuardedDataFrame { schema, columns }
    }

    pub(crate) fn early_projection(&mut self, project_list: &[String]) -> PicachvResult<()> {
        // First make sure if the project list contains valid columns.
        for col in project_list.iter() {
            picachv_ensure!(
                self.schema.contains(col),
                ComputeError: format!("The column {} is not in the schema.", col),
            );
        }

        // Then we filter the columns.
        self.schema.retain(|col| project_list.contains(col));

        Ok(())
    }

    /// Convert the [`PolicyGuardedDataFrame`] into a vector of rows.
    pub fn into_rows(&self) -> Vec<Row> {
        let shape = self.shape();

        let mut rows = vec![];
        for i in 0..shape.0 {
            let mut row = vec![];
            for j in 0..shape.1 {
                row.push(self.columns[j].policies[i].clone());
            }
            rows.push(row);
        }
        rows
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

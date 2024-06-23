use std::fmt;
use std::ops::Range;
use std::sync::{Arc, RwLock};

use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::group_by_idx::Groups;
use picachv_message::transform_info::Information;
use picachv_message::{JoinInformation, TransformInfo};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tabled::builder::Builder;
use tabled::settings::object::Rows;
use tabled::settings::{Alignment, Style};
use uuid::Uuid;

use crate::arena::Arena;
use crate::policy::{Policy, PolicyLabel};
use crate::rwlock_unlock;

pub type Row = Vec<Policy<PolicyLabel>>;

pub type DfArena = Arena<PolicyGuardedDataFrame>;

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
#[derive(Clone, Serialize, Deserialize)]
pub struct PolicyGuardedDataFrame {
    /// Policies for the column.
    pub(crate) columns: Vec<PolicyGuardedColumn>,
}

impl From<Vec<Row>> for PolicyGuardedDataFrame {
    fn from(value: Vec<Row>) -> Self {
        let mut columns = vec![];
        for i in 0..value[0].len() {
            let mut policies = vec![];
            for cur in value.iter() {
                policies.push(cur[i].clone());
            }
            columns.push(PolicyGuardedColumn { policies });
        }

        PolicyGuardedDataFrame { columns }
    }
}

impl fmt::Display for PolicyGuardedDataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Debug for PolicyGuardedDataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = Builder::new();
        let mut header = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, _)| format!("column_{}", i))
            .collect::<Vec<_>>();
        header.insert(0, "index".to_string());
        builder.push_record(header);

        for i in (0..self.shape().0).take(15) {
            let mut row = vec![i.to_string()];
            for j in 0..self.shape().1 {
                row.push(format!("{}", self.columns[j].policies[i]));
            }
            builder.push_record(row);
        }

        write!(
            f,
            "\n{}",
            builder
                .build()
                .with(Style::rounded())
                .modify(Rows::new(1..), Alignment::left())
        )
    }
}

impl PolicyGuardedDataFrame {
    pub fn reorder(&mut self, perm: &[usize]) -> PicachvResult<()> {
        self.columns.par_iter_mut().for_each(|c| {
            let policies = perm
                .par_iter()
                .map(|&i| c.policies[i].clone())
                .collect::<Vec<_>>();
            c.policies = policies;
        });

        Ok(())
    }

    pub fn new_from_slice(&self, slices: &[usize]) -> PicachvResult<Self> {
        let columns = self
            .columns
            .par_iter()
            .map(|c| {
                let policies = slices
                    .par_iter()
                    .map(|&i| c.policies[i].clone())
                    .collect::<Vec<_>>();
                PolicyGuardedColumn { policies }
            })
            .collect::<Vec<_>>();

        Ok(PolicyGuardedDataFrame { columns })
    }

    pub fn slice(&self, range: Range<usize>) -> PicachvResult<Self> {
        tracing::debug!("slicing: range = {range:?}");

        picachv_ensure!(
            range.end <= self.shape().0,
            ComputeError: "The range is out of bound.",
        );

        let mut columns = vec![];
        for col in self.columns.iter() {
            let mut policies = vec![];
            for i in range.clone() {
                policies.push(col.policies[i].clone());
            }
            columns.push(PolicyGuardedColumn { policies });
        }

        Ok(PolicyGuardedDataFrame { columns })
    }

    /// Joins two policy-carrying dataframes.
    ///
    /// The function iterates over the `row_info` to join the policies specified by `common_list`. After
    /// this is done, it re-arranges all the columns according to the `output_schema`.
    pub fn join(
        lhs: &PolicyGuardedDataFrame,
        rhs: &PolicyGuardedDataFrame,
        info: &JoinInformation,
    ) -> PicachvResult<Self> {
        // We select columns according to `left_columns` and `right_columns`.
        let left_columns = || {
            info.left_columns
                .par_iter()
                .map(|e| *e as usize)
                .collect::<Vec<_>>()
        };
        let right_columns = || {
            info.right_columns
                .par_iter()
                .map(|e| *e as usize)
                .collect::<Vec<_>>()
        };

        let (left_columns, right_columns) = rayon::join(left_columns, right_columns);
        let (lhs, rhs) = rayon::join(
            || {
                let mut lhs = lhs.clone();
                lhs.projection_by_id(&left_columns)?;
                PicachvResult::Ok(lhs)
            },
            || {
                let mut rhs = rhs.clone();
                rhs.projection_by_id(&right_columns)?;
                PicachvResult::Ok(rhs)
            },
        );

        let (lhs, rhs) = (lhs?, rhs?);

        let (lhs, rhs) = rayon::join(
            || {
                let left_idx = info
                    .row_join_info
                    .par_iter()
                    .map(|e| e.left_row as usize)
                    .collect::<Vec<_>>();
                lhs.new_from_slice(&left_idx)
            },
            || {
                let right_idx = info
                    .row_join_info
                    .par_iter()
                    .map(|e| e.right_row as usize)
                    .collect::<Vec<_>>();
                rhs.new_from_slice(&right_idx)
            },
        );
        let (lhs, rhs) = (lhs?, rhs?);

        // We then stitch them together.
        let res = PolicyGuardedDataFrame::stitch(&lhs, &rhs)?;
        Ok(res)
    }

    /// According to the `groups` struct, fetch the group of columns.
    pub fn groups(&self, groups: &Groups) -> PicachvResult<Self> {
        let col = self
            .columns
            .par_iter()
            .map(|c| {
                let policies = groups
                    .group
                    .par_iter()
                    .map(|g| c.policies[*g as usize].clone())
                    .collect::<Vec<_>>();
                PolicyGuardedColumn { policies }
            })
            .collect::<Vec<_>>();

        Ok(PolicyGuardedDataFrame { columns: col })
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

    /// Stitch two dataframes (veritcally).
    pub fn stitch(
        lhs: &PolicyGuardedDataFrame,
        rhs: &PolicyGuardedDataFrame,
    ) -> PicachvResult<PolicyGuardedDataFrame> {
        tracing::debug!("stitching\n{lhs}\n{rhs}");

        if lhs.columns.is_empty() {
            // semi edge case.
            return Ok(rhs.clone());
        } else if rhs.columns.is_empty() {
            // semi edge case.
            return Ok(lhs.clone());
        }

        picachv_ensure!(
            lhs.shape().0 == rhs.shape().0,
            ComputeError: "The number of rows must be the same: {} != {}", lhs.shape().0, rhs.shape().0
        );

        Ok(PolicyGuardedDataFrame {
            columns: {
                let mut lhs = lhs.columns.clone();
                lhs.extend(rhs.columns.clone());

                lhs
            },
        })
    }

    pub fn union(inputs: &[&Arc<Self>]) -> PicachvResult<Self> {
        // Ensures we are really doing unions.
        picachv_ensure!(
            !inputs.is_empty(),
            ComputeError: "Doing union on an empty vector of dataframes is meaningless.",
        );

        // Ensures that the schemas are the same.
        picachv_ensure!(
            inputs.par_iter().all(|df| df.columns.len() == inputs[0].columns.len()),
            ComputeError: "The schemas of the inputs must be the same.",
        );

        // Do unions.
        let mut columns = vec![];
        for i in 0..inputs[0].columns.len() {
            let mut policies = vec![];
            for input in inputs.iter() {
                policies.extend(input.columns[i].policies.clone());
            }
            columns.push(PolicyGuardedColumn { policies });
        }

        Ok(PolicyGuardedDataFrame { columns })
    }

    #[inline]
    pub fn new(columns: Vec<PolicyGuardedColumn>) -> Self {
        PolicyGuardedDataFrame { columns }
    }

    pub(crate) fn projection_by_id(&mut self, project_list: &[usize]) -> PicachvResult<()> {
        // First make sure if the project list contains valid columns.
        for &col in project_list.iter() {
            picachv_ensure!(
                col < self.columns.len(),
                ComputeError: "The column {} is out of bound", col,
            );
        }

        self.columns = project_list
            .par_iter()
            .map(|&i| self.columns[i].clone())
            .collect();
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
    pub fn finalize(&self) -> PicachvResult<()> {
        tracing::debug!("finalizing\n{self}");

        for c in self.columns.iter() {
            picachv_ensure!(
                c.policies.par_iter().all(
                    |p| matches!(p, Policy::PolicyClean),
                ),
                ComputeError: "Possible policy breach detected; abort early.\n\nThe required policy is\n{self}",
            );
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
        picachv_ensure!(
            pred.len() == self.shape().0,
            ComputeError: "The length of the predicate does not match the dataframe: {} != {}", pred.len(), self.shape().0,
        );

        self.columns = self
            .columns
            .par_iter()
            .map(|c| {
                let policies = c
                    .policies
                    .par_iter()
                    .zip(pred.par_iter())
                    .filter_map(|(p, b)| if *b { Some(p.clone()) } else { None })
                    .collect::<Vec<_>>();

                PolicyGuardedColumn { policies }
            })
            .collect();

        Ok(())
    }
}

/// Apply the transformation on the involved dataframes.
///
/// This function is important for keeping synchronization between the policy and the data.
/// Any operations that alter the schema must send `TransformInfo` to this function to ensure
/// that the policy dataframe is in sync with the data.
pub fn apply_transform(
    df_arena: &Arc<RwLock<DfArena>>,
    df_uuid: Uuid,
    transform: TransformInfo,
) -> PicachvResult<Uuid> {
    match transform.information {
        Some(ti) => match ti {
            Information::Filter(pred) => {
                let mut df_arena = rwlock_unlock!(df_arena, write);
                let df = df_arena.get_mut(&df_uuid)?;

                // We then apply the transformation.
                //
                // We first check if we are holding a strong reference to the dataframe, if so
                // we can directly apply the transformation on the dataframe, otherwise we need
                // to clone the dataframe and apply the transformation on the cloned dataframe.
                // By doing so we can save the memory usage.
                let new_uuid = match Arc::get_mut(df) {
                    Some(df) => {
                        df.filter(&pred.filter)?;
                        println!("after filtering: df.shape() = {:?}", df.shape());
                        // We just re-use the UUID.
                        df_uuid
                    },
                    None => {
                        let mut df = (**df).clone();
                        df.filter(&pred.filter)?;
                        // We insert the new dataframe and this methods returns a new UUID.
                        df_arena.insert(df)?
                    },
                };

                Ok(new_uuid)
            },

            Information::Union(union_info) => {
                let mut df_arena = rwlock_unlock!(df_arena, write);

                let involved_dfs = [&union_info.lhs_df_uuid, &union_info.rhs_df_uuid]
                    .par_iter()
                    .map(|uuid| {
                        let uuid = Uuid::from_slice_le(uuid)
                            .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;
                        df_arena.get(&uuid)
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                // We just union them all.
                let new_df = PolicyGuardedDataFrame::union(&involved_dfs)?;

                // Assign the new UUID.
                df_arena.insert(new_df)
            },

            Information::Join(join) => {
                let mut df_arena = rwlock_unlock!(df_arena, write);

                let lhs = Uuid::from_slice_le(&join.lhs_df_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;
                let rhs = Uuid::from_slice_le(&join.rhs_df_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;

                let lhs_df = df_arena.get(&lhs)?;
                let rhs_df = df_arena.get(&rhs)?;

                let new_df = PolicyGuardedDataFrame::join(lhs_df, rhs_df, &join)?;

                df_arena.insert(new_df)
            },

            Information::Reorder(reorder_info) => {
                let mut df_arena = rwlock_unlock!(df_arena, write);
                let df = df_arena.get_mut(&df_uuid)?;
                // This is the permutation array where arr[i] = j means that the i-th row should be
                // placed with the j-th row.
                let perm = reorder_info
                    .perm
                    .par_iter()
                    .map(|e| *e as usize)
                    .collect::<Vec<_>>();

                // We then apply the transformation.
                match Arc::get_mut(df) {
                    Some(df) => {
                        df.reorder(&perm)?;
                        // We just re-use the UUID.
                        Ok(df_uuid)
                    },
                    None => {
                        let mut df = (**df).clone();
                        df.reorder(&perm)?;
                        // We insert the new dataframe and this methods returns a new UUID.
                        df_arena.insert(df)
                    },
                }
            },

            _ => todo!(),
        },
        None => Ok(df_uuid),
    }
}

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::{Deref, Index};
use std::sync::Arc;

use arrow_array::{BinaryArray, RecordBatch};
use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::transform_info::Information;
use picachv_message::{
    ContextOptions, GroupByIdx, GroupByIdxMultiple, JoinInformation, TransformInfo,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use spin::RwLock;
use tabled::builder::Builder;
use tabled::settings::object::Rows;
use tabled::settings::{Alignment, Style};
use uuid::Uuid;

use crate::arena::Arena;
use crate::expr::Expr;
use crate::io::BinIo;
use crate::plan::groupby_single;
use crate::policy::Policy;
use crate::profiler::PROFILER;
use crate::thread_pool::THREAD_POOL;
use crate::udf::Udf;
use crate::{Arenas, GroupInformation};

pub type PolicyGuardedColumnRef = Arc<PolicyGuardedColumn>;
pub type PolicyRef = Arc<Policy>;
pub type PolicyId = Uuid;
pub type Row = Vec<PolicyId>;

pub type DfArena = Arena<PolicyGuardedDataFrame>;

#[derive(Debug, Clone)]
pub(crate) struct Chunk {
    pub uuid: Uuid,
    pub groups: Vec<GroupInformation>,
}

#[derive(Debug, Clone)]
pub(crate) struct Chunks(Vec<Chunk>);

impl Chunks {
    pub fn new_from_groupby_multiple(gbm: &GroupByIdxMultiple) -> PicachvResult<Self> {
        let chunks = {
            THREAD_POOL.install(|| {
                gbm.chunks
                    .par_iter()
                    .map(|chunk| {
                        Ok(Chunk {
                            uuid: Uuid::from_slice_le(&chunk.uuid).map_err(|e| {
                                PicachvError::InvalidOperation(e.to_string().into())
                            })?,
                            groups: chunk
                                .groups
                                .par_iter()
                                .map(|g| GroupInformation {
                                    first: g.first as _,
                                    groups: g.group.iter().map(|e| *e as _).collect(),
                                    hash: Some(g.hash),
                                })
                                .collect(),
                        })
                    })
                    .collect::<PicachvResult<Vec<_>>>()
            })
        }?;

        Ok(Chunks(chunks))
    }

    fn do_groupby(
        &self,
        arenas: &Arenas,
        keys: &[&Arc<Expr>],
        aggs: &[&Arc<Expr>],
        udfs: &HashMap<String, Udf>,
        hashmap: &HashMap<u64, Vec<(usize, &GroupInformation)>>,
        options: &ContextOptions,
    ) -> PicachvResult<PolicyGuardedDataFrame> {
        if self.0.is_empty() {
            return Ok(Default::default());
        }

        // Don't extend the lifetime of the lock since this causes deadlock otherwise.
        let column_num = arenas
            .df_arena
            .read()
            .get(&self.0.first().unwrap().uuid)?
            .columns
            .len();

        // Iterate over the grouping information which stands for one group.
        // But this time we need to pick rows from different chunks.
        let groups = THREAD_POOL.install(|| {
            hashmap
                .into_par_iter()
                .map(|(_, info)| {
                    // Now we construct for each column the correct rows that
                    // be chosen from each chunk
                    let mut columns = vec![Vec::new(); column_num];
                    for (col_idx, column) in columns.iter_mut().enumerate() {
                        // We pick the rows from the chunks.
                        for (i, group) in info.iter() {
                            let chunk = &self.0[*i];
                            let df_arena = arenas.df_arena.read();
                            let df = df_arena.get(&chunk.uuid)?;

                            for idx in group.groups.iter() {
                                column.push(df.columns[col_idx][*idx].clone());
                            }
                        }
                    }

                    Ok(PolicyGuardedDataFrame {
                        columns: columns
                            .into_par_iter()
                            .map(|c| Ok(Arc::new(PolicyGuardedColumn::new_from_iter(c.iter())?)))
                            .collect::<PicachvResult<Vec<_>>>()?,
                        ..Default::default()
                    })
                })
                .collect::<PicachvResult<Vec<_>>>()
        })?;

        let groups = THREAD_POOL.install(|| {
            groups
                .into_par_iter()
                .map(|g| {
                    let gi = GroupInformation {
                        first: 0,
                        groups: (0..g.shape().0).collect(),
                        hash: None,
                    };
                    Ok(Arc::new(groupby_single(
                        arenas,
                        &Arc::new(g),
                        keys,
                        udfs,
                        options,
                        &[gi],
                        aggs,
                    )?))
                })
                .collect::<PicachvResult<Vec<_>>>()
        })?;

        let mut df = PolicyGuardedDataFrame::union(&groups)?;
        df.additional_info = DfInformation {
            hash_info: hashmap.iter().enumerate().map(|(k, v)| (*v.0, k)).collect(),
        };

        Ok(df)
    }

    pub fn check(
        &self,
        arena: &Arenas,
        keys: &[&Arc<Expr>],
        aggs: &[&Arc<Expr>],
        udfs: &HashMap<String, Udf>,
        options: &ContextOptions,
    ) -> PicachvResult<PolicyGuardedDataFrame> {
        // This algorithm works slightly different since we are on multiple chunks.
        // In this case where multiple chunks need to be grouped, we cannot simply
        // evaluate the groupby operation on each chunk as we did before.
        //
        // We need to collect all the groups and then group them together. Fortunately,
        // this is doable as we have the `hash` field in the `GroupInformation` struct.
        //
        // We now first create a hashmap where the key is the hash value and the value
        // is a vector of tuples, where the first element is the index of the chunk and
        // the second element is the group information.
        //
        // At high level, this map represents groups over groups.
        let mut hashmap = HashMap::new();

        // We then do a one-time pass over the chunks.
        for (i, chunk) in self.0.iter().enumerate() {
            for group in chunk.groups.iter() {
                let hash = group.hash.ok_or(PicachvError::InvalidOperation(
                    "The hash value is missing.".into(),
                ))?;

                hashmap
                    .entry(hash)
                    .or_insert_with(Vec::new)
                    .push((i, group));
            }
        }

        // Now we can finally group them together.
        self.do_groupby(arena, keys, aggs, udfs, &hashmap, options)
    }
}

pub(crate) fn idx_to_group_info_vec(idx: &GroupByIdx) -> Vec<GroupInformation> {
    idx.groups
        .par_iter()
        .map(|group| {
            let first = group.first as usize;
            let groups = group.group.par_iter().map(|e| *e as usize).collect();
            GroupInformation {
                first,
                groups,
                hash: None,
            }
        })
        .collect()
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
/// # Optimizations
///
/// In reality the policies are often sparse which means that most of the cells share
/// the same policy. We can use a bitmap to indicate which cells differ from the base
/// policy.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PolicyGuardedColumn {
    /// The policies for the column.
    pub(crate) base_policy: PolicyRef,
    /// The length of this column.
    pub(crate) len: usize,
    /// The policies for each cell.
    pub(crate) policies: HashMap<usize, PolicyRef>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct PolicyGuardedColumnProxy {
    pub(crate) policies: Vec<PolicyRef>,
}

impl Index<usize> for PolicyGuardedColumn {
    type Output = PolicyRef;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.policies.get(&index).unwrap_or(&self.base_policy)
    }
}

impl PolicyGuardedColumn {
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn new(base_policy: PolicyRef, len: usize, policies: HashMap<usize, PolicyRef>) -> Self {
        PolicyGuardedColumn {
            base_policy,
            len,
            policies,
        }
    }

    pub fn new_from_iter<'a>(iter: impl IntoIterator<Item = &'a PolicyRef>) -> PicachvResult<Self> {
        let mut policies = HashMap::new();
        let mut policy_count = HashMap::new();
        for (i, p) in iter.into_iter().enumerate() {
            policy_count.entry(p).and_modify(|e| *e += 1).or_insert(1);
            policies.insert(i, p.clone());
        }

        let base_policy = THREAD_POOL.install(|| {
            policy_count
                .into_par_iter()
                .max_by_key(|(_, v)| *v)
                .map(|(k, _)| k)
        });

        picachv_ensure!(
            base_policy.is_some() || policies.len() == 0,
            ComputeError: "Failed to find the base policy."
        );

        let base_policy = match base_policy {
            Some(bp) => bp,
            None => &Arc::new(Policy::PolicyClean),
        };

        policies.retain(|_, v: &mut Arc<Policy>| v != base_policy);

        Ok(Self {
            base_policy: base_policy.clone(),
            len: policies.len(),
            policies,
        })
    }

    /// Append to this column.
    pub fn append(&self, other: &Self) -> PicachvResult<Self> {
        todo!("append")
    }

    /// Apply the filter.
    pub fn filter(&self, filter: &[bool]) -> PicachvResult<Self> {
        picachv_ensure!(
            filter.len() == self.len,
            ComputeError: "The length of the filter does not match the column: {} != {}", filter.len(), self.len,
        );

        let slice = filter
            .par_iter()
            .enumerate()
            .filter_map(|(i, b)| if *b { Some(i) } else { None })
            .collect::<Vec<_>>();

        self.new_from_slice(&slice)
    }

    /// According to the `groups` struct, fetch the group of columns.
    pub fn groups(&self, groups: &GroupInformation) -> PicachvResult<Self> {
        let policies = groups
            .groups
            .par_iter()
            .map(|g| self[*g as usize].clone())
            .collect::<Vec<_>>();

        Self::new_from_iter(policies.iter())
    }

    /// Construct a new [`PolicyGuardedColumn`] from a slice of the original object.
    pub fn new_from_slice(&self, slice: &[usize]) -> PicachvResult<Self> {
        Ok(Self {
            base_policy: self.base_policy.clone(),
            len: slice.len(),
            policies: self
                .policies
                .par_iter()
                .filter_map(|(k, v)| {
                    if slice.contains(k) {
                        Some((*k, v.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
        })
    }
}

impl PolicyGuardedColumnProxy {
    pub fn new(policies: Vec<PolicyRef>) -> Self {
        PolicyGuardedColumnProxy { policies }
    }
}

/// Some other additional information for the [`PolicyGuardedDataFrame`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct DfInformation {
    /// Used to quickly look up the index.
    pub(crate) hash_info: HashMap<u64, usize>,
}

/// A contiguous growable collection of `Series` that have the same length.
///
/// This [`PolicyGuardedDataFrame`] is just a conceptual wrapper around a vector of
/// [`PolicyGuardedColumnRef`]s. It is not a real data structure; it does not contain
/// any data. It is just a way to group columns together.
///
/// The reason we use a vector of [`PolicyGuardedColumnRef`]s is that it is more efficient
/// to store the reference to avoid unnecessary cloning.
#[derive(Clone, PartialEq, Default)]
pub struct PolicyGuardedDataFrame {
    /// Policies for the column.
    pub(crate) columns: Vec<PolicyGuardedColumnRef>,
    /// Other additional information.
    pub(crate) additional_info: DfInformation,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct PolicyGuardedDataFrameProxy {
    pub(crate) columns: Vec<PolicyGuardedColumnProxy>,
}

impl From<&PolicyGuardedColumn> for PolicyGuardedColumnProxy {
    fn from(c: &PolicyGuardedColumn) -> Self {
        let mut policies = vec![c.base_policy.clone(); c.len()];

        for (&k, v) in c.policies.iter() {
            policies[k] = v.clone();
        }

        Self { policies }
    }
}

impl From<PolicyGuardedColumnProxy> for PolicyGuardedColumn {
    #[inline]
    fn from(proxy: PolicyGuardedColumnProxy) -> Self {
        Self::new_from_iter(proxy.policies.iter()).expect("Failed to create a new column.")
    }
}

impl From<&PolicyGuardedDataFrame> for PolicyGuardedDataFrameProxy {
    fn from(df: &PolicyGuardedDataFrame) -> Self {
        let columns =
            THREAD_POOL.install(|| df.columns.par_iter().map(|c| c.deref().into()).collect());

        Self { columns }
    }
}

impl From<PolicyGuardedDataFrameProxy> for PolicyGuardedDataFrame {
    fn from(proxy: PolicyGuardedDataFrameProxy) -> Self {
        let columns = THREAD_POOL.install(|| {
            proxy
                .columns
                .into_par_iter()
                .map(|c| Arc::new(c.into()))
                .collect()
        });

        Self {
            columns,
            ..Default::default()
        }
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
                row.push(format!("{}", self.columns[j][i]));
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

impl PolicyGuardedDataFrameProxy {
    pub(crate) fn new_from_record_batch(rb: RecordBatch) -> PicachvResult<Self> {
        let columns = THREAD_POOL.install(|| {
            rb.columns()
                .par_iter()
                .map(|c| {
                    let policies = c
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or(PicachvError::InvalidOperation(
                            "Failed to downcast to BinaryArray.".into(),
                        ))?
                        .iter()
                        .map(|e| {
                            Ok(Arc::new(Policy::from_byte_array(e.unwrap()).map_err(
                                |e| PicachvError::InvalidOperation(e.to_string().into()),
                            )?))
                        })
                        .collect::<PicachvResult<Vec<_>>>()?;
                    Ok(PolicyGuardedColumnProxy { policies })
                })
                .collect::<PicachvResult<Vec<_>>>()
        })?;

        Ok(Self { columns })
    }
}

impl PolicyGuardedDataFrame {
    pub fn reorder(&mut self, _perm: &[usize]) -> PicachvResult<()> {
        // THREAD_POOL.install(|| {
        //     self.columns.par_iter_mut().for_each(|c| {
        //         let policies = perm
        //             .par_iter()
        //             .map(|&i| c.policies[i].clone())
        //             .collect::<Vec<_>>();
        //         // c.policies = policies;
        //     })
        // });

        Ok(())
    }

    /// Constructs a new [`PolicyGuardedDataFrame`] from a [`RecordBatch`].
    #[inline]
    pub fn new_from_record_batch(rb: RecordBatch) -> PicachvResult<Self> {
        PolicyGuardedDataFrameProxy::new_from_record_batch(rb).map(Into::into)
    }

    /// Constructs a new [`PolicyGuardedDataFrame`] from the slice of the original
    /// object according to the `slices` parameter.
    pub fn new_from_slice(&self, slices: &[usize]) -> PicachvResult<Self> {
        let columns = THREAD_POOL.install(|| {
            self.columns
                .par_iter()
                .map(|c| Ok(Arc::new(c.new_from_slice(slices)?)))
                .collect::<PicachvResult<Vec<_>>>()
        })?;

        Ok(PolicyGuardedDataFrame {
            columns,
            ..Default::default()
        })
    }

    /// Joins two policy-carrying dataframes.
    ///
    /// The function iterates over the `row_info` to join the policies specified by `common_list`. After
    /// this is done, it re-arranges all the columns according to the `output_schema`.
    pub fn join(
        lhs: &PolicyGuardedDataFrame,
        rhs: &PolicyGuardedDataFrame,
        info: &JoinInformation,
        options: &ContextOptions,
    ) -> PicachvResult<Self> {
        let join_preparation = || {
            let left_columns = unsafe {
                std::slice::from_raw_parts(
                    info.left_columns.as_ptr() as *const usize,
                    info.left_columns.len(),
                )
            };
            let right_columns = unsafe {
                std::slice::from_raw_parts(
                    info.right_columns.as_ptr() as *const usize,
                    info.right_columns.len(),
                )
            };

            let mut lhs = lhs.clone();
            let mut rhs = rhs.clone();
            let (lhs, rhs) = THREAD_POOL.install(|| {
                rayon::join(
                    || {
                        lhs.projection_by_id(left_columns)?;
                        PicachvResult::Ok(lhs)
                    },
                    || {
                        rhs.projection_by_id(right_columns)?;
                        PicachvResult::Ok(rhs)
                    },
                )
            });

            (lhs, rhs)
        };

        let (lhs, rhs) = if options.enable_profiling {
            PROFILER.profile(join_preparation, "join_preparation".into())
        } else {
            join_preparation()
        };

        let (lhs, rhs) = (lhs?, rhs?);
        let (lhs, rhs) = THREAD_POOL.install(|| {
            rayon::join(
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
            )
        });
        let (lhs, rhs) = (lhs?, rhs?);

        // We then stitch them together.
        let res = PolicyGuardedDataFrame::stitch(&lhs, &rhs)?;
        Ok(res)
    }

    pub fn select_group(&self, hashes: &[u64]) -> PicachvResult<Self> {
        picachv_ensure!(
            hashes.len() <= self.shape().0,
            ComputeError: "The length of the hashes is out of bound. {} > {}",
            hashes.len(), self.shape().0
        );

        let hash_info = &self.additional_info.hash_info;

        let slices = match THREAD_POOL.install(|| {
            hashes
                .par_iter()
                .map(|hash| {
                    hash_info.get(hash).copied().ok_or({
                        PicachvError::InvalidOperation(
                            format!("The hash {} is missing.", hash).into(),
                        )
                    })
                })
                .collect::<PicachvResult<Vec<_>>>()
        }) {
            Ok(s) => s,
            Err(e) => {
                panic!("Error: {e:?}");
            },
        };

        self.new_from_slice(&slices)
    }

    /// According to the `groups` struct, fetch the group of columns.
    pub fn groups(&self, groups: &GroupInformation) -> PicachvResult<Self> {
        let columns = THREAD_POOL.install(|| {
            self.columns
                .par_iter()
                .map(|c: &Arc<PolicyGuardedColumn>| {
                    // let policies = groups
                    //     .groups
                    //     .par_iter()
                    //     .map(|g| c[*g as usize].clone())
                    //     .collect::<Vec<_>>();
                    Ok(Arc::new(c.groups(groups)?))
                })
                .collect::<PicachvResult<Vec<_>>>()
        })?;

        Ok(PolicyGuardedDataFrame {
            columns,
            ..Default::default()
        })
    }

    pub fn row(&self, idx: usize) -> PicachvResult<Vec<&PolicyRef>> {
        picachv_ensure!(
            idx < self.shape().0,
            ComputeError: "The index is out of bound.",
        );

        let res =
            THREAD_POOL.install(|| self.columns.par_iter().map(|c| &c[idx]).collect::<Vec<_>>());

        Ok(res)
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
            ..Default::default()
        })
    }

    pub fn union(inputs: &[Arc<Self>]) -> PicachvResult<Self> {
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
            let mut policies = PolicyGuardedColumn::default();
            for input in inputs.iter() {
                policies = policies.append(&input.columns[i])?;
            }
            columns.push(Arc::new(policies));
        }

        Ok(PolicyGuardedDataFrame {
            columns,
            ..Default::default()
        })
    }

    #[inline]
    pub fn new(columns: Vec<PolicyGuardedColumnRef>) -> Self {
        PolicyGuardedDataFrame {
            columns,
            ..Default::default()
        }
    }

    pub(crate) fn projection_by_id(&mut self, project_list: &[usize]) -> PicachvResult<()> {
        picachv_ensure!(
            project_list.par_iter().all(|&col| col < self.columns.len()),
            ComputeError: "The column is out of bound: {:?} vs {:?}", self.shape().1, project_list,
        );
        let index_set: HashSet<_> = project_list.iter().collect();

        let mut retained = 0;
        for i in 0..self.columns.len() {
            if index_set.contains(&i) {
                self.columns.swap(i, retained);
                retained += 1;
            }
        }

        self.columns.truncate(retained);
        Ok(())
    }

    /// This checks if we can safely release this dataframe.
    pub fn finalize(&self) -> PicachvResult<()> {
        tracing::debug!("finalizing\n{self}");

        for c in self.columns.iter() {
            picachv_ensure!(
                c.policies.par_iter().all(
                    |(_, v)| matches!(v.deref(), Policy::PolicyClean),
                ) && matches!(c.base_policy.deref(), Policy::PolicyClean),
                ComputeError: "Possible policy breach detected; abort early.\n\nThe required policy is\n{self}",
            );
        }

        Ok(())
    }

    /// Get (height, width) of the [`DataFrame`].
    pub fn shape(&self) -> (usize, usize) {
        match self.columns.as_slice() {
            &[] => (0, 0),
            v => (v[0].len(), v.len()),
        }
    }

    /// Apply the filter.
    pub fn filter(&mut self, pred: &[bool]) -> PicachvResult<()> {
        picachv_ensure!(
            pred.len() == self.shape().0,
            ComputeError: "The length of the predicate does not match the dataframe: {} != {}", pred.len(), self.shape().0,
        );

        self.columns = THREAD_POOL.install(|| {
            self.columns
                .par_iter()
                .map(|c| Ok(Arc::new(c.filter(pred)?)))
                .collect::<PicachvResult<Vec<_>>>()
        })?;

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
    options: &ContextOptions,
) -> PicachvResult<Uuid> {
    match transform.information {
        Some(ti) => match ti {
            Information::Filter(pred) => {
                let mut df_arena = df_arena.write();
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
                let mut df_arena = df_arena.write();

                let involved_dfs = union_info
                    .df_uuids
                    .par_iter()
                    .map(|uuid| {
                        let uuid = Uuid::from_slice_le(uuid)
                            .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;
                        df_arena.get(&uuid).cloned()
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                // We just union them all.
                let new_df = PolicyGuardedDataFrame::union(&involved_dfs)?;

                // Assign the new UUID.
                df_arena.insert(new_df)
            },

            Information::Join(join) => {
                let mut df_arena = df_arena.write();

                let lhs = Uuid::from_slice_le(&join.lhs_df_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;
                let rhs = Uuid::from_slice_le(&join.rhs_df_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;

                let lhs_df = df_arena.get(&lhs)?;
                let rhs_df = df_arena.get(&rhs)?;

                let new_df = if options.enable_profiling {
                    PROFILER.profile(
                        || PolicyGuardedDataFrame::join(lhs_df, rhs_df, &join, options),
                        "join".into(),
                    )
                } else {
                    PolicyGuardedDataFrame::join(lhs_df, rhs_df, &join, options)
                }?;

                df_arena.insert(new_df)
            },

            Information::Reorder(reorder_info) => {
                let mut df_arena = df_arena.write();
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

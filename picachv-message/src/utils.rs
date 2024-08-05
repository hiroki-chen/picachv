use picachv_error::PicachvResult;
use uuid::Uuid;

use crate::transform_info::Information;
use crate::{
    FilterInformation, JoinInformation, RenamingInformation, RowJoinInformation, TransformInfo,
    UnionInformation,
};

impl TransformInfo {
    pub fn from_filter(pred: &[bool]) -> PicachvResult<Self> {
        Ok(Self {
            information: Some(Information::Filter(FilterInformation {
                filter: pred.to_vec(),
            })),
        })
    }

    pub fn from_union(lhs_df_uuid: Uuid, rhs_df_uuid: Uuid) -> PicachvResult<Self> {
        let lhs_df_uuid = lhs_df_uuid.to_bytes_le().to_vec();
        let rhs_df_uuid = rhs_df_uuid.to_bytes_le().to_vec();

        Ok(Self {
            information: Some(Information::Union(UnionInformation {
                df_uuids: vec![lhs_df_uuid, rhs_df_uuid],
            })),
        })
    }

    pub fn from_join(
        lhs_df_uuid: Uuid,
        rhs_df_uuid: Uuid,
        left_columns: Vec<u64>,
        right_columns: Vec<u64>,
        row_join_info: Vec<RowJoinInformation>,
        renaming_info: Vec<RenamingInformation>,
    ) -> PicachvResult<Self> {
        Ok(Self {
            information: Some(Information::Join(JoinInformation {
                lhs_df_uuid: lhs_df_uuid.to_bytes_le().to_vec(),
                rhs_df_uuid: rhs_df_uuid.to_bytes_le().to_vec(),
                left_columns,
                right_columns,
                row_join_info,
                renaming_info,
            })),
        })
    }
}

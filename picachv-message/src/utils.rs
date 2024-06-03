use picachv_error::PicachvResult;
use uuid::Uuid;

use crate::join_information::How;
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
        Ok(Self {
            information: Some(Information::Union(UnionInformation {
                lhs_df_uuid: lhs_df_uuid.to_bytes_le().to_vec(),
                rhs_df_uuid: rhs_df_uuid.to_bytes_le().to_vec(),
            })),
        })
    }

    pub fn from_join_by_name(
        lhs_df_uuid: Uuid,
        rhs_df_uuid: Uuid,
        left_on: Vec<String>,
        right_on: Vec<String>,
        row_join_info: Vec<RowJoinInformation>,
        lhs_input_schema: Vec<String>,
        rhs_input_schema: Vec<String>,
        renaming: Vec<RenamingInformation>,
    ) -> PicachvResult<Self> {
        Ok(Self {
            information: Some(Information::Join(JoinInformation {
                lhs_df_uuid: lhs_df_uuid.to_bytes_le().to_vec(),
                rhs_df_uuid: rhs_df_uuid.to_bytes_le().to_vec(),
                how: Some(How::JoinByName(crate::JoinByName {
                    left_on,
                    right_on,
                    row_join_info,
                    lhs_input_schema,
                    rhs_input_schema,
                    renaming,
                })),
            })),
        })
    }
}

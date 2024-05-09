use picachv_error::PicachvResult;
use uuid::Uuid;

use crate::join_information::{RenamingInformation, RowJoinInformation};
use crate::transform_info::{information, Information};
use crate::{FilterInformation, JoinInformation, TransformInfo, UnionInformation};

impl TransformInfo {
    pub fn push_filter(&mut self, df_uuid: Uuid, pred: &[bool]) -> PicachvResult<()> {
        let ti = Information {
            information: Some(information::Information::Filter(FilterInformation {
                df_uuid: df_uuid.to_bytes_le().to_vec(),
                filter: pred.to_vec(),
            })),
        };

        self.trans_info.push(ti);

        Ok(())
    }

    pub fn push_union(&mut self, lhs_df_uuid: Uuid, rhs_df_uuid: Uuid) -> PicachvResult<()> {
        let ti = Information {
            information: Some(information::Information::Union(UnionInformation {
                lhs_df_uuid: lhs_df_uuid.to_bytes_le().to_vec(),
                rhs_df_uuid: rhs_df_uuid.to_bytes_le().to_vec(),
            })),
        };

        self.trans_info.push(ti);

        Ok(())
    }

    pub fn push_join(
        &mut self,
        lhs_df_uuid: Uuid,
        rhs_df_uuid: Uuid,
        left_on: Vec<String>,
        right_on: Vec<String>,
        row_join_info: Vec<RowJoinInformation>,
        lhs_input_schema: Vec<String>,
        rhs_input_schema: Vec<String>,
        renaming: Vec<RenamingInformation>,
    ) -> PicachvResult<()> {
        let ti = Information {
            information: Some(information::Information::Join(JoinInformation {
                lhs_df_uuid: lhs_df_uuid.to_bytes_le().to_vec(),
                rhs_df_uuid: rhs_df_uuid.to_bytes_le().to_vec(),
                left_on,
                right_on,
                row_join_info,
                lhs_input_schema,
                rhs_input_schema,
                renaming,
            })),
        };

        self.trans_info.push(ti);

        Ok(())
    }
}

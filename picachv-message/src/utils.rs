use picachv_error::PicachvResult;
use uuid::Uuid;

use crate::{
    transform_info::{information, Information},
    DummyInformation, FilterInformation, TransformInfo,
};

impl TransformInfo {
    pub fn push_dummy(&mut self, df_uuid: Uuid) -> PicachvResult<()> {
        let ti = Information {
            information: Some(information::Information::Dummy(DummyInformation {
                df_uuid: df_uuid.to_bytes_le().to_vec(),
            })),
        };

        self.trans_info.push(ti);

        Ok(())
    }

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
}

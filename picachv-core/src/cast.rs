use parse_duration::parse;
use picachv_error::{PicachvError, PicachvResult};

use crate::policy::types::AnyValue;

pub fn into_duration(value: AnyValue) -> PicachvResult<AnyValue> {
    match value {
        AnyValue::Duration(d) => Ok(AnyValue::Duration(d)),
        AnyValue::String(s) => {
            let d = parse(&s).map_err(|e| {
                PicachvError::InvalidOperation(
                    format!("Failed to parse the duration: {}", e).into(),
                )
            })?;

            Ok(AnyValue::Duration(d))
        },

        _ => Err(PicachvError::InvalidOperation(
            "The value is not a duration.".into(),
        )),
    }
}

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

pub fn into_i64(value: AnyValue) -> PicachvResult<AnyValue> {
    match value {
        AnyValue::Int32(i) => Ok(AnyValue::Int64(i as i64)),
        AnyValue::Int64(i) => Ok(AnyValue::Int64(i)),
        AnyValue::String(s) => {
            let i = s.parse::<i64>().map_err(|e| {
                PicachvError::InvalidOperation(format!("Failed to parse the integer: {}", e).into())
            })?;

            Ok(AnyValue::Int64(i))
        },

        _ => Err(PicachvError::InvalidOperation(
            "The value is not an integer.".into(),
        )),
    }
}

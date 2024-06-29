use std::sync::Arc;

use parse_duration::parse;
use picachv_error::{PicachvError, PicachvResult};

use crate::policy::types::{AnyValue, AnyValueRef};

pub fn into_duration(value: &AnyValueRef) -> PicachvResult<AnyValueRef> {
    match value.as_ref() {
        AnyValue::Duration(_) => Ok(value.clone()),
        AnyValue::String(s) => {
            let d = parse(&s).map_err(|e| {
                PicachvError::InvalidOperation(
                    format!("Failed to parse the duration: {}", e).into(),
                )
            })?;

            Ok(Arc::new(AnyValue::Duration(d)))
        },

        _ => Err(PicachvError::InvalidOperation(
            "The value is not a duration.".into(),
        )),
    }
}

pub fn into_i64(value: &AnyValueRef) -> PicachvResult<AnyValueRef> {
    match value.as_ref() {
        AnyValue::Int32(i) => Ok(Arc::new(AnyValue::Int64(*i as i64))),
        AnyValue::Int64(_) => Ok(value.clone()),
        AnyValue::String(s) => {
            let i = s.parse::<i64>().map_err(|e| {
                PicachvError::InvalidOperation(format!("Failed to parse the integer: {}", e).into())
            })?;

            Ok(Arc::new(AnyValue::Int64(i)))
        },

        _ => Err(PicachvError::InvalidOperation(
            "The value is not an integer.".into(),
        )),
    }
}

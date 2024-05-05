use std::cmp::Ordering;
use std::hash::Hash;
use std::time::Duration;

use ordered_float::OrderedFloat;
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::PrimitiveValue;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Hash, Serialize, Deserialize)]
pub struct DpParam(OrderedFloat<f64>, Option<OrderedFloat<f64>>);

impl DpParam {
    #[inline]
    pub fn epsilon(&self) -> f64 {
        self.0 .0
    }

    #[inline]
    pub fn delta(&self) -> Option<f64> {
        self.1.map(|x| x.0)
    }
}

impl PartialEq for DpParam {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl PartialOrd for DpParam {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.partial_cmp(&other.0).unwrap())
    }
}

impl Eq for DpParam {}
impl Ord for DpParam {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// A type that can represent any value.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnyValue {
    Boolean(bool),
    String(String),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    Duration(Duration),
}

impl TryFrom<PrimitiveValue> for AnyValue {
    type Error = PicachvError;

    fn try_from(value: PrimitiveValue) -> PicachvResult<Self> {
        use picachv_message::primitive_value::Value;

        match value.value {
            Some(v) => Ok(match v {
                Value::Bool(b) => Self::Boolean(b),
                Value::I8(i) => Self::Int8(i as _),
                Value::I16(i) => Self::Int16(i as _),
                Value::I32(i) => Self::Int32(i),
                Value::I64(i) => Self::Int64(i),
                Value::U8(u) => Self::UInt8(u as _),
                Value::U16(u) => Self::UInt16(u as _),
                Value::U32(u) => Self::UInt32(u),
                Value::U64(u) => Self::UInt64(u),
                Value::F32(f) => Self::Float32(OrderedFloat(f as _)),
                Value::F64(f) => Self::Float64(OrderedFloat(f)),
                Value::Str(s) => Self::String(s),
                Value::Duration(d) => Self::Duration(Duration::new(d.sec as _, d.nsec as _)),
            }),
            None => Err(PicachvError::ComputeError("The value is empty.".into())),
        }
    }
}

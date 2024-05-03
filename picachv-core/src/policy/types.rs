use std::cmp::Ordering;
use std::hash::Hash;
use std::time::Duration;

use ordered_float::OrderedFloat;
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

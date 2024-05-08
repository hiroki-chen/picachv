//! This module is automatically generated from the protobuf definition file.
//!
//! The reason why we need ProtoBuf as our data interchange format is that it is
//! a language-agnostic format, which means that we can use it to communicate
//! between different programming languages.
//!
//! If we are purely using Rust, we can use `bincode` or `serde` to serialize
//! the data dependening on the specific use cases: `bincode` is faster and
//! `serde` is more flexible and human-readable.

include!("./picachv_messages.rs");

pub mod utils;

#[cfg(test)]
mod test {
    use prost::Message;

    use super::*;

    #[test]
    fn test_message() {
        let agg_expr = AggExpr {
            input_uuid: b"123".into(),
            method: 1,
        };

        let protobuf = agg_expr.encode_to_vec();
        let decoded = AggExpr::decode(protobuf.as_slice()).unwrap();

        assert_eq!(agg_expr, decoded);
    }
}

include!("./picachv_messages.rs");

#[cfg(test)]
mod test{
    use prost::Message;

    use super::*;

    #[test]
    fn test_message() {
        let agg_expr = AggExpr {
            expr: Some(agg_expr::Expr::Sum(SumExpr {
                input_uuid: "123".into(),
            }))
        };

        let protobuf = agg_expr.encode_to_vec();
        let decoded = AggExpr::decode(protobuf.as_slice()).unwrap();

        assert_eq!(agg_expr, decoded);
    }
}
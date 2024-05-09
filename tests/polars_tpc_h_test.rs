const TPC_QUERY_1: &str = r#"
-- TPC-H Query 1

-- generate a table, with ('l_returnflag', 'l_linestatus') as the unique id;
--   calculate sum, avg on different columns;
--   count the total number of rows in the group.
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
        
-- from a table named 'lineitem'
from
        lineitem

-- it must follows the rule that
--   data in the column named 'l_shipdate' must be less or equal to XXX
where
        l_shipdate <= date '1998-12-01' - interval '90' day

-- data with the same 'l_returnflag' and 'l_linestatus' are grouped together.
--   calcualte sum, avg, count, such kind of aggregations.
group by
        l_returnflag,
        l_linestatus
-- sort the results with increasing order of 'l_returnflag';
--   break ties using 'l_linestatus'.
order by
        l_returnflag,
        l_linestatus
        "#;

#[cfg(test)]
mod polars_tests {
    use polars::sql::*;

    #[test]
    fn test_polars_query_1() {}
}

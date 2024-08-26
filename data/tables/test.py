# import duckdb
import polars as pl

df = pl.read_parquet("lineitem.parquet")
# "select l_orderkey "
#                           "from '" +
#                           lineitem +
#                           "' "
#                           "group by l_orderkey ";
#                           "having sum(l_quantity) > 300"
df = (
    df.groupby("l_orderkey")
    .agg(pl.sum("l_quantity"))
    # .filter(pl.col("l_quantity") > 300)
    .select(["l_orderkey"])
    .filter(pl.col("l_orderkey") == 229088)
)

print(df)


# # DuckDB version
# def duckdb_query():
#     conn = duckdb.connect(":memory:")
#     result = conn.execute(
#         """
#         SELECT MIN(ps_supplycost)
# FROM partsupp.parquet, supplier.parquet, nation.parquet, region.parquet, part.parquet
# WHERE p_partkey = ps_partkey
#   AND s_suppkey = ps_suppkey
#   AND s_nationkey = n_nationkey
#   AND n_regionkey = r_regionkey
#   AND r_name = 'EUROPE';
#         """
#     )
#     df = result.df()
#     conn.close()
#     return df


# # Polars version
# def polars_query():
#     lineitem_df = pl.read_parquet("lineitem.parquet")
#     orders_df = pl.read_parquet("orders.parquet")
#     result = lineitem_df.join(
#         orders_df, left_on="l_orderkey", right_on="o_orderkey"
#     ).select(["l_orderkey", "l_tax"])
#     return result.to_pandas()


# # Run both queries
# duckdb_result = duckdb_query()
# # polars_result = polars_query()

# print(duckdb_result)
# # print(polars_result)
# # print(polars_result.columns)

import argparse
import polars as pl
import subprocess
import os

from settings import Settings

settings = Settings()

DEFAULT_ROW_GROUP_SIZE = 2048


table_columns = {
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "comments",
    ],
    "nation": [
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    "region": [
        "r_regionkey",
        "r_name",
        "r_comment",
    ],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
}


def dbgen(scale_factor: int) -> None:
    ret = subprocess.run(
        ["make", "-C", "dbgen", "dbgen"], stdout=subprocess.PIPE, check=True
    )
    if ret.returncode != 0:
        print("Error running dbgen")
        print(ret.stdout)
        return

    # Generate tables.
    os.chdir("dbgen")
    ret = subprocess.run(
        ["./dbgen", "-vf", "-s", str(scale_factor)], stdout=subprocess.PIPE, check=True
    )
    if ret.returncode != 0:
        print("Error running dbgen")
        return
    ret = subprocess.Popen("mv *.tbl ../../data/tables", shell=True)
    os.chdir("..")


def main(percentage: float, skip_dbgen: bool, scale_factor: int) -> None:
    if not skip_dbgen:
        dbgen(scale_factor)

    for table_name, columns in table_columns.items():
        print(f"Processing table: {table_name}")

        path = settings.dataset_base_dir / f"{table_name}.tbl"
        lf = pl.scan_csv(
            path,
            has_header=False,
            separator="|",
            try_parse_dates=True,
            new_columns=columns,
        )

        # Drop empty last column because CSV ends with a separator
        lf = lf.select(columns)

        if table_name in ["lineitem", "orders", "part", "partsupp", "customer"]:
            num = int(lf.select(pl.len()).collect().item())
            lf = lf.limit(int(num * percentage))

        # The reason why we avoid using `lf.sink_parquet` is that it is buggy when dealing with
        # `row_group_size`; this argument seems to be ignored.
        df = lf.collect()
        print("writing to parquet: {}".format(settings.dataset_base_dir / f"{table_name}.parquet"))
        df.write_parquet(
            settings.dataset_base_dir / f"{table_name}.parquet",
            use_pyarrow=True,
            row_group_size=2048,
            pyarrow_options={"row_group_size": DEFAULT_ROW_GROUP_SIZE},
        )
        df.write_csv(settings.dataset_base_dir / f"{table_name}.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare data for benchmark")
    parser.add_argument(
        "--percentage",
        type=float,
        default=1.0,
        help="Percentage of data to generate",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        default=1,
        help="Scale factor for dbgen",
    )
    parser.add_argument(
        "--skip-dbgen",
        action="store_true",
        help="Skip dbgen step",
    )

    args = parser.parse_args()

    main(args.percentage, args.skip_dbgen, args.scale_factor)

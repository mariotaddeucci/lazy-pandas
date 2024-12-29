from functools import wraps

import duckdb

from pandas_lazy.frame import LazyFrame


@wraps(duckdb.read_csv)
def read_csv(*args, **kwargs) -> LazyFrame:
    relation = duckdb.read_csv(*args, **kwargs)
    return LazyFrame(relation)


@wraps(duckdb.read_json)
def read_json(*args, **kwargs) -> LazyFrame:
    relation = duckdb.read_json(*args, **kwargs)
    return LazyFrame(relation)


@wraps(duckdb.read_parquet)
def read_parquet(*args, columns: list[str] | None = None, **kwargs) -> LazyFrame:
    relation = duckdb.read_parquet(*args, **kwargs)
    df = LazyFrame(relation)
    if columns is None:
        return df
    return df[columns]


def read_delta(path: str) -> LazyFrame:
    relation = duckdb.sql(f"FROM delta_scan('{path}')")
    return LazyFrame(relation)


def read_iceberg(path: str) -> LazyFrame:
    duckdb.sql("install iceberg; load iceberg;")
    relation = duckdb.sql(f"FROM iceberg_scan('{path}')")
    return LazyFrame(relation)

from functools import wraps

import duckdb

from pandas_lazy.frame import LazyFrame


@wraps(duckdb.read_csv)
def read_csv(*args, **kwargs) -> LazyFrame:
    """
    Reads a CSV file and returns a LazyFrame.

    This function wraps `duckdb.read_csv` to return a LazyFrame instead of a DuckDB relation.

    Parameters
    ----------
    *args : tuple
        Positional arguments to pass to `duckdb.read_csv`.
    **kwargs : dict
        Keyword arguments to pass to `duckdb.read_csv`.

    Returns
    -------
    LazyFrame
        A LazyFrame object containing the data from the CSV file.
    """
    relation = duckdb.read_csv(*args, **kwargs)
    return LazyFrame(relation)


@wraps(duckdb.read_json)
def read_json(*args, **kwargs) -> LazyFrame:
    """
    Reads a JSON file and returns a LazyFrame.

    This function wraps `duckdb.read_json` to return a LazyFrame instead of a DuckDB relation.

    Parameters
    ----------
    *args : tuple
        Positional arguments to pass to `duckdb.read_json`.
    **kwargs : dict
        Keyword arguments to pass to `duckdb.read_json`.

    Returns
    -------
    LazyFrame
        A LazyFrame object containing the data from the JSON file.
    """
    relation = duckdb.read_json(*args, **kwargs)
    return LazyFrame(relation)


@wraps(duckdb.read_parquet)
def read_parquet(*args, columns: list[str] | None = None, **kwargs) -> LazyFrame:
    """
    Reads a Parquet file and returns a LazyFrame.

    This function wraps `duckdb.read_parquet` to return a LazyFrame instead of a DuckDB relation.
    Optionally, specific columns can be selected from the Parquet file.

    Parameters
    ----------
    *args : tuple
        Positional arguments to pass to `duckdb.read_parquet`.
    columns : list of str, optional
        List of column names to select. If None, all columns are included.
    **kwargs : dict
        Keyword arguments to pass to `duckdb.read_parquet`.

    Returns
    -------
    LazyFrame
        A LazyFrame object containing the data from the Parquet file, optionally filtered to specific columns.
    """
    relation = duckdb.read_parquet(*args, **kwargs)
    df = LazyFrame(relation)
    if columns is None:
        return df
    return df[columns]


def read_delta(path: str) -> LazyFrame:
    """
    Reads a Delta table and returns a LazyFrame.

    Parameters
    ----------
    path : str
        The file path or directory of the Delta table.

    Returns
    -------
    LazyFrame
        A LazyFrame object containing the data from the Delta table.
    """
    relation = duckdb.sql(f"FROM delta_scan('{path}')")
    return LazyFrame(relation)


def read_iceberg(path: str) -> LazyFrame:
    """
    Reads an Iceberg table and returns a LazyFrame.

    This function installs and loads the Iceberg extension in DuckDB before scanning the table.

    Parameters
    ----------
    path : str
        The file path or directory of the Iceberg table.

    Returns
    -------
    LazyFrame
        A LazyFrame object containing the data from the Iceberg table.
    """
    duckdb.sql("install iceberg; load iceberg;")
    relation = duckdb.sql(f"FROM iceberg_scan('{path}')")
    return LazyFrame(relation)

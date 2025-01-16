from io import StringIO, TextIOBase

import duckdb
import duckdb.typing
from lazy_pandas.frame.lazy_frame import LazyFrame


def from_pandas(df) -> LazyFrame:
    """
    Converts a pandas DataFrame to a LazyFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to convert.

    Returns:
        LazyFrame: A LazyFrame containing the data from the pandas DataFrame.

    Example:
    ```python
    import pandas as pd
    import lazy_pandas as lp
    df = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
    lazy_df = lp.from_pandas(df)
    ```
    """
    return LazyFrame(duckdb.from_df(df))


def read_csv(
    path_or_buffer: str | StringIO | TextIOBase,
    *,
    header: bool | int | None = None,
    compression: str | None = None,
    sep: str | None = None,
    delimiter: str | None = None,
    dtype: dict[str, str] | list[str] | None = None,
    na_values: str | list[str] | None = None,
    skip_rows: int | None = None,
    quote_char: str | None = None,
    escape_char: str | None = None,
    encoding: str | None = None,
    parallel: bool | None = None,
    date_format: str | None = None,
    timestamp_format: str | None = None,
    sample_size: int | None = None,
    all_varchar: bool | None = None,
    normalize_names: bool | None = None,
    null_padding: bool | None = None,
    names: list[str] | None = None,
    line_terminator: str | None = None,
    columns: dict[str, str] | None = None,
    auto_type_candidates: list[str] | None = None,
    max_line_size: int | None = None,
    ignore_errors: bool | None = None,
    store_rejects: bool | None = None,
    rejects_table: str | None = None,
    rejects_scan: str | None = None,
    rejects_limit: int | None = None,
    force_not_null: list[str] | None = None,
    buffer_size: int | None = None,
    decimal: str | None = None,
    allow_quoted_nulls: bool | None = None,
    include_filename: bool | str | None = None,
    hive_partitioning: bool | None = None,
    union_by_name: bool | None = None,
    hive_types: dict[str, str] | None = None,
    hive_types_autocast: bool | None = None,
    parse_dates: list[str] | None = None,
) -> LazyFrame:
    """
    Reads a CSV file and returns a LazyFrame.

    Args:
        path_or_buffer (str | StringIO | TextIOBase): Path to the CSV file or a buffer-like object.
        header (bool | int | None, optional): Indicates whether the CSV file has a header row. Can be a boolean or the row number of the header. Defaults to None.
        compression (str | None, optional): Compression type of the file. Options are 'none', 'gzip', or 'zstd'. Defaults to None.
        sep (str | None, optional): Character that separates columns. Alias for 'delimiter'. Defaults to None.
        delimiter (str | None, optional): Character that separates columns. Defaults to None.
        dtype (dict[str, str] | list[str] | None, optional): Specifies column data types. Can be a dictionary with column names and types, or a list of types. Defaults to None.
        na_values (str | list[str] | None, optional): Values to interpret as NA/NaN. Defaults to None.
        skip_rows (int | None, optional): Number of lines to skip at the start of the file. Defaults to None.
        quote_char (str | None, optional): Character used for quoting. Defaults to None.
        escape_char (str | None, optional): Character used for escaping. Defaults to None.
        encoding (str | None, optional): File encoding. Defaults to None.
        parallel (bool | None, optional): Enables or disables parallel reading. Defaults to None.
        date_format (str | None, optional): Format to use when parsing dates. Defaults to None.
        timestamp_format (str | None, optional): Format to use when parsing timestamps. Defaults to None.
        sample_size (int | None, optional): Number of rows to sample for type inference. Defaults to None.
        all_varchar (bool | None, optional): If True, assumes all columns are of type VARCHAR, skipping type inference. Defaults to None.
        normalize_names (bool | None, optional): Normalizes column names to lowercase and replaces spaces with underscores. Defaults to None.
        null_padding (bool | None, optional): If True, adds null padding to text columns. Defaults to None.
        names (list[str] | None, optional): List of column names to use. Defaults to None.
        line_terminator (str | None, optional): Character that indicates the end of a line. Defaults to None.
        columns (dict[str, str] | None, optional): Dictionary specifying column names and types in the CSV file. Defaults to None.
        auto_type_candidates (list[str] | None, optional): List of types for the parser to consider during type inference. Defaults to None.
        max_line_size (int | None, optional): Maximum size of a line in the CSV file. Defaults to None.
        ignore_errors (bool | None, optional): If True, ignores errors during CSV reading. Defaults to None.
        store_rejects (bool | None, optional): If True, stores rejected lines during reading. Defaults to None.
        rejects_table (str | None, optional): Name of the table to store rejected lines. Defaults to None.
        rejects_scan (str | None, optional): Path to store the scan of rejected lines. Defaults to None.
        rejects_limit (int | None, optional): Limit of rejected lines before stopping the read. Defaults to None.
        force_not_null (list[str] | None, optional): List of columns that should not be interpreted as NULL. Defaults to None.
        buffer_size (int | None, optional): Size of the read buffer. Defaults to None.
        decimal (str | None, optional): Decimal separator for numbers. Defaults to None.
        allow_quoted_nulls (bool | None, optional): If True, allows conversion of quoted values to NULL. Defaults to None.
        include_filename (bool | str | None, optional): If True or a string, includes the filename in the output. Defaults to None.
        hive_partitioning (bool | None, optional): Enables Hive partitioning. Defaults to None.
        union_by_name (bool | None, optional): If True, unions files by column name. Defaults to None.
        hive_types (dict[str, str] | None, optional): Dictionary specifying Hive types for columns. Defaults to None.
        hive_types_autocast (bool | None, optional): If True, automatically casts Hive types. Defaults to None.
        parse_dates (list[str] | None, optional): List of column names to parse as dates. Defaults to None.

    Returns:
        LazyFrame: A LazyFrame containing the data from the CSV file.

    Example:
    ```python
    import lazy_pandas as lp
    df = lp.read_csv('data.csv', header=True, sep=',', dtype={'column1': 'INTEGER', 'column2': 'VARCHAR'})
    df.head()
    ```
    """
    relation = duckdb.read_csv(
        path_or_buffer=path_or_buffer,
        header=header,
        compression=compression,
        sep=sep,
        delimiter=delimiter,
        dtype=dtype,
        na_values=na_values,
        skiprows=skip_rows,
        quotechar=quote_char,
        escapechar=escape_char,
        encoding=encoding,
        parallel=parallel,
        date_format=date_format,
        timestamp_format=timestamp_format,
        sample_size=sample_size,
        all_varchar=all_varchar,
        normalize_names=normalize_names,
        null_padding=null_padding,
        names=names,
        lineterminator=line_terminator,
        columns=columns,
        auto_type_candidates=auto_type_candidates,
        max_line_size=max_line_size,
        ignore_errors=ignore_errors,
        store_rejects=store_rejects,
        rejects_table=rejects_table,
        rejects_scan=rejects_scan,
        rejects_limit=rejects_limit,
        force_not_null=force_not_null,
        buffer_size=buffer_size,
        decimal=decimal,
        allow_quoted_nulls=allow_quoted_nulls,
        filename=include_filename,
        hive_partitioning=hive_partitioning,
        union_by_name=union_by_name,
        hive_types=hive_types,
        hive_types_autocast=hive_types_autocast,
    )
    df = LazyFrame(relation)
    for col in parse_dates or []:
        df[col] = df[col].astype(duckdb.typing.TIMESTAMP)
    return df


def read_json(
    path_or_buffer: str | StringIO | TextIOBase,
    *,
    columns: dict[str, str] | None = None,
    sample_size: int | None = None,
    maximum_depth: int | None = None,
    records: str | None = None,
    format: str | None = None,
    date_format: str | None = None,
    timestamp_format: str | None = None,
    compression: str | None = None,
    maximum_object_size: int | None = None,
    ignore_errors: bool | None = None,
    convert_strings_to_integers: bool | None = None,
    field_appearance_threshold: float | None = None,
    map_inference_threshold: int | None = None,
    maximum_sample_files: int | None = None,
    include_filename: bool | str | None = None,
    hive_partitioning: bool | None = None,
    union_by_name: bool | None = None,
    hive_types: dict[str, str] | None = None,
    hive_types_autocast: bool | None = None,
) -> LazyFrame:
    """
    Reads a JSON file or buffer and returns a LazyFrame.

    Args:
        path_or_buffer (str | StringIO | TextIOBase): Path to the JSON file or a buffer-like object.
        columns (dict[str, str] | None): Dictionary specifying the key names and value types contained within the JSON file.
        sample_size (int | None): Number of sample objects for automatic JSON type detection. Set to -1 to scan the entire input file.
        maximum_depth (int | None): Maximum nesting depth for automatic schema detection. Set to -1 to fully detect nested JSON types.
        records (str | None): Specifies whether the JSON contains records that should be unpacked into individual columns. Can be 'auto', 'true', or 'false'.
        format (str | None): Format of the JSON file. Can be 'auto', 'unstructured', 'newline_delimited', or 'array'.
        date_format (str | None): Specifies the date format to use when parsing dates.
        timestamp_format (str | None): Specifies the timestamp format to use when parsing timestamps.
        compression (str | None): Compression type of the file. Options are 'none', 'gzip', 'zstd', and 'auto'.
        maximum_object_size (int | None): Maximum size of a JSON object in bytes.
        ignore_errors (bool | None): Whether to ignore parse errors (only possible when format is 'newline_delimited').
        convert_strings_to_integers (bool | None): Whether to convert strings to integers during parsing.
        field_appearance_threshold (float | None): Threshold for field appearance to determine data types.
        map_inference_threshold (int | None): Threshold for inferring MAP types instead of STRUCT types during auto-detection.
        maximum_sample_files (int | None): Maximum number of JSON files sampled for auto-detection.
        include_filename (bool | str | None): Whether to include an extra filename column in the result.
        hive_partitioning (bool | None): Whether to interpret the path as a Hive partitioned path.
        union_by_name (bool | None): Whether the schemas of multiple JSON files should be unified by column name.
        hive_types (dict[str, str] | None): Dictionary specifying Hive types for columns.
        hive_types_autocast (bool | None): Whether to automatically cast Hive types.

    Returns:
        LazyFrame: A LazyFrame containing the data from the JSON file or buffer.

    Example:
    ```python
    import lazy_pandas as lp
    df = lp.read_json('data.json', columns={'userId': 'INTEGER', 'completed': 'BOOLEAN'}, format='array')
    df.head()
    ```
    """
    relation = duckdb.read_json(
        path_or_buffer,
        columns=columns,
        sample_size=sample_size,
        maximum_depth=maximum_depth,
        records=records,
        format=format,
        date_format=date_format,
        timestamp_format=timestamp_format,
        compression=compression,
        maximum_object_size=maximum_object_size,
        ignore_errors=ignore_errors,
        convert_strings_to_integers=convert_strings_to_integers,
        field_appearance_threshold=field_appearance_threshold,
        map_inference_threshold=map_inference_threshold,
        maximum_sample_files=maximum_sample_files,
        filename=include_filename,
        hive_partitioning=hive_partitioning,
        union_by_name=union_by_name,
        hive_types=hive_types,
        hive_types_autocast=hive_types_autocast,
    )
    return LazyFrame(relation)


def read_parquet(
    path: str,
    *,
    columns: list[str] | None = None,
    include_file_row_number: bool = False,
    include_filename: bool = False,
    use_hive_partitioning: bool = False,
    union_by_name: bool = False,
    compression: str | None = None,
) -> LazyFrame:
    """
    Reads a Parquet file and returns a LazyFrame.

    Args:
        path (str): Path to the Parquet file.
        columns (list[str] | None): List of column names to read from the file. If None, all columns are read.
        include_file_row_number (bool): If True, includes a column with the row number from the file.
        include_filename (bool): If True, includes a column with the filename.
        use_hive_partitioning (bool): If True, enables Hive partitioning.
        union_by_name (bool): If True, unions files by column name.
        compression (str | None): Compression type to use when reading the Parquet file.

    Returns:
        LazyFrame: A LazyFrame containing the data from the Parquet file.

    Example:
    ```python
    import lazy_pandas as lp
    df = lp.read_parquet('data.parquet', columns=['column1', 'column2'])
    df.head()
    ```
    """
    relation = duckdb.read_parquet(
        path,
        file_row_number=include_file_row_number,
        filename=include_filename,
        hive_partitioning=use_hive_partitioning,
        union_by_name=union_by_name,
        compression=compression,
    )
    df = LazyFrame(relation)
    if columns is None:
        return df
    return df[columns]


def read_delta(path: str, *, conn: duckdb.DuckDBPyConnection | None = None) -> LazyFrame:
    """
    Reads a Delta Lake table and returns a LazyFrame.

    Args:
        path (str): Path to the Delta Lake table.

    Returns:
        LazyFrame: A LazyFrame containing the data from the Delta Lake table.

    Example:
    ```python
    import lazy_pandas as lp
    from datetime import date
    df = lp.read_delta('s3://bucket/path_to_delta_table')
    df.head()
    ```
    """
    if conn is None:
        relation = duckdb.sql(f"FROM delta_scan('{path}')")
    else:
        relation = conn.sql(f"FROM delta_scan('{path}')")
    return LazyFrame(relation)


def read_iceberg(path: str, *, conn: duckdb.DuckDBPyConnection | None = None) -> LazyFrame:
    """
    Reads an Apache Iceberg table and returns a LazyFrame.

    Args:
        path (str): Path to the Apache Iceberg table.

    Returns:
        LazyFrame: A LazyFrame containing the data from the Apache Iceberg table.

    Example:
    ```python
    import lazy_pandas as lp
    df = lp.read_iceberg('s3://bucket/path_to_iceberg_table')
    df.head()
    ```
    """
    if conn is None:
        duckdb.sql("install iceberg; load iceberg;")
        relation = duckdb.sql(f"FROM iceberg_scan('{path}')")
    else:
        relation = conn.sql(f"FROM iceberg_scan('{path}')")
    return LazyFrame(relation)

# Lazy Pandas
Lazy Pandas is a Python library that simplifies the use duckdb wrapping the pandas API. This library is not a pandas replacement, but a way to use the pandas API with DuckDB. Pandas is awesome and adopted by many people, but it is not the best tool for datasets that do not fit in memory. So why not give the power of duckdb to pandas users?

## Installation

To install Lazy Pandas, you can use pip:

```sh
pip install lazy-pandas
```

## Usage

Here is a basic example of how to use Lazy Pandas:
```python
import lazy_pandas as lp

df = lp.read_csv(location, parse_dates=["pickup_datetime"])
df = df[["pickup_datetime", "passenger_count"]]
df["pickup_date"] = df["pickup_datetime"].dt.date
df = df.sort_values("pickup_date")
df = df.collect()  # Materialize the lazy DataFrame to a pandas DataFrame
```

Features

- Lazy evaluation
- SQL support
- Support for DuckDB extensions (e.g., Delta, Iceberg, etc.)

Contribution

Contributions are welcome! Feel free to open issues and pull requests.

License

This project is licensed under the MIT License - see the LICENSE file for details.

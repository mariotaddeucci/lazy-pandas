---
title: Lazy Pandas
hide:
    - navigation
    - toc
---
Welcome to the Lazy Pandas official documentation! A library that allows you to use the pandas API with DuckDB as simple as a pip install.

To start using Lazy Pandas, you can install it using pip:

```sh
pip install lazy-pandas
```

## What is Lazy Pandas?

LazyPandas is a wrapper around DuckDB that allows you to use the pandas API to interact with DuckDB. This library is not a pandas replacement, but a way to use the pandas API with DuckDB. Pandas is awesome and adopted by many people, but it is not the best tool for datasets that do not fit in memory. So why not give the power of duckdb to pandas users?

## Code Comparison

Below is a side-by-side comparison showing how the same operation would look in **Pandas** versus **Lazy Pandas**:

=== "Lazy Pandas"

    ```python linenums="1" hl_lines="2 5 13"
    import pandas as pd
    import lazy_pandas as lp

    def read_taxi_dataset(location: str) -> pd.DataFrame:
        df = lp.read_csv(location, parse_dates=["pickup_datetime"])
        df = df[["pickup_datetime", "passenger_count"]]
        df["pickup_date"] = df["pickup_datetime"].dt.date
        del df["pickup_datetime"]
        df = df.groupby("pickup_date").sum().reset_index()
        df = df[["pickup_date", "passenger_count"]]
        df = df.sort_values("pickup_date")
        df = df.collect()  # Materialize the lazy DataFrame to a pandas DataFrame
        return df
    ```

=== "Pandas"

    ```python linenums="1"
    import pandas as pd


    def read_taxi_dataset(location: str) -> pd.DataFrame:
        df = pd.read_csv(location, parse_dates=["pickup_datetime"])
        df = df[["pickup_datetime", "passenger_count"]]
        df["pickup_date"] = df["pickup_datetime"].dt.date
        del df["pickup_datetime"]
        df = df.groupby("pickup_date").sum().reset_index()
        df = df[["pickup_date", "passenger_count"]]
        df = df.sort_values("pickup_date")

        return df
    ```

Notice that in traditional **pandas**, operations are executed immediately, while in **Lazy Pandas**, computation only occurs when you call `.collect()`.

## Memory Usage

Running the previous code on a 5.7GB CSV file with 55 million rows, we can see the memory usage difference between **Pandas** and **Lazy Pandas**:

<div class="grid cards" markdown>
```plotly
{"file_path": "./assets/profiler/lazy_pandas.json"}
```

```plotly
{"file_path": "./assets/profiler/pandas.json"}
```
</div>

In the **Pandas** example, the memory usage spikes to 25.8GB and takes 8 minutes to complete, while in the **Lazy Pandas** example, the memory usage remains constant at 500mb and takes 6 seconds to complete.
For the test, we used a MacBook Pro M1 with 16GB. The dataset used was the [NYC Taxi Dataset](https://www.kaggle.com/code/debjeetdas/nyc-taxi-fare-eda-prediction-using-linear-reg/input?select=train.csv) available on Kaggle.



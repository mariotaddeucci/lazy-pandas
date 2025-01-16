---
title: Lazy Pandas
hide:
  - navigation
  - toc
---

# Lazy Pandas

Welcome to the **Lazy Pandas** official documentation!
A library inspired by [pandas](https://pandas.pydata.org/) that focuses on *lazy* processing, enabling high performance and lower memory usage for large datasets.

## What is Lazy Pandas?

Lazy Pandas is built on the concept of delaying DataFrame operations until they are strictly necessary (lazy evaluation). This allows:
- Operations to be optimized in batches.
- Memory usage to be minimized during processing.
- Total runtime to be reduced for complex pipelines.

## Code Comparison

Below is a side-by-side comparison showing how the same operation would look in **Pandas** versus **Lazy Pandas**:


=== "Lazy Pandas"

    ```python linenums="1" hl_lines="2 5 13"
    import pandas as pd
    import lazy_pandas as lp

    def read_taxi_dataset(location: str) -> pd.DataFrame:
        df = lp.read_csv(location, parse_dates=["pickup_datetime"])
        df = df[["pickup_datetime", "passenger_count"]]
        df["passenger_count"] = df["passenger_count"]
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
        df["passenger_count"] = df["passenger_count"]
        df["pickup_date"] = df["pickup_datetime"].dt.date
        del df["pickup_datetime"]
        df = df.groupby("pickup_date").sum().reset_index()
        df = df[["pickup_date", "passenger_count"]]
        df = df.sort_values("pickup_date")

        return df
    ```

Notice that in traditional **pandas**, operations are executed immediately, while in **Lazy Pandas**, computation only occurs when you call `.collect()`.

## Memory Usage

Below is a fictitious performance comparison between **pandas** and **Lazy Pandas**, showing a scenario where a large dataset is processed in three stages (reading, aggregation, and complex filtering).


<div class="grid cards" markdown>
```plotly
{"file_path": "./assets/profiler/lazy_pandas.json"}
```

```plotly
{"file_path": "./assets/profiler/pandas.json"}
```
</div>



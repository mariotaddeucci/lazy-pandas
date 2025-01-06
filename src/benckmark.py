import time

import pandas as pd
import pandas_lazy as pdl
from memray import Tracker


def read_taxi_dataset_pd(location: str) -> pd.DataFrame:
    df = pd.read_csv(location, parse_dates=["pickup_datetime"])
    df = df[["pickup_datetime", "passenger_count"]]
    df["passenger_count"] = df["passenger_count"]
    df["pickup_date"] = df["pickup_datetime"].dt.date
    del df["pickup_datetime"]
    df = df.groupby("pickup_date").sum().reset_index()
    df = df[["pickup_date", "passenger_count"]]
    df = df.sort_values("pickup_date")
    return df


def read_taxi_dataset(location: str) -> pd.DataFrame:
    df = pdl.read_csv(location, parse_dates=["pickup_datetime"])
    df = df[["pickup_datetime", "passenger_count"]]
    df["passenger_count"] = df["passenger_count"]
    df["pickup_date"] = df["pickup_datetime"].dt.date
    del df["pickup_datetime"]
    df = df.groupby("pickup_date").sum().reset_index()
    df = df[["pickup_date", "passenger_count"]]
    df = df.sort_values("pickup_date")
    df = df.collect()  # Materialize the lazy DataFrame to a pandas DataFrame
    return df


def lazy():
    with Tracker("lazy.bin"):
        df = read_taxi_dataset("~/Downloads/train.csv")
        df.to_parquet("lazy.parquet")
        del df


def simple():
    with Tracker("pandas.bin"):
        df2 = read_taxi_dataset_pd("~/Downloads/train.csv")
        df2.to_parquet("pandas.parquet")
        del df2


if __name__ == "__main__":
    lazy()
    time.sleep(5)
    simple()
    time.sleep(5)

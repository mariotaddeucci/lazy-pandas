import time

import memray
import pandas as pd
import pandas_lazy as pdl

TAXI_TRAIN_TEST_CSV = "/tmp/bench/train.csv"


def with_pandas() -> None:
    df = pd.read_csv(TAXI_TRAIN_TEST_CSV)
    df = df[df["passenger_count"] > 1]
    df["fare_ammount_by_passagenr"] = df["fare_amount"] / df["passenger_count"]
    df["fare_ammount_by_passagenr"] = df["fare_ammount_by_passagenr"].astype("int")
    df = df.drop_duplicates("fare_ammount_by_passagenr")
    time.sleep(1)
    del df
    time.sleep(1)


def with_pandas_lazy() -> None:
    df = pdl.read_csv(TAXI_TRAIN_TEST_CSV)
    df = df[df["passenger_count"] > 1]
    df["fare_ammount_by_passagenr"] = df["fare_amount"] / df["passenger_count"]
    df["fare_ammount_by_passagenr"] = df["fare_ammount_by_passagenr"].astype("int")
    df = df.drop_duplicates("fare_ammount_by_passagenr")
    df = df.collect()  # materialize the LazyFrame to a pandas DataFrame
    time.sleep(1)
    del df
    time.sleep(1)


if __name__ == "__main__":
    with memray.Tracker("profiler.bin"):
        with_pandas()
        with_pandas_lazy()

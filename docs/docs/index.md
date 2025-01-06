---
title: Pandas Lazy
hide:
  - navigation
  - toc
---

# Pandas Lazy

Bem-vindo(a) à documentação oficial do **Pandas Lazy**!
Uma biblioteca inspirada no [pandas](https://pandas.pydata.org/) que foca em processamento “lazy” (preguiçoso), permitindo alto desempenho e menor uso de memória em datasets extensos.

## O que é Pandas Lazy?

O Pandas Lazy é construído sobre o conceito de atrasar a execução de operações em DataFrames até que seja estritamente necessário (lazy evaluation). Isso possibilita que:
- As operações sejam otimizadas em lote.
- O uso de memória seja reduzido ao mínimo durante o processo.
- O tempo de execução total seja menor para pipelines complexos.

## Comparativo de Código

Abaixo, um comparativo lado a lado mostrando como seria a mesma operação em **pandas** e em **Pandas Lazy**:

=== "Pandas Lazy"

    ```python linenums="1" hl_lines="2 5 13"
    import pandas as pd
    import pandas_lazy as pdl

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

Note que no **pandas** tradicional as operações são executadas de imediato, enquanto no **Pandas Lazy** a computação só ocorre quando chamamos `.collect()`.

## Memory Usage

A seguir, apresentamos um comparativo fictício de desempenho entre **pandas** e **Pandas Lazy**, exibindo um cenário em que um grande dataset é processado em 3 etapas (leitura, agregação e filtragem complexa).

<div class="grid cards" markdown>
```plotly
{"file_path": "./assets/profiler/lazy_pandas.json"}
```

```plotly
{"file_path": "./assets/profiler/pandas.json"}
```
</div>



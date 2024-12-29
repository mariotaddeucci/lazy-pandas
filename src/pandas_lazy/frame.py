import uuid
from typing import TYPE_CHECKING, Literal, Union, overload

from duckdb import ColumnExpression, ConstantExpression, DuckDBPyRelation, Expression, StarExpression

from pandas_lazy.column import LazyColumn
from pandas_lazy.exceptions import PandasLazyUnsupporttedOperation

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa

__all__ = ["LazyFrame"]


class LazyFrame:
    def __init__(self, relation: DuckDBPyRelation):
        self._relation = relation

    def collect(self) -> "pd.DataFrame":
        return self._relation.to_df()

    def to_pandas(self) -> "pd.DataFrame":
        return self.collect()

    def to_polars(self) -> "pl.DataFrame":
        return self._relation.pl()

    @overload
    def to_arrow(self, batch_size: Literal[None]) -> "pa.Table": ...
    @overload
    def to_arrow(self, batch_size: int) -> "pa.RecordBatch": ...

    def to_arrow(self, batch_size: int | None = None) -> Union["pa.Table", "pa.RecordBatch"]:
        if batch_size is None:
            return self._relation.arrow()
        return self._relation.arrow(batch_size=batch_size)

    @property
    def columns(self):
        return self._relation.columns.copy()

    @overload
    def __getitem__(self, key: str) -> LazyColumn: ...
    @overload
    def __getitem__(self, key: list[str]) -> "LazyFrame": ...

    def __getitem__(self, key: str | list[str]) -> Union["LazyColumn", "LazyFrame"]:
        if isinstance(key, list):
            return LazyFrame(self._relation.select(*key))

        if isinstance(key, str):
            return LazyColumn(ColumnExpression(key))

        raise PandasLazyUnsupporttedOperation(
            f"PandasLazy does not support all pandas operations, use collect() to get a pandas DataFrame and then perform the operation {key}"
        )

    def head(self, n: int = 10):
        return LazyFrame(self._relation.limit(n))

    def __setitem__(self, key, value):
        if isinstance(key, str):
            if isinstance(value, LazyColumn):
                expr = value.expr.alias(key)
            elif isinstance(value, Expression):
                expr = value.alias(key)
            else:
                expr = ConstantExpression(value).alias(key)

            if key in (columns := self._relation.columns):
                idx = columns.index(key)
                self._relation = self._relation.project(*columns[:idx], expr, *columns[idx + 1 :])
            else:
                self._relation = self._relation.project(*columns, expr)
            return

    @property
    def empty(self) -> bool:
        rel = self._relation.limit(1).fetchone()
        return rel is None

    @overload
    def sort_values(self, by: str | list[str], inplace: Literal[False] = ...) -> "LazyFrame": ...

    @overload
    def sort_values(self, by: str | list[str], inplace: Literal[True] = ...) -> None: ...

    def sort_values(self, by: str | list[str], inplace: bool = False) -> Union["LazyFrame", None]:
        if isinstance(by, str):
            by = [by]

        rel = self._relation.sort(*[ColumnExpression(col) for col in by])
        if inplace:
            self._relation = rel
        else:
            return LazyFrame(rel)

    @overload
    def drop_duplicates(self, subset: list[str] | None = ..., inplace: Literal[False] = ...) -> "LazyFrame": ...

    @overload
    def drop_duplicates(self, subset: list[str] | None = ..., inplace: Literal[True] = ...) -> None: ...

    def drop_duplicates(self, subset: list[str] | None = None, inplace: bool = False) -> Union["LazyFrame", None]:
        if subset is None:
            rel = self._relation.distinct()
        else:
            rn_col = f"tmp_col_{uuid.uuid1().hex}"
            subset_str = ", ".join([f'"{c}"' for c in subset])
            window_spec = f"OVER(PARTITION BY {subset_str}) AS {rn_col}"
            expr = StarExpression(exclude=[rn_col])
            rel = self._relation.row_number(window_spec, "*").filter(f'"{rn_col}" == 1').select(expr)

        if inplace:
            self._relation = rel
        else:
            return LazyFrame(rel)

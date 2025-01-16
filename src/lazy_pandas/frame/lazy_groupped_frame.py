from typing import TYPE_CHECKING

from duckdb import DuckDBPyRelation

if TYPE_CHECKING:
    from lazy_pandas import LazyFrame


class LazyGrouppedFrame:
    def __init__(self, frame: "LazyFrame", by: list[str] | str):
        self._frame = frame
        if isinstance(by, str):
            by = [by]
        self._by = by

    @property
    def _relation(self) -> DuckDBPyRelation:
        return self._frame._relation

    def max(self) -> "LazyFrame":
        agg_cols = set(map(str.lower, self._relation.columns)) - set(map(str.lower, self._by))
        aggs_exprs = [f"max({col}) as {col}" for col in agg_cols]
        aggs_expr = ", ".join(self._by + aggs_exprs)
        rel = self._relation.aggregate(aggr_expr=aggs_expr, group_expr="all")
        return type(self._frame)(rel)

    def sum(self) -> "LazyFrame":
        agg_cols = set(map(str.lower, self._relation.columns)) - set(map(str.lower, self._by))
        aggs_exprs = [f"sum({col}) as {col}" for col in agg_cols]
        aggs_expr = ", ".join(self._by + aggs_exprs)
        rel = self._relation.aggregate(aggr_expr=aggs_expr, group_expr="all")
        return type(self._frame)(rel)

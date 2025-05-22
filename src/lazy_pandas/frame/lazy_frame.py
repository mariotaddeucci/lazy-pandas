import uuid
from typing import TYPE_CHECKING, Literal, Union, overload

import duckdb
from duckdb import (
    ColumnExpression,
    ConstantExpression,
    DuckDBPyRelation,
    Expression,
    FunctionExpression,
    StarExpression,
)
from duckdb.typing import DuckDBPyType

from lazy_pandas.column.lazy_column import LazyColumn
from lazy_pandas.exceptions import ContributionAcceptedError
from lazy_pandas.frame.lazy_groupped_frame import LazyGrouppedFrame

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa

__all__ = ["LazyFrame"]


ColumnOrName = Union["LazyColumn", str]


class LazyFrame:
    _relation: DuckDBPyRelation
    _conn: Union[duckdb.DuckDBPyConnection, None]  # Declare class attribute

    def __init__(self, relation: DuckDBPyRelation):
        """
        Initialize a LazyFrame with a DuckDB relation.

        Args:
            relation (DuckDBPyRelation): The underlying DuckDB relation.
        """
        self._relation = relation
        self._conn = None  # Initialize instance attribute

    def collect(self) -> "pd.DataFrame":
        """
        Collect the lazy relation and materialize it as a pandas DataFrame.

        Returns:
            pd.DataFrame: The materialized DataFrame.
        """
        return self._relation.to_df()

    def dropna(
        self, *, how: Literal["any", "all"] = "any", subset: str | list[str] | None = None, inplace: bool = False
    ) -> Union["LazyFrame", None]:
        operator = " AND " if how == "any" else " OR "

        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]

        filter_expr = operator.join(f'"{col}" IS NOT NULL' for col in subset)
        rel = self._relation.filter(filter_expr)

        if inplace:
            self._relation = rel
        else:
            return LazyFrame(rel)

    def to_pandas(self) -> "pd.DataFrame":
        """
        Alias for `collect()`.

        Returns:
            pd.DataFrame: The materialized DataFrame.
        """
        return self.collect()

    def to_polars(self) -> "pl.DataFrame":
        """
        Convert the relation to a Polars DataFrame.

        Returns:
            pl.DataFrame: The Polars DataFrame.
        """
        return self._relation.pl()

    @overload
    def to_arrow(self, batch_size: Literal[None]) -> "pa.Table": ...

    @overload
    def to_arrow(self, batch_size: int) -> "pa.RecordBatch": ...

    def to_arrow(self, batch_size: int | None = None) -> Union["pa.Table", "pa.RecordBatch"]:
        """
        Convert the relation to an Arrow Table or RecordBatch.

        Args:
            batch_size (int | None): The size of each RecordBatch. If None, returns an Arrow Table.

        Returns:
            pa.Table or pa.RecordBatch: The Arrow representation of the data.
        """
        if batch_size is None:
            return self._relation.arrow()
        return self._relation.arrow(batch_size=batch_size)

    @property
    def columns(self) -> list[str]:
        """
        Get the list of column names in the relation.

        Returns:
            list[str]: A copy of the column names.
        """
        return self._relation.columns.copy()

    def head(self, n: int = 10) -> "LazyFrame":
        """
        Retrieve the first `n` rows of the relation.

        Args:
            n (int): Number of rows to retrieve. Defaults to 10.

        Returns:
            LazyFrame: A new LazyFrame containing the first `n` rows.
        """
        return LazyFrame(self._relation.limit(n))

    @property
    def empty(self) -> bool:
        """
        Check if the relation is empty.

        Returns:
            bool: True if the relation is empty, False otherwise.
        """
        rel = self._relation.limit(1).fetchone()
        return rel is None

    @overload
    def sort_values(self, by: str | list[str], *, inplace: Literal[False] = False) -> "LazyFrame": ...

    @overload
    def sort_values(self, by: str | list[str], *, inplace: Literal[True]) -> None: ...

    def sort_values(self, by: str | list[str], inplace: bool = False) -> Union["LazyFrame", None]:
        """
        Sort the relation by one or more columns.

        Args:
            by (str | list[str]): The column(s) to sort by.
            inplace (bool): Whether to modify the relation in place. Defaults to False.

        Returns:
            LazyFrame or None: A new LazyFrame if not inplace, otherwise None.
        """
        if isinstance(by, str):
            by = [by]

        rel = self._relation.sort(*[ColumnExpression(col) for col in by])
        if inplace:
            self._relation = rel
        else:
            return LazyFrame(rel)

    @overload
    def drop_duplicates(self, subset: str | list[str] | None = None, *, inplace: Literal[False] = False) -> "LazyFrame": ...

    @overload
    def drop_duplicates(self, subset: str | list[str] | None = None, *, inplace: Literal[True]) -> None: ...

    def drop_duplicates(self, subset: str | list[str] | None = None, inplace: bool = False) -> Union["LazyFrame", None]:
        """
        Remove duplicate rows from the relation.

        Args:
            subset (list[str] | None): The subset of columns to consider for duplicates. Defaults to None.
            inplace (bool): Whether to modify the relation in place. Defaults to False.

        Returns:
            LazyFrame or None: A new LazyFrame if not inplace, otherwise None.
        """
        if subset is None:
            rel = self._relation.distinct()
        else:
            if isinstance(subset, str):
                subset = [subset]

            rn_col = f"tmp_col_{uuid.uuid1().hex}"
            subset_str = ", ".join([f'"{c}"' for c in subset])
            window_spec = f"OVER(PARTITION BY {subset_str}) AS {rn_col}"
            expr = StarExpression(exclude=[rn_col])
            rel = self._relation.row_number(window_spec, "*").filter(f'"{rn_col}" == 1').select(expr)

        if inplace:
            self._relation = rel
        else:
            return LazyFrame(rel)

    def astype(self, dtype: Union[str, type, dict[str, Union[str, DuckDBPyType]]]) -> "LazyFrame":
        map_to_apply: dict[str, Union[str, type, DuckDBPyType]]
        if isinstance(dtype, dict):
            # Ensure keys are str and values are type specifiers
            map_to_apply = {str(k): v for k, v in dtype.items()}
        else:  # dtype is str, type, or DuckDBPyType (if not str/type already)
            map_to_apply = {col: dtype for col in self.columns}

        curr = self
        for col, new_type in map_to_apply.items():
            curr[col] = curr[col].astype(new_type)
        return curr

    def explode(self, column: str | list[str]) -> "LazyFrame":
        if isinstance(column, str):
            column = [column]

        rel = self._relation.select(
            StarExpression(exclude=column),
            *[FunctionExpression("explode", ColumnExpression(col)) for col in column],
        )

        return LazyFrame(rel)

    def reset_index(self) -> "LazyFrame":
        return self

    def groupby(self, by: Union[str, list[str]]) -> LazyGrouppedFrame:
        """
        Group the relation by one or more columns.

        Args:
            by (str | list[str]): The column(s) to group by.

        Returns:
            LazyGrouppedFrame: A new LazyGrouppedFrame with the grouped data.
        """
        return LazyGrouppedFrame(self, by)

    def copy(self) -> "LazyFrame":
        """
        Create a copy of the LazyFrame.

        Returns:
            LazyFrame: A new LazyFrame with the same underlying relation.
        """
        return LazyFrame(self._relation)

    def merge(
        self,
        right: Union["LazyFrame", duckdb.DuckDBPyRelation],
        on: str | list[str],
        how: Literal["inner", "right", "left", "outer"] = "inner",
    ) -> "LazyFrame":
        """
        Merge two relations on one or more columns.

        Args:
            right (LazyFrame): The right relation to merge.
            on (str | list[str]): The column(s) to merge on.
            how (str): The type of merge to perform. Defaults to 'inner'.

        Returns:
            LazyFrame: A new LazyFrame with the merged data.
        """
        if isinstance(on, str):
            on = [on]

        right_relation = right._relation if isinstance(right, LazyFrame) else right

        return LazyFrame(self._relation.join(right_relation, *on, how=how))

    def sample(self, n: int | None = None, frac: float | None = None, random_state: int | None = None) -> "LazyFrame":
        """
        Return a random sample of items from the LazyFrame.

        Args:
            n (int | None, optional): Number of items to return. Cannot be used with frac.
                Defaults to None.
            frac (float | None, optional): Fraction of items to return. Cannot be used with n.
                Should be between 0 and 1. Defaults to None.
            random_state (int | None, optional): Seed for the random number generator.
                Defaults to None.

        Returns:
            LazyFrame: A new LazyFrame with the sampled rows.

        Raises:
            ValueError: If both n and frac are specified or if neither is specified.
                       Also if frac is not between 0 and 1.

        Examples:
            ```python
            # Sample 2 rows from the DataFrame
            df.sample(n=2)

            # Sample 10% of rows from the DataFrame
            df.sample(frac=0.1)

            # Sample 5 rows with a fixed random seed
            df.sample(n=5, random_state=42)
            ```
        """
        if (n is None and frac is None) or (n is not None and frac is not None):
            raise ValueError("Exactly one of n or frac must be specified")

        if frac is not None and (frac <= 0 or frac > 1):
            raise ValueError("frac must be between 0 and 1")

        # For simplicity, when a fixed random_state is provided, use pandas' built-in implementation
        # This is a pragmatic solution that ensures reproducibility
        if random_state is not None:
            # First materialize the data to pandas
            df = self._relation.to_df()

            # Use pandas sampling with fixed random_state
            if n is not None:
                sampled_df = df.sample(n=n, random_state=random_state)
            else:
                sampled_df = df.sample(frac=frac, random_state=random_state)

            # Create a new in-memory DuckDB connection that will live with the relation
            # Use the same connection for the relation to avoid it being closed
            import duckdb

            conn = duckdb.connect(":memory:")
            rel = conn.from_df(sampled_df)

            # Store the connection on the object to prevent it from being garbage collected
            # This is important to keep the connection alive as long as the relation is used
            result = LazyFrame(rel)
            result._conn = conn  # Store the connection to prevent premature closing
            return result
        else:
            # Use standard random sampling when no fixed seed is needed
            if n is not None:
                return LazyFrame(self._relation.order("random()").limit(n))
            else:
                return LazyFrame(self._relation.filter(f"random() <= {frac}"))

    @overload
    def __getitem__(self, key: str) -> LazyColumn: ...

    @overload
    def __getitem__(self, key: list[str] | LazyColumn) -> "LazyFrame": ...

    def __getitem__(self, key: str | list[str] | LazyColumn) -> Union[LazyColumn, "LazyFrame"]:
        """
        Select a single column or a subset of columns.

        Args:
            key (str | list[str]): A column name or a list of column names.

        Returns:
            LazyColumn or LazyFrame: The selected column or a LazyFrame with the selected columns.

        Raises:
            ContributionAcceptedError: If an unsupported operation is attempted.
        """
        if isinstance(key, list):
            return LazyFrame(self._relation.select(*key))

        if isinstance(key, str):
            return LazyColumn(ColumnExpression(key))

        if isinstance(key, LazyColumn):
            return LazyFrame(self._relation.filter(key.expr))

        raise ContributionAcceptedError(
            f"LazyPandas does not support all pandas operations, use collect() to get a pandas DataFrame and then perform the operation {key}"
        )

    def __setitem__(self, key, value) -> None:
        """
        Set or update a column in the relation.

        Args:
            key (str): The name of the column.
            value (LazyColumn | Expression | Any): The value or expression for the column.
        """
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

    def __delitem__(self, key: str) -> None:
        self._relation = self._relation.project(StarExpression(exclude=[key]))

    def __repr__(self) -> str:
        return repr(self._relation)

    def __str__(self) -> str:
        return str(self._relation)

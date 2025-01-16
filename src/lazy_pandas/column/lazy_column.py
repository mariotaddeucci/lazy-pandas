from typing import Any, Callable, Literal, Tuple, Union, cast

from duckdb import CoalesceOperator, ConstantExpression, Expression, FunctionExpression
from duckdb.typing import DuckDBPyType

from lazy_pandas.column.lazy_datetime_column import LazyDateTimeColumn
from lazy_pandas.column.lazy_string_column import LazyStringColumn

__all__ = ["LazyColumn"]

ColumnOrName = Union["LazyColumn", str]


class UnsupporttedOperation(Exception): ...


def _get_expr(x) -> Expression:
    return x.expr if isinstance(x, LazyColumn) else ConstantExpression(x)


def _func_op(name: str, doc: str = "") -> Callable[["LazyColumn"], "LazyColumn"]:
    def _(self: "LazyColumn") -> "LazyColumn":
        njc = getattr(self.expr, name)()
        return LazyColumn(njc)

    _.__doc__ = doc
    return _


def _bin_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[["LazyColumn", Any], "LazyColumn"]:
    def _(self: "LazyColumn", other) -> "LazyColumn":
        jc = _get_expr(other)
        njc = getattr(self.expr, name)(jc)
        return LazyColumn(njc)

    _.__doc__ = doc
    return _


class LazyColumn:
    __div__ = _bin_op("__div__")
    __rdiv__ = _bin_op("__rdiv__")

    def __init__(self, expr: Expression):
        """
        Initializes a new instance of LazyColumn.

        Args:
            expr (Expression):
                An expression or object representing the column or dataset
                in the context of LazyColumn.

        Examples:
            ```python
            # Creating a LazyColumn from an expression:
            expr = Expression("col_name")
            lazy_col = LazyColumn(expr)
            # Now lazy_col is tied to the expression "col_name"
            ```
        """
        self.expr = expr

    def abs(self) -> "LazyColumn":
        """
        Returns the absolute value of each element in this column.

        Returns:
            LazyColumn:
                A new LazyColumn where all numeric values are converted
                to their absolute value. Null values remain null.

        Examples:
            ```python
            print(df.head())
            # Example output (illustrative):
            #    col1  my_column_to_test
            # 0     1                 -4
            # 1     2                  0
            # 2     3                 -2
            # 3     4                  5
            # 4     5                 -6

            # Performing the operation on the column
            df["my_column_to_test"].abs()
            # Expected output (LazyColumn in lazy mode):
            # [4, 0, 2, 5, 6]
            ```
        """
        return self.create_from_function("abs", self.expr)

    def round(self, decimals: int = 0) -> "LazyColumn":
        """
        Rounds the numeric values in this column to the specified number of decimal places.

        Args:
            decimals (int, optional):
                Number of decimal places to round to. Defaults to 0.

        Returns:
            LazyColumn:
                A new LazyColumn with rounded values.

        Examples:
            ```python
            print(df.head())
            #    col1  my_column_to_test
            # 0    1               1.2345
            # 1    2               2.6599
            # 2    3               3.1000
            # 3    4               4.9999
            # 4    5               5.0500

            # Rounding to 2 decimal places
            df["my_column_to_test"].round(2)
            # Expected output (LazyColumn in lazy mode):
            # [1.23, 2.66, 3.1, 5.0, 5.05]

            # Rounding without specifying decimals (default=0)
            df["my_column_to_test"].round()
            # [1.0, 3.0, 3.0, 5.0, 5.0]
            ```
        """
        return self.create_from_function("round", self.expr, ConstantExpression(decimals))

    def isin(self, *cols: Any) -> "LazyColumn":
        """
        Checks whether the values in this column are contained in a specified set of values.

        This method is similar to `pandas.Series.isin`. It can accept multiple values
        separated by commas or a single iterable (like a list, set, or tuple).

        Args:
            *cols (Any):
                Values or collections of values to be checked for containment. Can be
                a list, tuple, or multiple values passed separately.

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values, where `True` indicates that the value
                is present in `cols` and `False` otherwise.

        Examples:
            ```python
            print(df.head())
            #    col1  my_column_to_test
            # 0     1                  3
            # 1     2                  4
            # 2     3                  6
            # 3     4                  8
            # 4     5                  3

            # Checking if values are in {3, 8}
            df["my_column_to_test"].isin(3, 8)
            # Expected output (LazyColumn in lazy mode):
            # [True, False, False, True, True]

            # Checking if values are in a list [4, 6]
            df["my_column_to_test"].isin([4, 6])
            # [False, True, True, False, False]
            ```
        """
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cast(Tuple, cols[0])

        cols = cast(Tuple, [_get_expr(c) for c in cols])
        return LazyColumn(self.expr.isin(*cols))

    def astype(self, dtype: str | DuckDBPyType) -> "LazyColumn":
        """
        Converts the type of the values in this column to the specified type.

        Args:
            dtype (str | DuckDBPyType):
                The desired type for conversion. Can be a string (e.g., "INTEGER",
                "VARCHAR") or a DuckDBPyType object.

        Returns:
            LazyColumn:
                A new LazyColumn with values converted to the specified type.

        Examples:
            ```python
            print(df.head())
            #    col1 my_column_to_test
            # 0     1                "1"
            # 1     2                "2"
            # 2     3               None
            # 3     4               "10"
            # 4     5               "20"

            # Converting to integer
            df["my_column_to_test"].astype("INTEGER")
            # [1, 2, None, 10, 20]

            # Converting to string
            df["my_column_to_test"].astype("VARCHAR")
            # ["1", "2", None, "10", "20"]
            ```
        """
        if isinstance(dtype, str):
            dtype = DuckDBPyType(dtype)
        return LazyColumn(self.expr.cast(dtype))

    def fillna(self, value: Any) -> "LazyColumn":
        """
        Replaces null (NA) values in this column with a specified value.

        Args:
            value (Any):
                The value to use for filling null values. Can be
                a number, string, or any valid DuckDB expression.

        Returns:
            LazyColumn:
                A new LazyColumn where all null values have been replaced
                by the specified `value`.

        Examples:
            ```python
            print(df.head())
            #    col1  my_column_to_test
            # 0     1               10.0
            # 1     2               None
            # 2     3                7.5
            # 3     4               None
            # 4     5               12.0

            # Filling null values with 0
            df["my_column_to_test"].fillna(0)
            # [10.0, 0, 7.5, 0, 12.0]
            ```
        """
        return LazyColumn(CoalesceOperator(self.expr, _get_expr(value)))

    def isnull(self) -> "LazyColumn":
        """
        Returns a boolean column indicating whether each value is null (NA).

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values, where `True` indicates
                a null value and `False` indicates a non-null value.

        Examples:
            ```python
            print(df.head())
            #    col1  my_column_to_test
            # 0     1              10.0
            # 1     2               None
            # 2     3               2.0
            # 3     4               None
            # 4     5               4.0

            df["my_column_to_test"].isnull()
            # [False, True, False, True, False]
            ```
        """
        return LazyColumn(self.expr.isnull())

    def isna(self) -> "LazyColumn":
        """
        A synonym for isnull(). Returns a boolean column indicating whether each value is null (NA).

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values, where `True` indicates
                a null value and `False` indicates a non-null value.

        Examples:
            ```python
            print(df.head())
            #    col1 my_column_to_test
            # 0     1             "abc"
            # 1     2             None
            # 2     3             "xyz"
            # 3     4             None
            # 4     5             "foo"

            df["my_column_to_test"].isna()
            # [False, True, False, True, False]
            ```
        """
        return self.isnull()

    def notnull(self) -> "LazyColumn":
        """
        Returns a boolean column indicating whether each value is non-null (NA).

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values, where `True` indicates
                a non-null value and `False` indicates a null value.

        Examples:
            ```python
            print(df.head())
            #    col1 my_column_to_test
            # 0     1             None
            # 1     2             10
            # 2     3             None
            # 3     4             15
            # 4     5             None

            df["my_column_to_test"].notnull()
            # [False, True, False, True, False]
            ```
        """
        return LazyColumn(self.expr.isnotnull())

    def notna(self) -> "LazyColumn":
        """
        A synonym for notnull(). Returns a boolean column indicating whether each value is non-null (NA).

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values, where `True` indicates
                a non-null value and `False` indicates a null value.

        Examples:
            ```python
            print(df.head())
            #    col1  my_column_to_test
            # 0     1               None
            # 1     2               10.0
            # 2     3               None
            # 3     4               20.0
            # 4     5               None

            df["my_column_to_test"].notna()
            # [False, True, False, True, False]
            ```
        """
        return self.notnull()

    def between(
        self, left: Any, right: Any, inclusive: Literal["both", "neither", "left", "right"] = "both"
    ) -> "LazyColumn":
        """
        Returns boolean values indicating whether each element is between two values,
        with optional inclusiveness.

        Similar to `pandas.Series.between`. The `inclusive` parameter can be:
        - "both": includes lower (left) and upper (right) bounds.
        - "neither": excludes both bounds.
        - "left": includes only the lower bound (left).
        - "right": includes only the upper bound (right).

        Args:
            left (Any):
                Lower bound value.
            right (Any):
                Upper bound value.
            inclusive (Literal["both", "neither", "left", "right"], optional):
                Defines whether bounds should be included or excluded.
                Defaults to "both".

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values where `True` indicates
                that the value is between the specified bounds and `False` otherwise.

        Raises:
            ValueError:
                If the `inclusive` parameter is given an invalid value.

        Examples:
            ```python
            print(df.head())
            #    col1  my_column_to_test
            # 0     1                  2
            # 1     2                  3
            # 2     3                  5
            # 3     4                  8
            # 4     5                 10

            # Between 3 and 8 (inclusive of both bounds)
            df["my_column_to_test"].between(3, 8)
            # [False, True, True, True, False]

            # Excluding both bounds
            df["my_column_to_test"].between(3, 8, inclusive="neither")
            # [False, False, True, False, False]

            # Including only the lower bound
            df["my_column_to_test"].between(3, 8, inclusive="left")
            # [False, True, True, False, False]

            # Including only the upper bound
            df["my_column_to_test"].between(3, 8, inclusive="right")
            # [False, False, True, True, False]
            ```
        """
        left_expr = _get_expr(left)
        right_expr = _get_expr(right)

        if inclusive == "both":
            result = self.expr >= left_expr & self.expr <= right_expr
        elif inclusive == "neither":
            result = self.expr > left_expr & self.expr < right_expr
        elif inclusive == "left":
            result = self.expr >= left_expr & self.expr < right_expr
        elif inclusive == "right":
            result = self.expr > left_expr & self.expr <= right_expr
        else:
            raise ValueError(f"Invalid value for inclusive: {inclusive}")

        return LazyColumn(result)

    @classmethod
    def create_from_function(cls, function: str, *arguments: Expression) -> "LazyColumn":
        return LazyColumn(FunctionExpression(function, *arguments))

    @property
    def dt(self) -> LazyDateTimeColumn:
        return LazyDateTimeColumn(self)

    @property
    def str(self) -> LazyStringColumn:
        return LazyStringColumn(self)

    __add__ = _bin_op("__add__")
    __radd__ = _bin_op("__radd__")
    __sub__ = _bin_op("__sub__")
    __rsub__ = _bin_op("__rsub__")
    __mul__ = _bin_op("__mul__")
    __rmul__ = _bin_op("__rmul__")
    __truediv__ = _bin_op("__truediv__")
    __rtruediv__ = _bin_op("__rtruediv__")
    __mod__ = _bin_op("__mod__")
    __rmod__ = _bin_op("__rmod__")
    __pow__ = _bin_op("__pow__")
    __rpow__ = _bin_op("__rpow__")
    __and__ = _bin_op("__and__")
    __rand__ = _bin_op("__rand__")
    __or__ = _bin_op("__or__")
    __ror__ = _bin_op("__ror__")

    def __neg__(self):
        return LazyColumn(-self.expr)

    __invert__ = _func_op("__invert__")
    __lt__ = _bin_op("__lt__")
    __le__ = _bin_op("__le__")

    def __eq__(  # type: ignore[override]
        self,
        other,
    ) -> "LazyColumn":
        return LazyColumn(self.expr == (_get_expr(other)))

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "LazyColumn":
        return LazyColumn(self.expr != (_get_expr(other)))

    __gt__ = _bin_op("__gt__")
    __ge__ = _bin_op("__ge__")

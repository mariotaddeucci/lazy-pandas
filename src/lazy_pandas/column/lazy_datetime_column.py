from typing import TYPE_CHECKING

from duckdb import ConstantExpression

if TYPE_CHECKING:
    from lazy_pandas.column.lazy_column import LazyColumn


class LazyDateTimeColumn:
    def __init__(self, col: "LazyColumn"):
        """
        Initializes a LazyDateTimeColumn wrapper for date and time operations.

        Args:
            col (LazyColumn):
                The LazyColumn instance on which date/time operations will be performed.

        Examples:
            ```python
            # Suppose we have a DataFrame with a datetime column named "my_datetime_column"
            print(df.head())
            # Example output (illustrative):
            #              my_datetime_column
            # 0  2023-01-01 00:00:00
            # 1  2023-01-15 12:34:56
            # 2  2023-03-31 23:59:59
            # 3  2024-05-01 08:00:00
            # 4  2025-12-31 23:00:00

            # We can wrap this column with LazyDateTimeColumn (internally it may be done as df["my_datetime_column"].dt)
            dt_col = LazyDateTimeColumn(df["my_datetime_column"])
            # Now dt_col has methods like year(), month(), day(), etc.
            ```
        """
        self.col = col

    @property
    def date(self) -> "LazyColumn":
        """
        Converts the datetime values to date-only (removes time component).

        Returns:
            LazyColumn:
                A new LazyColumn with date values (YYYY-MM-DD).

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00
            # 1  2023-01-15 12:34:56
            # 2  2023-03-31 23:59:59

            df["my_datetime_column"].dt.date
            # Expected output (LazyColumn in lazy mode):
            # [2023-01-01, 2023-01-15, 2023-03-31, ...]
            ```
        """
        return self.round("d")

    @property
    def year(self) -> "LazyColumn":
        """
        Extracts the year component from each datetime value.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the year.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00
            # 1  2024-05-10 15:30:00
            # 2  2025-12-31 23:59:59

            df["my_datetime_column"].dt.year
            # [2023, 2024, 2025, ...]
            ```
        """
        return self.col.create_from_function("year", self.col.expr)

    @property
    def quarter(self) -> "LazyColumn":
        """
        Extracts the quarter (1 to 4) from each datetime value.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the quarter of the year.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-15 00:00:00  # Q1
            # 1  2023-03-31 23:59:59  # Q1
            # 2  2023-04-01 00:00:00  # Q2
            # 3  2023-07-10 10:10:10  # Q3
            # 4  2023-10-25 11:11:11  # Q4

            df["my_datetime_column"].dt.quarter
            # [1, 1, 2, 3, 4]
            ```
        """
        return self.col.create_from_function("quarter", self.col.expr)

    @property
    def month(self) -> "LazyColumn":
        """
        Extracts the month (1 to 12) from each datetime value.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the month.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00
            # 1  2023-02-15 12:34:56
            # 2  2023-12-31 23:59:59

            df["my_datetime_column"].dt.month
            # [1, 2, 12, ...]
            ```
        """
        return self.col.create_from_function("month", self.col.expr)

    @property
    def day(self) -> "LazyColumn":
        """
        Extracts the day of the month (1 to 31) from each datetime value.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the day of the month.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00  # day=1
            # 1  2023-01-15 12:34:56  # day=15
            # 2  2023-12-31 23:59:59  # day=31

            df["my_datetime_column"].dt.day
            # [1, 15, 31, ...]
            ```
        """
        return self.col.create_from_function("day", self.col.expr)

    @property
    def is_month_start(self) -> "LazyColumn":
        """
        Checks if each datetime value is the start of the month.

        Returns:
            LazyColumn:
                A boolean LazyColumn, where `True` indicates the value is the first day of its month.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00  # start of January
            # 1  2023-01-15 12:34:56
            # 2  2023-02-01 00:00:00  # start of February

            df["my_datetime_column"].dt.is_month_start
            # [True, False, True, ...]
            ```
        """
        return self.col == self.col.create_from_function(
            "date_trunc",
            ConstantExpression("month"),
            self.col.expr,
        )

    @property
    def is_quarter_start(self) -> "LazyColumn":
        """
        Checks if each datetime value is the start of the quarter.

        Returns:
            LazyColumn:
                A boolean LazyColumn, where `True` indicates the value is the first day of its quarter.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00  # start of Q1
            # 1  2023-04-01 12:34:56  # start of Q2
            # 2  2023-07-01 08:00:00  # start of Q3
            # 3  2023-10-01 23:59:59  # start of Q4

            df["my_datetime_column"].dt.is_quarter_start
            # [True, True, True, True, ...]
            ```
        """
        return self.col == self.col.create_from_function(
            "date_trunc",
            ConstantExpression("quarter"),
            self.col.expr,
        )

    @property
    def is_year_start(self) -> "LazyColumn":
        """
        Checks if each datetime value is the start of the year.

        Returns:
            LazyColumn:
                A boolean LazyColumn, where `True` indicates the value is January 1st.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-01 00:00:00  # start of the year
            # 1  2023-02-15 12:34:56
            # 2  2024-01-01 23:59:59  # start of the next year

            df["my_datetime_column"].dt.is_year_start
            # [True, False, True, ...]
            ```
        """
        return self.col == self.col.create_from_function(
            "date_trunc",
            ConstantExpression("year"),
            self.col.expr,
        )

    @property
    def is_month_end(self) -> "LazyColumn":
        """
        Checks if each datetime value is the end of the month.

        Returns:
            LazyColumn:
                A boolean LazyColumn, where `True` indicates the value is the last day of its month.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-31 23:59:59  # end of January
            # 1  2023-02-28 12:34:56
            # 2  2023-02-15 00:00:00
            # 3  2023-02-28 23:59:59  # end of February (non leap year)

            df["my_datetime_column"].dt.is_month_end
            # [True, True, False, True, ...]
            ```
        """
        return self.col == self.col.create_from_function("last_day", self.col.expr)

    @property
    def is_year_end(self) -> "LazyColumn":
        return (self.is_month_end) & (self.month == 12)

    def weekday(self) -> "LazyColumn":
        """
        Returns the day of the week for each datetime value (1 to 7).

        Note:
            The `dayofweek` function in DuckDB returns 1 for Monday through 7 for Sunday.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the weekday,
                where 1=Monday, 7=Sunday.

        Examples:
            ```python
            print(df.head())
            #              my_datetime_column
            # 0  2023-01-02 00:00:00  # Monday
            # 1  2023-01-03 12:34:56  # Tuesday
            # 2  2023-01-07 23:59:59  # Saturday
            # 3  2023-01-08 08:00:00  # Sunday

            df["my_datetime_column"].dt.weekday
            # [1, 2, 6, 7, ...]
            ```
        """
        return self.col.create_from_function("dayofweek", self.col.expr)

    @property
    def hour(self) -> "LazyColumn":
        return self.col.create_from_function("hour", self.col.expr)

    def round(self, freq: str) -> "LazyColumn":
        return self.col.create_from_function("date_trunc", ConstantExpression(freq), self.col.expr)

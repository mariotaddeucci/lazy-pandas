from typing import TYPE_CHECKING, Literal

from duckdb import ConstantExpression

if TYPE_CHECKING:
    from lazy_pandas.column.lazy_column import LazyColumn


class LazyStringColumn:
    def __init__(self, col: "LazyColumn"):
        """
        Initializes a LazyStringColumn wrapper for string operations.

        Args:
            col (LazyColumn):
                The LazyColumn instance on which string operations will be performed.

        Examples:
            ```python
            # Suppose we have a DataFrame with a string column named "my_string_column"
            print(df.head())
            # Example output (illustrative):
            #    col1 my_string_column
            # 0     1             "Hello"
            # 1     2             "world"
            # 2     3            "  Test "
            # 3     4             None
            # 4     5             "FooBar"

            # We can wrap this column with LazyStringColumn (internally it may be done as df["my_string_column"].str)
            str_col = LazyStringColumn(df["my_string_column"])

            # Now str_col has methods like lower(), upper(), strip(), etc.
            ```
        """
        self.col = col

    def lower(self) -> "LazyColumn":
        """
        Converts all characters in the string to lowercase.

        Returns:
            LazyColumn:
                A new LazyColumn with all lowercase string values.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1           "Hello"
            # 1     2           "WORLD"
            # 2     3          "TeStInG"
            # 3     4             None
            # 4     5           "123ABC"

            df["my_string_column"].str.lower()
            # Expected output (LazyColumn in lazy mode):
            # ["hello", "world", "testing", None, "123abc"]
            ```
        """
        return self.col.create_from_function("lower", self.col.expr)

    def upper(self) -> "LazyColumn":
        """
        Converts all characters in the string to uppercase.

        Returns:
            LazyColumn:
                A new LazyColumn with all uppercase string values.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1           "hello"
            # 1     2           "WoRlD"
            # 2     3          "Test123"
            # 3     4             None
            # 4     5            "abc"

            df["my_string_column"].str.upper()
            # ["HELLO", "WORLD", "TEST123", None, "ABC"]
            ```
        """
        return self.col.create_from_function("upper", self.col.expr)

    def strip(self) -> "LazyColumn":
        """
        Strips whitespace from both ends of the string.

        Returns:
            LazyColumn:
                A new LazyColumn with leading and trailing whitespace removed.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "   abc   "
            # 1     2         " def  "
            # 2     3         "xyz   "
            # 3     4          None
            # 4     5        "   hello"

            df["my_string_column"].str.strip()
            # ["abc", "def", "xyz", None, "hello"]
            ```
        """
        return self.col.create_from_function("trim", self.col.expr)

    def lstrip(self) -> "LazyColumn":
        """
        Strips whitespace from the left (leading side) of the string.

        Returns:
            LazyColumn:
                A new LazyColumn with leading whitespace removed.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "   abc"
            # 1     2         "  def "
            # 2     3          None
            # 3     4        "  xyz   "
            # 4     5        " hello  "

            df["my_string_column"].str.lstrip()
            # ["abc", "def ", None, "xyz   ", "hello  "]
            ```
        """
        return self.col.create_from_function("ltrim", self.col.expr)

    def rstrip(self) -> "LazyColumn":
        """
        Strips whitespace from the right (trailing side) of the string.

        Returns:
            LazyColumn:
                A new LazyColumn with trailing whitespace removed.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "abc   "
            # 1     2         "def  "
            # 2     3          None
            # 3     4        "xyz  "
            # 4     5        " hello   "

            df["my_string_column"].str.rstrip()
            # ["abc", "def", None, "xyz", " hello"]
            ```
        """
        return self.col.create_from_function("rtrim", self.col.expr)

    def len(self) -> "LazyColumn":
        """
        Returns the length of each string.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the length of each string.
                Null entries remain null.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1          "Hello"
            # 1     2           None
            # 2     3       "Test test"
            # 3     4         "12345"
            # 4     5       "   xyz  "

            df["my_string_column"].str.len()
            # [5, None, 9, 5, 7]
            ```
        """
        return self.col.create_from_function("len", self.col.expr)

    def replace(self, old: str, new: str) -> "LazyColumn":
        """
        Replaces all occurrences of a substring within each string with a new value.

        Args:
            old (str):
                The substring to be replaced.
            new (str):
                The new string to replace occurrences of `old`.

        Returns:
            LazyColumn:
                A new LazyColumn with all occurrences of `old` replaced by `new`.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1          "Hello"
            # 1     2          "Hello World"
            # 2     3          "foo"
            # 3     4          "bar"
            # 4     5          None

            df["my_string_column"].str.replace("Hello", "Hi")
            # ["Hi", "Hi World", "foo", "bar", None]
            ```
        """
        return self.col.create_from_function(
            "replace",
            self.col.expr,
            ConstantExpression(old),
            ConstantExpression(new),
        )

    def startswith(self, prefix: str) -> "LazyColumn":
        """
        Checks whether each string starts with a given prefix.

        Args:
            prefix (str):
                The prefix to check for.

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values indicating whether each string
                starts with `prefix`. Null entries remain null (or False depending on implementation).

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1          "Hello"
            # 1     2          "HiWorld"
            # 2     3          None
            # 3     4          "Test"
            # 4     5          "HelloTest"

            df["my_string_column"].str.startswith("He")
            # [True, False, None, False, True]
            ```
        """
        return self.col.create_from_function("starts_with", self.col.expr, ConstantExpression(prefix))

    def endswith(self, suffix: str) -> "LazyColumn":
        """
        Checks whether each string ends with a given suffix.

        Args:
            suffix (str):
                The suffix to check for.

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values indicating whether each string
                ends with `suffix`. Null entries remain null (or False depending on implementation).

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1          "Hello"
            # 1     2          "HiWorld"
            # 2     3          None
            # 3     4          "Testing"
            # 4     5          "Example"

            df["my_string_column"].str.endswith("lo")
            # [True, False, None, False, False]
            ```
        """
        return self.col.create_from_function("ends_with", self.col.expr, ConstantExpression(suffix))

    def contains(self, pat: str) -> "LazyColumn":
        """
        Checks whether each string contains a specified substring.

        Args:
            pat (str):
                The substring to look for.

        Returns:
            LazyColumn:
                A new LazyColumn of boolean values indicating whether each string
                contains `pat`. Null entries remain null (or False depending on implementation).

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "Hello"
            # 1     2         "World"
            # 2     3         "Test123"
            # 3     4         None
            # 4     5         "hello world"

            df["my_string_column"].str.contains("lo")
            # [True, False, False, None, True]
            ```
        """
        return self.col.create_from_function("contains", self.col.expr, ConstantExpression(pat))

    def find(self, sub: str) -> "LazyColumn":
        """
        Returns the first zero-based index of the substring `sub` in each string, or -1 if not found.

        Args:
            sub (str):
                The substring to search for.

        Returns:
            LazyColumn:
                A new LazyColumn of integer values representing the first occurrence of `sub`.
                Null entries remain null (or -1 depending on implementation).

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "Hello"
            # 1     2         "abcdef"
            # 2     3         "abcabc"
            # 3     4         None
            # 4     5         "foo"

            # Searching for 'lo'
            df["my_string_column"].str.find("lo")
            # [3, -1, -1, None, -1]
            ```
        """
        return self.col.create_from_function("instr", self.col.expr, ConstantExpression(sub)) - 1

    def pad(
        self,
        width: int,
        side: Literal["left", "right", "both"] = "left",
        fillchar: str = " ",
    ) -> "LazyColumn":
        """
        Pads each string in the column to the specified width with `fillchar`.

        Args:
            width (int):
                The total width of the resulting string after padding.
            side (Literal["left", "right", "both"], optional):
                Which side(s) to pad. Options are "left", "right", or "both".
                Defaults to "left".
            fillchar (str, optional):
                The character to use for padding. Defaults to a space.

        Returns:
            LazyColumn:
                A new LazyColumn with the strings padded to the given width.

        Raises:
            ValueError:
                If `side` is not one of 'left', 'right', or 'both'.
            NotImplementedError:
                If `side='both'` is used, since it's not supported yet.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "abc"
            # 1     2         "1234"
            # 2     3         None
            # 3     4        "hello"
            # 4     5         "x"

            # Padding on the left to a width of 5 using '*'
            df["my_string_column"].str.pad(5, side="left", fillchar="*")
            # ["**abc", "*1234", None, "*hello", "****x"]

            # Padding on the right to a width of 5 using '-'
            df["my_string_column"].str.pad(5, side="right", fillchar="-")
            # ["abc--", "1234-", None, "hello", "x----"]
            ```
        """
        if side not in {"left", "right", "both"}:
            raise ValueError("side must be 'left', 'right', or 'both'")

        if side == "both":
            raise NotImplementedError("side='both' is not supported yet")

        return self.col.create_from_function(
            "lpad" if side == "left" else "rpad",
            self.col.expr,
            ConstantExpression(width),
            ConstantExpression(fillchar),
        )

    def zfill(self, width: int) -> "LazyColumn":
        """
        Pads each string in the column on the left with zeros ('0') to make it of specified width.

        Args:
            width (int):
                The total width of the resulting string after padding.

        Returns:
            LazyColumn:
                A new LazyColumn with the strings left-padded with '0' to the given width.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "abc"
            # 1     2         "1234"
            # 2     3         None
            # 3     4         "5"
            # 4     5         "xyz"

            df["my_string_column"].str.zfill(4)
            # ["0abc", "1234", None, "0005", "0xyz"]
            ```
        """
        return self.pad(width, fillchar="0")

    def ljust(self, width: int, fillchar: str = " ") -> "LazyColumn":
        """
        Left-justifies the string within a field of the specified width, padding with `fillchar` on the right.

        Args:
            width (int):
                The total width of the resulting string after padding.
            fillchar (str, optional):
                The character to use for right-side padding. Defaults to a space.

        Returns:
            LazyColumn:
                A new LazyColumn with strings left-justified to the given width.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "abc"
            # 1     2         "1234"
            # 2     3         None
            # 3     4        "hello"
            # 4     5         "x"

            df["my_string_column"].str.ljust(5, "-")
            # ["abc--", "1234-", None, "hello", "x----"]
            ```
        """
        return self.pad(width, side="left", fillchar=fillchar)

    def rjust(self, width: int, fillchar: str = " ") -> "LazyColumn":
        """
        Right-justifies the string within a field of the specified width, padding with `fillchar` on the left.

        Args:
            width (int):
                The total width of the resulting string after padding.
            fillchar (str, optional):
                The character to use for left-side padding. Defaults to a space.

        Returns:
            LazyColumn:
                A new LazyColumn with strings right-justified to the given width.

        Examples:
            ```python
            print(df.head())
            #    col1 my_string_column
            # 0     1         "abc"
            # 1     2         "1234"
            # 2     3         None
            # 3     4        "hello"
            # 4     5         "x"

            df["my_string_column"].str.rjust(5, "*")
            # ["**abc", "*1234", None, "*hello", "****x"]
            ```
        """
        return self.pad(width, side="right", fillchar=fillchar)

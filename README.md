# Lazy Pandas
Lazy Pandas is a Python library that simplifies the use duckdb wrapping the pandas API. This library is not a pandas replacement, but a way to use the pandas API with DuckDB. Pandas is awesome and adopted by many people, but it is not the best tool for datasets that do not fit in memory. So why not give the power of duckdb to pandas users?

## Installation

To install Lazy Pandas, you can use pip:

```sh
pip install lazy-pandas
```

## Usage

Here is a basic example of how to use Lazy Pandas:

import lazy_pandas as lp

# Load a DataFrame
df = lp.read_csv('data.csv')

# Display the first few rows of the DataFrame
lp.show(df)

# Get descriptive statistics
lp.describe(df)

Features

- File Reading: Simplified functions for reading CSV, Excel, etc.
- Quick Visualization: Functions for quickly displaying DataFrames.
- Statistical Analysis: Functions for obtaining descriptive statistics and other analyses.

Contribution

Contributions are welcome! Feel free to open issues and pull requests.

1. Fork the project
2. Create a branch for your feature (git checkout -b my-feature)
3. Commit your changes (git commit -am 'Add my feature')
4. Push to the branch (git push origin my-feature)
5. Open a Pull Request

License

This project is licensed under the MIT License - see the LICENSE file for details.

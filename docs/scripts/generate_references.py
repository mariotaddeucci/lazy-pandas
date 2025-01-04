import sys
from pathlib import Path

import mkdocs_gen_files

package_dir = Path(__file__).parent.parent.parent / "src"

print(str(package_dir))
sys.path.insert(0, str(package_dir))


import pandas_lazy as pdl  # noqa: E402

vls = [
    ("pandas_lazy.LazyColumn", f"LazyColumn.{attr}", attr)
    for attr in dir(pdl.LazyColumn)
    if not attr.startswith("_") and attr not in ["str", "dt", "create_from_function"]
]

vls += [
    ("pandas_lazy.LazyStringColumn", f"LazyColumn.str.{attr}", attr)
    for attr in dir(pdl.LazyStringColumn)
    if not attr.startswith("_")
]

vls += [
    ("pandas_lazy.LazyDateTimeColumn", f"LazyColumn.dt.{attr}", attr)
    for attr in dir(pdl.LazyDateTimeColumn)
    if not attr.startswith("_")
]

template = """
# {page_name}
::: {function_location}
    options:
        members:
        - {function_name}
"""

for function_location, page_name, function_name in vls:
    with mkdocs_gen_files.open(f"references/LazyColumn/{page_name}.md", "w") as f:
        f.write(template.format(page_name=page_name, function_location=function_location, function_name=function_name))

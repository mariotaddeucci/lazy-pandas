import sys
from pathlib import Path

import mkdocs_gen_files

package_dir = Path(__file__).parent.parent.parent / "src"

print(str(package_dir))
sys.path.insert(0, str(package_dir))


import pandas_lazy as pdl  # noqa: E402

vls = []

vls += [
    (1000 + idx, "pandas_lazy.LazyFrame", f"LazyFrame.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(pdl.LazyFrame)))
    if not attr.startswith("_")
]

vls += [
    (1000 + idx, "pandas_lazy.LazyColumn", f"LazyColumn.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(pdl.LazyColumn)))
    if not attr.startswith("_") and attr not in ["str", "dt", "create_from_function"]
]

vls += [
    (2000 + idx, "pandas_lazy.LazyStringColumn", f"LazyColumn.str.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(pdl.LazyStringColumn)))
    if not attr.startswith("_")
]

vls += [
    (3000 + idx, "pandas_lazy.LazyDateTimeColumn", f"LazyColumn.dt.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(pdl.LazyDateTimeColumn)))
    if not attr.startswith("_")
]

template = """
# {page_name}
::: {function_location}
    options:
        members:
        - {function_name}
"""

for pos, function_location, page_name, function_name in vls:
    parent_page = page_name.split(".", 1)[0]
    with mkdocs_gen_files.open(f"references/{parent_page}/{pos}_{page_name}.md", "w") as f:
        f.write(template.format(page_name=page_name, function_location=function_location, function_name=function_name))


fn_names = [
    attr
    for idx, attr in enumerate(sorted(dir(pdl)))
    if not attr.startswith("_")
    and callable(getattr(pdl, attr))
    and attr not in ["LazyFrame", "LazyColumn", "LazyStringColumn", "LazyDateTimeColumn"]
]


template = """
# pdl.{function_name}
::: pandas_lazy.{function_name}
    options:
        members:
        - {function_name}
"""

for function_name in fn_names:
    with mkdocs_gen_files.open(f"references/General_Functions/pandas_lazy{function_name}.md", "w") as f:
        f.write(template.format(function_name=function_name))

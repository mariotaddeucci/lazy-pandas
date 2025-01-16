import sys
from pathlib import Path

import mkdocs_gen_files

package_dir = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(package_dir))

import lazy_pandas as lp  # noqa: E402

vls = []

vls += [
    (1000 + idx, "lazy_pandas.LazyFrame", f"LazyFrame.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(lp.LazyFrame)))
    if not attr.startswith("_")
]

vls += [
    (1000 + idx, "lazy_pandas.LazyColumn", f"LazyColumn.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(lp.LazyColumn)))
    if not attr.startswith("_") and attr not in ["str", "dt", "create_from_function"]
]

vls += [
    (2000 + idx, "lazy_pandas.LazyStringColumn", f"LazyColumn.str.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(lp.LazyStringColumn)))
    if not attr.startswith("_")
]

vls += [
    (3000 + idx, "lazy_pandas.LazyDateTimeColumn", f"LazyColumn.dt.{attr}", attr)
    for idx, attr in enumerate(sorted(dir(lp.LazyDateTimeColumn)))
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
    for idx, attr in enumerate(sorted(dir(lp)))
    if not attr.startswith("_")
    and callable(getattr(lp, attr))
    and attr not in ["LazyFrame", "LazyColumn", "LazyStringColumn", "LazyDateTimeColumn"]
]


template = """
# lp.{function_name}
::: lazy_pandas.{function_name}
    options:
        members:
        - {function_name}
"""

for function_name in fn_names:
    with mkdocs_gen_files.open(f"references/General_Functions/lazy_pandas{function_name}.md", "w") as f:
        f.write(template.format(function_name=function_name))

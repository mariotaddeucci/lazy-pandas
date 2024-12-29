test:
	uvx hatch test

test-all:
	uvx hatch test --all

format:
	uvx ruff check --fix
	uvx ssort
	uvx ruff format

lint:
	uvx ruff check
	uvx ssort --check
	uvx ruff format --check

docs-serve:
	uvx --with "mkdocstrings-python,mkdocs-material[imaging]" mkdocs serve

UVX = uvx
MKDOCS_OPTS = --with "mkdocstrings-python,mkdocs-material[imaging]"

test:
	$(UVX) hatch test

test-all:
	$(UVX) hatch test --all

format:
	$(UVX) ruff check --fix
	$(UVX) ssort
	$(UVX) ruff format
	$(UVX) pyprojectsort


lint:
	$(UVX) ruff check
	$(UVX) ssort --check
	$(UVX) ruff format --check
	$(UVX) codespell
	$(UVX) pyprojectsort --check

docs-serve:
	$(UVX) $(MKDOCS_OPTS) mkdocs serve

docs-build:
	$(UVX) $(MKDOCS_OPTS) mkdocs build

docs-deploy:
	$(UVX) $(MKDOCS_OPTS) mkdocs gh-deploy --force

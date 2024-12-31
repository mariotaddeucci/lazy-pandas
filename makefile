UVX = uvx
MKDOCS_OPTS = --with-requirements ./docs/requirements.txt

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

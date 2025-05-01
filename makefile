UVX = uvx
MKDOCS_OPTS = --with-requirements requirements.txt

build:
	uv build

test:
	$(UVX) hatch test

test-all:
	$(UVX) hatch test --all

test-cov:
	$(UVX) hatch test --cov=lazy_pandas --cov-report=term --cov-report=html

test-cov-xml:
	$(UVX) hatch test --cov=lazy_pandas --cov-report=xml

coverage-report:
	$(UVX) hatch run python -m http.server -d htmlcov

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
	cd docs && $(UVX) $(MKDOCS_OPTS) mkdocs serve

docs-build:
	cd docs && $(UVX) $(MKDOCS_OPTS) mkdocs build

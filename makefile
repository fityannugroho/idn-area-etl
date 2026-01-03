install:
	uv sync --all-extras

test:
	uv run pytest

test-cov:
	uv run pytest --cov

lint:
	uv run ruff check && uv run ruff format && uv run pyright

build:
	uv build

# Testing

## Test Organization
- `tests/`: Root test directory.
- `tests/fixtures/`: Sample PDFs and expected CSV outputs.
- Test files follow `test_*.py` naming convention.
- Test functions follow `test_*` naming convention.
- Test classes follow `Test*` naming convention.

## Markers
Defined in `pyproject.toml`:
- `e2e`: End-to-end PDF extraction tests.

## Best Practices
- Verify extraction logic against `tests/fixtures/`.
- Add new test cases for new extraction patterns.
- Use `pytest-mock` for mocking dependencies.
- Keep tests focused and isolated.
- Test from the exposed public API. Do not test private methods/attributes directly.

## Coverage
- Source coverage tracked in `src/`.
- Excluded patterns defined in `pyproject.toml` under `[tool.coverage.report]`.

# Workflows

## Development Workflow

### Environment Setup
```bash
uv sync --all-extras
```

### Build & Lint
```bash
uv run ruff check .
uv run pyright
uv build
```

### Testing
```bash
uv run pytest
uv run pytest --cov
```

---

## CLI Commands

### Extract
```bash
idnareaetl extract input.pdf -o output -d ./output/
```

### Validate
```bash
idnareaetl validate province output.province.csv
idnareaetl validate regency output.regency.csv -o report.csv
```

### Normalize
```bash
# Remote ground truth (default)
idnareaetl normalize province dirty.csv -o corrected.csv

# Local ground truth
idnareaetl normalize province dirty.csv -g ./ground_truth/ -o corrected.csv -r report.csv

# Force refresh remote cache
idnareaetl normalize province dirty.csv --refresh-cache

# Check cached version
idnareaetl normalize province dirty.csv --version-info
```

---

## Remote Ground Truth

**Module:** `remote.py` - Downloads and caches ground truth from `github.com/fityannugroho/idn-area-data`.

**Cache Location:** `~/.cache/idn-area-etl/ground-truth/`

**Cache Strategy:**
- Downloads latest release on first use
- Checks for updates every 7 days
- Falls back to cached data if network unavailable
- Metadata stored in `~/.cache/idn-area-etl/metadata.json`

**Key Functions:**
- `get_default_ground_truth_path()` - Returns cached ground truth path
- `_get_latest_release_info()` - Fetches release metadata from GitHub API
- `_download_and_extract_zipball()` - Downloads and extracts release archive
- `show_version_info()` - Displays cached version
- `get_cached_version()` - Returns version string

**GitHub Token:** Set `GITHUB_TOKEN` for higher rate limits (optional, recommended for CI/CD).

---

## Common Patterns

### Adding a New Extractor
1. Create a class inheriting from `TableExtractor` in `extractors.py`
2. Define `areas` frozenset with area keys it handles
3. Implement `matches(df)` to detect compatible tables
4. Implement `_extract_rows(df)` to parse DataFrame into domain rows
5. Register in `cli.py` with context manager

### Adding a New Validator
1. Create a class inheriting from `RowValidator` in `validator.py`
2. Define `area` class attribute with area type
3. Define `expected_columns` as required column names tuple
4. Implement `_validate_code(row)` for code format validation
5. Implement `_validate_area_specific(row)` for additional checks

### Adding Normalization for a New Area
1. Add `normalize_{area}()` method in `Normalizer` class
2. Use `GroundTruthIndex` methods for lookups and fuzzy search
3. Return `RowNormalization` with appropriate status

---

## Flows

### Extraction Flow
1. CLI reads PDF using `camelot.read_pdf()` with `lattice` flavor
2. For each table, iterate through registered extractors
3. First matching extractor processes via `extract_and_write()`
4. Rows buffered in `OutputWriter`, flushed at `batch_size`
5. On completion/interrupt, all buffers flushed and files closed

### Validation Flow
1. Load CSV using pandas
2. Instantiate appropriate `RowValidator` subclass
3. Call `validate()` for each row:
   - `_validate_required_columns()` - check required fields
   - `_validate_code()` - check code format
   - `_validate_area_specific()` - area-specific checks
4. Collect `ValidationError` objects and generate `ValidationReport`

### Normalization Flow
1. Load ground truth CSVs into `GroundTruthIndex`
2. Index builds hierarchical lookups (province → regencies → districts → villages)
3. For each input row:
   - Look up by code in ground truth
   - If name matches exactly → `valid`
   - If name differs → fuzzy search within context
   - If match above threshold → `corrected`
   - If multiple close matches → `ambiguous`
   - If no match → `not_found`
4. Output corrected CSV and/or normalization report

---

## Code Examples

### Fuzzy Search
```python
from idn_area_etl.utils import fuzzy_search_top_n, MatchCandidate

candidates = [
    MatchCandidate(value="ACEH", data={"code": "11"}),
    MatchCandidate(value="SUMATERA UTARA", data={"code": "12"}),
]
results = fuzzy_search_top_n("ACEH BESAR", candidates, n=3, threshold=60.0)
```

### Ground Truth Index
```python
from idn_area_etl.ground_truth import GroundTruthIndex

index = GroundTruthIndex.load_from_directory(Path("./ground_truth/"))
regencies = index.get_regencies_for_province("11")
matches = index.search_name_in_context("BANDA ACEH", province_code="11", area="regency")
```

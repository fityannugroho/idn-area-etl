# idn-area ETL

A command-line interface for extracting Indonesian administrative area data from PDF files, transforming it, and saving it in structured CSV format.

The tool processes PDF tables containing area codes and names for provinces, regencies, districts, villages, and islands, organizing them into separate CSV files with proper hierarchical relationships. It also provides validation and normalization capabilities for data quality assurance.

## Features

- **Extract**: Parse PDF tables and output structured CSV files
- **Validate**: Check data integrity (code formats, parent references, required fields)
- **Normalize**: Correct names using fuzzy matching against ground truth data

## Commands

### Extract

Extract tables of Indonesian administrative areas and islands from PDF.

```
Usage: idnareaetl extract [OPTIONS] PDF_PATH

╭─ Arguments ──────────────────────────────────────────────────────────────────╮
│ *    pdf_path      FILE  Path to the PDF file [required]                     │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --chunk-size   -c      INTEGER    Number of pages to read per chunk          │
│                                   [default: 3]                               │
│ --config               FILE       Path to the configuration TOML file        │
│ --range        -r      TEXT       Specific pages to extract, e.g., '1,3,4'   │
│                                   or '1-4,6'                                 │
│ --output       -o      TEXT       Name of the output CSV file (without       │
│                                   extension)                                 │
│ --destination  -d      DIRECTORY  Destination folder for the output files    │
│ --tmpdir       -t      DIRECTORY  Custom directory for temporary files       │
│ --parallel                        Enable parallel processing for reading PDF │
│                                   tables                                     │
│ --version      -v                 Show the version of this package           │
╰──────────────────────────────────────────────────────────────────────────────╯
```

**Example:**
```bash
idnareaetl extract data/area_codes.pdf -o output -d ./output/
```

This produces:
- `output.province.csv`
- `output.regency.csv`
- `output.district.csv`
- `output.village.csv`
- `output.island.csv`

### Validate

Validate extracted CSV data and report errors.

```
Usage: idnareaetl validate [OPTIONS] AREA INPUT_FILE

╭─ Arguments ──────────────────────────────────────────────────────────────────╮
│ *    area            [province|regency|district|village|island]              │
│                            Area type [required]                              │
│ *    input_file      FILE  Path to the CSV file to validate [required]       │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────╮
│    --output  -o      FILE  Output path for the validation report CSV         │
│    --quiet   -q            Only show summary, suppress detailed errors       │
╰──────────────────────────────────────────────────────────────────────────────╯
```

**Example:**
```bash
# Validate province data
idnareaetl validate province output.province.csv

# Validate with report output
idnareaetl validate regency output.regency.csv -o validation_report.csv
```

**Validation checks include:**
- Code format validation (correct length and pattern)
- Parent code reference validation (e.g., regency must reference valid province)
- Required field presence (code, name)
- Area-specific validations (e.g., coordinates for islands)

### Normalize

Normalize extracted CSV data against ground truth using fuzzy matching.

```
Usage: idnareaetl normalize [OPTIONS] AREA INPUT_FILE

╭─ Arguments ──────────────────────────────────────────────────────────────────╮
│ *    area            [province|regency|district|village|island]              │
│                            Area type [required]                              │
│ *    input_file      FILE  Path to the CSV file to normalize [required]      │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────╮
│    --ground-truth  -g      DIRECTORY  Directory containing ground truth CSV  │
│                            files. If not provided, automatically downloads    │
│                            from github.com/fityannugroho/idn-area-data        │
│    --output        -o      FILE  Output path for the corrected CSV file      │
│    --report        -r      FILE  Output path for the normalization report    │
│    --confidence    -c      FLOAT Minimum fuzzy match confidence (0-100)      │
│                            [default: 80.0]                                   │
│    --quiet         -q            Only show summary, suppress detailed output │
│    --refresh-cache               Force refresh of remote ground truth cache  │
│    --version-info                Show cached ground truth version info       │
╰──────────────────────────────────────────────────────────────────────────────╯
```

**Example:**
```bash
# Normalize province names against ground truth (uses remote data by default)
idnareaetl normalize province dirty.province.csv \
    -o corrected.province.csv -r normalization_report.csv

# Use local ground truth directory
idnareaetl normalize province dirty.province.csv -g ./ground_truth/ \
    -o corrected.province.csv -r normalization_report.csv

# With custom confidence threshold
idnareaetl normalize regency dirty.regency.csv \
    -c 85 -o corrected.regency.csv

# Force refresh remote cache
idnareaetl normalize province dirty.province.csv --refresh-cache

# Check cached ground truth version
idnareaetl normalize province dirty.province.csv --version-info
```

**Ground Truth Directory:**

The `--ground-truth` directory should contain CSV files with area data. Files are auto-detected based on their column headers, so naming is flexible. The directory can contain:

- Province data (columns: `code`, `name`)
- Regency data (columns: `code`, `province_code`, `name`)
- District data (columns: `code`, `regency_code`, `name`)
- Village data (columns: `code`, `district_code`, `name`)
- Island data (columns: `code`, `regency_code`, `name`, plus optional `coordinate`, `is_populated`, `is_outermost_small`)

File names can be anything (e.g., `provinces.csv`, `province_data.csv`, `my_kabupaten.csv`), as the tool detects the area type automatically from the column headers.

**Remote Ground Truth:**

If you don't specify `--ground-truth`, the tool automatically:
1. Downloads the latest release from [fityannugroho/idn-area-data](https://github.com/fityannugroho/idn-area-data)
2. Caches it locally at `~/.cache/idn-area-etl/ground-truth/`
3. Checks for updates every 7 days
4. Works offline using cached data when internet is unavailable

To check your cached ground truth version:
```bash
idnareaetl normalize province input.csv --version-info
```

To force refresh the cache:
```bash
idnareaetl normalize province input.csv --refresh-cache
```

To use a local directory instead of remote data:
```bash
idnareaetl normalize province input.csv -g ./my_ground_truth/
```

**GitHub Token (Optional):**

Set the `GITHUB_TOKEN` environment variable to use authenticated GitHub API requests for higher rate limits:
```bash
export GITHUB_TOKEN=your_token_here
idnareaetl normalize province input.csv
```

**Normalization statuses:**
- `valid`: Name matches ground truth exactly
- `corrected`: Name was corrected via fuzzy matching (above confidence threshold)
- `ambiguous`: Multiple matches found with similar confidence
- `not_found`: No matching record in ground truth

## Workflow Example

A typical workflow for processing PDF data:

```bash
# 1. Extract data from PDF
idnareaetl extract source.pdf -o extracted -d ./output/

# 2. Validate extracted data
idnareaetl validate province ./output/extracted.province.csv
idnareaetl validate regency ./output/extracted.regency.csv

# 3. Normalize against ground truth (uses remote data automatically)
idnareaetl normalize province ./output/extracted.province.csv \
    -o ./output/normalized.province.csv \
    -r ./output/province_report.csv

# Or use local ground truth if you have reference data
idnareaetl normalize province ./output/extracted.province.csv \
    -g ./ground_truth/ -o ./output/normalized.province.csv \
    -r ./output/province_report.csv
```

## Development Setup

### Prerequisites

- [`uv`](https://docs.astral.sh/uv/getting-started/installation) package manager
- Python 3.12 or higher

> Tip: You can use `uv` to install Python. See the [`uv` Python installation guide](https://docs.astral.sh/uv/guides/install-python) for more details.

### Installation Steps

1. Clone this repository
1. Navigate to the project directory
1. Install dependencies using `uv`:
   ```bash
   uv sync --all-extras
   ```
1. Run the tool locally

   You can run the tool directly using `uv` or by activating the virtual environment created by `uv`.

   With `uv`:
   ```bash
   uv run idnareaetl --help
   ```

   From the virtual environment:
   ```bash
   source .venv/bin/activate
   idnareaetl --help
   ```

   > **Note:** To exit the virtual environment, use the command `deactivate`.

## Building the Package

To build the package, you can use the `uv` command:

```bash
uv build
```

## Configuration

You can customize the behavior of the program by providing a configuration file in TOML format and passing its path using the `--config` option.

If no `--config` option is provided, the program will look for a default configuration file in [`idnareaetl.toml`](./src/idn_area_etl/idnareaetl.toml).

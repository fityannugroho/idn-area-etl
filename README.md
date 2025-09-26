# 📄 idn-area ETL

A command-line interface for extracting Indonesian administrative area data from PDF files, transforming it, and saving it in structured CSV format.

The extractor processes PDF tables containing area codes and names for provinces, regencies, districts, and villages, organizing them into separate CSV files with proper hierarchical relationships.

## Usage

```
Usage: idnareaetl [OPTIONS] PDF_PATH

Extract tables of Indonesian administrative areas and islands from PDF. All cleansing,
mapping to final schema, and CSV writing are handled by extractors.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────╮
│ *    pdf_path      FILE  Path to the PDF file [default: None] [required]                 │
╰──────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────╮
│ --chunk-size          -c      INTEGER    Number of pages to read per chunk [default: 3]  │
│ --config                      FILE       Path to the configuration TOML file             │
│                                          [default: None]                                 │
│ --range               -r      TEXT       Specific pages to extract, e.g., '1,3,4' or     │
│                                          '1-4,6'                                         │
│                                          [default: None]                                 │
│ --output              -o      TEXT       Name of the output CSV file (without extension) │
│                                          [default: None]                                 │
│ --destination         -d      DIRECTORY  Destination folder for the output files         │
│ --parallel                               Enable parallel processing for reading PDF      │
│                                          tables                                          │
│ --version             -v                 Show the version of this package                │
│ --install-completion                     Install completion for the current shell.       │
│ --show-completion                        Show completion for the current shell, to copy  │
│                                          it or customize the installation.               │
│ --help                                   Show this message and exit.                     │
╰──────────────────────────────────────────────────────────────────────────────────────────╯
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

If no `--config` option is provided, the program will look for a default configuration file in [`idnareaetl.toml`](idnareaetl.toml).

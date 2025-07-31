import csv
import os
import re
import signal
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Iterator, Annotated

import camelot
import pandas as pd
import typer
from pypdf import PdfReader
from tqdm import tqdm

app = typer.Typer()

# Compiled regex patterns for better performance
RE_BEGIN_DIGITS_NEWLINE = re.compile(r"^\d+\n")
RE_END_DIGITS_NEWLINE = re.compile(r"\n\d+$")
RE_MULTINEWLINE = re.compile(r"\n+")
RE_BEGIN_DIGITS_SPACE = re.compile(r"^\d+\s+")
RE_DOUBLE_SPACE = re.compile(r"\s{2,}")

# Constants for area code lengths
PROVINCE_CODE_LENGTH = 2
REGENCY_CODE_LENGTH = 5
DISTRICT_CODE_LENGTH = 8
VILLAGE_CODE_LENGTH = 13

MAIN_PID = os.getpid()

# Flag to indicate user interrupted
interrupted = False

def handle_sigint(signum, frame) -> None:
    """Handle SIGINT signal (Ctrl+C) gracefully."""
    global interrupted
    interrupted = True
    if os.getpid() == MAIN_PID:
        typer.echo("\n⛔ Aborted by user. Finishing current chunk and exiting...")

signal.signal(signal.SIGINT, handle_sigint)

def clean_name(name: str) -> str:
    """
    Clean area names by removing unwanted digits and normalizing whitespace.

    Steps:
    - Remove digits+\n at the beginning
    - Remove \n+digits at the end
    - Replace remaining \n with spaces
    - Remove digits+space at the beginning
    - Remove double spaces
    """
    if not isinstance(name, str):
        return ""

    # Apply basic text normalization
    text = name.strip().replace("\r", "").replace("\t", " ")

    # Apply regex transformations
    return _apply_regex_transformations(text)


def _apply_regex_transformations(text: str) -> str:
    """Apply regex transformations to clean text."""
    transformations = [
        (RE_BEGIN_DIGITS_NEWLINE, ""),
        (RE_END_DIGITS_NEWLINE, ""),
        (RE_MULTINEWLINE, " "),
        (RE_BEGIN_DIGITS_SPACE, ""),
        (RE_DOUBLE_SPACE, " "),
    ]

    for pattern, replacement in transformations:
        text = pattern.sub(replacement, text)

    return text.strip()

def validate_page_range(page_range: str) -> bool:
    """
    Validate the page range string using regex.
    Allowed formats:
    - Single pages: '1,3,4'
    - Ranges: '1-2,5,7-10'
    """
    pattern = r"^(\d+(-\d+)?)(,(\d+(-\d+)?))*$"
    return bool(re.match(pattern, page_range))

def parse_page_range(page_range: str, total_pages: int) -> list[int]:
    """
    Parse a page range string (e.g., '1,3,4' or '1-2,5,7-10') into a list of valid page numbers.
    """
    pages = set()
    for part in page_range.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            pages.update(range(start, end + 1))
        else:
            pages.add(int(part))
    return sorted(p for p in pages if 1 <= p <= total_pages)

def normalize_header(header: str) -> str:
    """
    Normalize header values by removing extra spaces.

    If the number of single-character words exceeds the number of longer words,
    the phrase is considered "invalid" (likely a misparsed header) and all spaces are removed.
    Otherwise, the phrase is left as is.

    Examples:
        "K o d e" -> "Kode"   # invalid phrase, spaces removed
        "Nama Provinsi" -> "Nama Provinsi"  # valid phrase, unchanged
        "N a m a  P r o v i n s i" -> "NamaProvinsi"  # invalid phrase, spaces removed
    """
    if not isinstance(header, str) or not header:
        return ""

    tokens = header.split()
    if not tokens:
        return ""

    single_char_count = sum(1 for token in tokens if len(token) == 1)
    longer_word_count = len(tokens) - single_char_count

    # If more single characters than longer words, likely misparsed
    return ''.join(tokens) if single_char_count > longer_word_count else header

def is_target_table(df: pd.DataFrame) -> bool:
    """
    Check if the given DataFrame matches the target table structure.

    Returns True if the table has the expected structure with 'kode' as first column
    and 'nama provinsi' in the second column.
    """
    if df.empty or df.shape[0] < 1:
        return False

    # Normalize and set the column headers directly
    normalized_headers = [normalize_header(col).lower() for col in df.iloc[0]]
    df.columns = normalized_headers

    # Check if the table matches the target structure
    return (
        len(normalized_headers) >= 2 and
        normalized_headers[0] == "kode" and
        "nama provinsi" in normalized_headers[1]
    )

def extract_area_code_and_name_from_table(df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Extract area code and name from a DataFrame using vectorized operations.

    Args:
        df: DataFrame containing the table data

    Returns:
        List of tuples containing (code, name) pairs

    Note:
        Skips the first 2 rows assumed as headers.
        Prioritizes column 1 for names, then falls back to columns 4, 5, 6.
    """
    if df.empty or df.shape[1] < 2:
        return []

    # Skip header rows
    data_df = df.iloc[2:].copy()

    if data_df.empty:
        return []

    # Extract codes from first column
    codes = data_df.iloc[:, 0].astype(str).str.strip()

    # Initialize candidates for names
    candidates = pd.Series([""] * len(data_df), index=data_df.index)

    # Prioritize columns for name extraction
    name_columns = [1, 3] if data_df.shape[1] == 6 else [1, 4, 5, 6]
    for col_idx in name_columns:
        if col_idx >= data_df.shape[1]:
            continue

        # Fill empty candidates with non-empty values from current column
        column_data = data_df.iloc[:, col_idx]
        mask = (
            (candidates == "") &
            column_data.notna() &
            column_data.astype(str).str.strip().ne("")
        )
        candidates[mask] = column_data[mask].astype(str)

    # Clean names and filter valid entries
    names = candidates.astype(str).apply(clean_name)
    return [(code, name) for code, name in zip(codes, names) if code and name]

def chunked(iterable: list[int], size: int) -> Iterator[list[int]]:
    """Yield successive chunks of given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def version_option_callback(value: bool) -> None:
    """
    Callback function for the `--version` option.
    """
    if value:
        package_name = "idn-area-etl"
        try:
            typer.echo(f"{package_name}: {version(package_name)}")
            raise typer.Exit()
        except PackageNotFoundError:
            typer.echo(f"{package_name}: Version information not available. Make sure the package is installed.")
            raise typer.Exit(1)

def _validate_inputs(pdf_path: Path, page_range: str | None, output: str | None, destination: Path) -> None:
    """Validate all input parameters."""
    # Validate PDF file extension
    if pdf_path.suffix.lower() != ".pdf":
        typer.echo("❌ The input file must be a PDF.")
        raise typer.Exit(code=1)

    # Validate page range format using regex
    if page_range and not validate_page_range(page_range):
        typer.echo("❌ Invalid page range format. Use formats like '1,3,4' or '1-4,6'.")
        raise typer.Exit(code=1)

    # Validate output file name
    if output:
        if not output.strip():
            typer.echo("❌ Output file name cannot be empty.")
            raise typer.Exit(code=1)
        if any(char in output for char in r'\/:*?"<>|'):
            typer.echo("❌ Invalid characters in output file name.")
            raise typer.Exit(code=1)

    # Ensure the destination is not a file
    if destination.exists() and not destination.is_dir():
        typer.echo("❌ The destination must be a directory.")
        raise typer.Exit(code=1)

def format_duration(duration: float) -> str:
    """Format duration in seconds to a string in h m s, m s, or s."""
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    if minutes:
        return f"{int(minutes)}m {int(seconds)}s"
    return f"{seconds:.2f}s"

@app.command()
def extract(
    pdf_path: Annotated[Path, typer.Argument(exists=True, file_okay=True, dir_okay=False, help="Path to the PDF file")],
    chunk_size: Annotated[int, typer.Option("--chunk-size", "-c", help="Number of pages to read per chunk")] = 3,
    page_range: Annotated[str | None, typer.Option("--range", "-r", help="Specific pages to extract, e.g., '1,3,4' or '1-4,6'")] = None,
    output: Annotated[str | None, typer.Option("--output", "-o", help="Name of the output CSV file (without extension)")] = None,
    destination: Annotated[Path, typer.Option("--destination", "-d", dir_okay=True, file_okay=False, help="Destination folder for the output files", show_default=False)] = Path.cwd(),
    parallel: Annotated[bool, typer.Option("--parallel", help="Enable parallel processing for reading PDF tables")] = False,
    version: Annotated[bool | None, typer.Option("--version", "-v", callback=version_option_callback, is_eager=True, help="Show the version of this package")] = None,
) -> None:
    """
    Extract tables of Indonesian administrative areas data from PDF file and save the cleaned data to a CSV file.
    """
    _validate_inputs(pdf_path, page_range, output, destination)

    # Ensure the destination folder exists
    destination.mkdir(parents=True, exist_ok=True)

    typer.echo("\n🏁 Program started")

    # Start timing
    start_time = time.time()

    # Determine the total number of pages in the PDF
    reader = PdfReader(str(pdf_path))
    total_pages = len(reader.pages)

    # Determine the pages to extract
    pages_to_extract = parse_page_range(page_range, total_pages) if page_range else list(range(1, total_pages + 1))

    output_name = output or pdf_path.stem
    extracted_count = 0
    province_codes = set()

    output_files = {
        'province': destination / f"{output_name}.province.csv",
        'regency': destination / f"{output_name}.regency.csv",
        'district': destination / f"{output_name}.district.csv",
        'village': destination / f"{output_name}.village.csv"
    }

    with open(output_files['province'], mode="w", newline='', encoding="utf-8") as f_province, \
         open(output_files['regency'], mode="w", newline='', encoding="utf-8") as f_regency, \
         open(output_files['district'], mode="w", newline='', encoding="utf-8") as f_district, \
         open(output_files['village'], mode="w", newline='', encoding="utf-8") as f_village:
        writers = {
            'province': csv.writer(f_province),
            'regency': csv.writer(f_regency),
            'district': csv.writer(f_district),
            'village': csv.writer(f_village)
        }

        # Write headers
        writers['province'].writerow(["code", "name"])
        writers['regency'].writerow(["code", "province_code", "name"])
        writers['district'].writerow(["code", "regency_code", "name"])
        writers['village'].writerow(["code", "district_code", "name"])

        with tqdm(total=len(pages_to_extract), desc="📄 Reading pages", colour="green", miniters=1, smoothing=0.1) as pbar:
            for chunk in chunked(pages_to_extract, chunk_size):
                # Check if the user interrupted the process
                if interrupted:
                    break

                page_str = ",".join(str(p) for p in chunk)
                try:
                    page_tables = camelot.read_pdf(str(pdf_path), pages=page_str, flavor="lattice", parallel=parallel)
                except Exception as e:
                    pbar.write(f"⚠️ Error reading pages {page_str}: {e}")
                    pbar.update(len(chunk))
                    continue

                for table in page_tables:
                    if not is_target_table(table.df):
                        continue

                    for code, name in extract_area_code_and_name_from_table(table.df):
                        code_length = len(code)

                        if code_length == PROVINCE_CODE_LENGTH:
                            # Check if it already exists in provinces, ensure no duplicates
                            if code not in province_codes:
                                province_codes.add(code)
                                writers['province'].writerow([code, name])
                        elif code_length == REGENCY_CODE_LENGTH:
                            writers['regency'].writerow([code, code[:PROVINCE_CODE_LENGTH], name])
                        elif code_length == DISTRICT_CODE_LENGTH:
                            writers['district'].writerow([code, code[:REGENCY_CODE_LENGTH], name])
                        elif code_length == VILLAGE_CODE_LENGTH:
                            writers['village'].writerow([code, code[:DISTRICT_CODE_LENGTH], name])

                        extracted_count += 1

                pbar.update(len(chunk))

    # End timing
    end_time = time.time()
    duration = end_time - start_time

    # Check results
    if extracted_count == 0:
        typer.echo("⚠️ No matching data found.")
        typer.echo("Ensure the PDF file contains tables in the correct format.")
        raise typer.Exit(code=1)

    # Display duration in appropriate format
    typer.echo(f"✅ Extraction completed in {format_duration(duration)}")
    typer.echo(f"🧾 Number of rows extracted: {extracted_count}")
    typer.echo(f"📁 Output file saved at: {(destination / f'{output_name}.*.csv').resolve()}")

if __name__ == "__main__":
    app()

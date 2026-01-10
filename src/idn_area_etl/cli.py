import os
import signal
import sys
import time
from enum import Enum
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import FrameType
from typing import Annotated

import camelot
import typer
from pypdf import PdfReader
from tqdm import tqdm

from idn_area_etl.config import AppConfig, Area, ConfigError
from idn_area_etl.extractors import AreaExtractor, IslandExtractor, TableExtractor
from idn_area_etl.ground_truth import GroundTruthIndex
from idn_area_etl.normalizer import normalize_csv
from idn_area_etl.remote import RemoteError, get_default_ground_truth_path, show_version_info
from idn_area_etl.utils import (
    CamelotTempDir,
    chunked,
    format_duration,
    parse_page_range,
    validate_page_range,
)
from idn_area_etl.validator import validate_csv

app = typer.Typer()

MAIN_PID = os.getpid()
interrupted = False


class AreaChoice(str, Enum):
    """Enum for CLI area type selection."""

    province = "province"
    regency = "regency"
    district = "district"
    village = "village"
    island = "island"

    def to_area(self) -> Area:
        """Convert to Area Literal type."""
        return self.value  # type: ignore[return-value]


def handle_sigint(signum: int, frame: FrameType | None) -> None:
    global interrupted
    interrupted = True
    if os.getpid() == MAIN_PID:
        typer.echo("\n⛔ Aborted by user. Finishing current chunk and exiting...")


signal.signal(signal.SIGINT, handle_sigint)


def version_option_callback(value: bool) -> None:
    if value:
        package_name = "idn-area-etl"
        try:
            typer.echo(f"{package_name}: {version(package_name)}")
            raise typer.Exit()
        except PackageNotFoundError:
            typer.echo(
                (
                    f"{package_name}: Version information not available. "
                    "Make sure the package is installed."
                )
            )
            raise typer.Exit(1)


def _validate_inputs(
    pdf_path: Path, page_range: str | None, output: str | None, destination: Path
) -> None:
    if pdf_path.suffix.lower() != ".pdf":
        typer.echo("❌ The input file must be a PDF.")
        raise typer.Exit(code=1)
    if page_range and not validate_page_range(page_range):
        typer.echo("❌ Invalid page range format. Use formats like '1,3,4' or '1-4,6'.")
        raise typer.Exit(code=1)
    if output:
        if not output.strip():
            typer.echo("❌ Output file name cannot be empty.")
            raise typer.Exit(code=1)
        if any(char in output for char in r'\/:*?"<>|'):
            typer.echo("❌ Invalid characters in output file name.")
            raise typer.Exit(code=1)
    if destination.exists() and not destination.is_dir():
        typer.echo("❌ The destination must be a directory.")
        raise typer.Exit(code=1)


@app.command()
def extract(
    pdf_path: Annotated[
        Path,
        typer.Argument(exists=True, file_okay=True, dir_okay=False, help="Path to the PDF file"),
    ],
    chunk_size: Annotated[
        int, typer.Option("--chunk-size", "-c", help="Number of pages to read per chunk")
    ] = 3,
    config_path: Annotated[
        str | None,
        typer.Option(
            "--config",
            help="Path to config file or directory containing idnareaetl.toml",
        ),
    ] = None,
    page_range: Annotated[
        str | None,
        typer.Option("--range", "-r", help="Specific pages to extract, e.g., '1,3,4' or '1-4,6'"),
    ] = None,
    output: Annotated[
        str | None,
        typer.Option("--output", "-o", help="Name of the output CSV file (without extension)"),
    ] = None,
    destination: Annotated[
        Path,
        typer.Option(
            "--destination",
            "-d",
            dir_okay=True,
            file_okay=False,
            help="Destination folder for the output files",
            show_default=False,
        ),
    ] = Path.cwd(),
    tmpdir: Annotated[
        Path | None,
        typer.Option(
            "--tmpdir",
            "-t",
            dir_okay=True,
            file_okay=False,
            help="Custom directory for temporary files",
        ),
    ] = None,
    parallel: Annotated[
        bool,
        typer.Option("--parallel", help="Enable parallel processing for reading PDF tables"),
    ] = False,
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            "-v",
            callback=version_option_callback,
            is_eager=True,
            help="Show the version of this package",
        ),
    ] = None,
) -> None:
    """
    Extract tables of Indonesian administrative areas and islands from PDF.
    All cleansing, mapping to final schema, and CSV writing are handled by extractors.
    """
    _validate_inputs(pdf_path, page_range, output, destination)
    destination.mkdir(parents=True, exist_ok=True)

    # Validate and prepare tmpdir if specified
    if tmpdir:
        tmpdir.mkdir(parents=True, exist_ok=True)
        typer.echo(f"📂 Using custom temp directory: {tmpdir.resolve()}")

    typer.echo("\n🏁 Program started")
    start_time = time.time()

    # Check for migration warning
    if config_path is None and AppConfig._check_cwd_config_exists():  # type: ignore[reportPrivateUsage]
        typer.echo(
            "⚠️  Found 'idnareaetl.toml' in current directory but it won't be used.\n"
            "   Use '--config ./' to load it explicitly.\n"
        )

    try:
        config = AppConfig.load(config_path)
    except ConfigError as exc:
        typer.echo(f"❌ Configuration error: {exc}")
        raise typer.Exit(code=1)

    reader = PdfReader(str(pdf_path))
    total_pages = len(reader.pages)
    pages_to_extract = (
        parse_page_range(page_range, total_pages) if page_range else list(range(1, total_pages + 1))
    )

    output_name = output or pdf_path.stem

    extracted_count = 0
    # Use context managers to guarantee file handles are closed on unexpected exceptions
    with (
        AreaExtractor(destination, output_name, config) as area_extractor,
        IslandExtractor(destination, output_name, config) as island_extractor,
    ):
        extractors: list[TableExtractor] = [area_extractor, island_extractor]

        with tqdm(
            total=len(pages_to_extract),
            desc="📄 Reading pages",
            colour="green",
            miniters=max(1, chunk_size),
            smoothing=0.1,
            disable=not sys.stdout.isatty(),
        ) as pbar:
            for chunk in chunked(pages_to_extract, chunk_size):
                if interrupted:
                    break

                page_str = ",".join(map(str, chunk))

                # Use isolated temp directory for this chunk to prevent temp file accumulation
                # Camelot's TemporaryDirectory.__exit__() is a no-op, causing temp files
                # to accumulate until process exit. This wrapper ensures cleanup after each chunk.
                with CamelotTempDir(base_dir=tmpdir):
                    try:
                        page_tables = camelot.read_pdf(
                            str(pdf_path), pages=page_str, flavor="lattice", parallel=parallel
                        )
                    except Exception as e:
                        pbar.write(f"⚠️ Error reading pages {page_str}: {e}")
                        pbar.update(len(chunk))
                        continue

                    for table in page_tables:
                        df = table.df
                        for ex in extractors:
                            try:
                                if ex.matches(df):
                                    extracted_count += ex.extract_and_write(df)
                                    break  # stop at first matching extractor
                            except Exception as ee:
                                pbar.write(f"⚠️ Extractor error on pages {page_str}: {ee}")
                                continue

                # Temp directory is automatically cleaned up here
                pbar.update(len(chunk))

    duration = time.time() - start_time

    if extracted_count == 0:
        typer.echo("⚠️ No matching data found.")
        typer.echo("Ensure the PDF file contains tables in the correct format.")
        raise typer.Exit(code=1)

    typer.echo(f"✅ Extraction completed in {format_duration(duration)}")
    typer.echo(f"🧾 Number of rows extracted: {extracted_count}")
    typer.echo(f"📁 Output files saved under: {destination.resolve()}")


@app.command()
def validate(
    area: Annotated[
        AreaChoice,
        typer.Argument(help="Area type: province, regency, district, village, island"),
    ],
    input_file: Annotated[
        Path,
        typer.Argument(
            exists=True, file_okay=True, dir_okay=False, help="Path to the CSV file to validate"
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            file_okay=True,
            dir_okay=False,
            help="Output path for the validation report CSV",
        ),
    ] = None,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Only show summary, suppress detailed errors"),
    ] = False,
) -> None:
    """
    Validate extracted CSV data and report errors.

    Checks data integrity including code formats, parent code references,
    required fields, and area-specific validations (e.g., coordinates for islands).
    """
    if input_file.suffix.lower() != ".csv":
        typer.echo("Error: The input file must be a CSV.")
        raise typer.Exit(code=1)

    area_type = area.to_area()
    typer.echo(f"Validating {input_file.name} as '{area_type}' data...")

    # Process validation with progress
    report = None
    with tqdm(
        desc="Validating rows",
        unit=" rows",
        colour="blue",
        disable=not sys.stdout.isatty(),
    ) as pbar:
        for report in validate_csv(input_file, area_type):
            pbar.total = report.total_rows
            pbar.n = report.total_rows
            pbar.refresh()

    if report is None:
        typer.echo("Error: Could not validate file.")
        raise typer.Exit(code=1)

    # Output results
    typer.echo("")
    typer.echo(report.summary())

    if report.has_errors():
        if output:
            report.to_csv(output)
            typer.echo(f"\nReport saved to: {output}")
        elif not quiet:
            typer.echo("\nErrors found:")
            # Show first 20 errors to avoid flooding the terminal
            max_display = 20
            for err in report.errors[:max_display]:
                typer.echo(
                    f"  Row {err.row_number}, {err.column}: {err.error_type} - {err.message}"
                )
            if len(report.errors) > max_display:
                typer.echo(f"  ... and {len(report.errors) - max_display} more errors")
                typer.echo("  Use --output to save all errors to a CSV file.")

        raise typer.Exit(code=1)

    typer.echo("\nValidation passed. No errors found.")


@app.command()
def normalize(
    area: Annotated[
        AreaChoice,
        typer.Argument(help="Area type: province, regency, district, village, island"),
    ],
    input_file: Annotated[
        Path,
        typer.Argument(
            exists=True, file_okay=True, dir_okay=False, help="Path to the CSV file to normalize"
        ),
    ],
    ground_truth_dir: Annotated[
        Path | None,
        typer.Option(
            "--ground-truth",
            "-g",
            exists=True,
            file_okay=False,
            dir_okay=True,
            help=(
                "Directory containing ground truth CSV files. "
                "If not provided, uses remote data from "
                "github.com/fityannugroho/idn-area-data"
            ),
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            file_okay=True,
            dir_okay=False,
            help="Output path for the corrected CSV file",
        ),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            "-r",
            file_okay=True,
            dir_okay=False,
            help="Output path for the normalization report CSV",
        ),
    ] = None,
    confidence: Annotated[
        float,
        typer.Option(
            "--confidence",
            "-c",
            help="Minimum fuzzy match confidence score (0-100)",
        ),
    ] = 80.0,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Only show summary, suppress detailed output"),
    ] = False,
    refresh_cache: Annotated[
        bool,
        typer.Option(
            "--refresh-cache",
            help="Force refresh of remote ground truth cache",
        ),
    ] = False,
    version_info: Annotated[
        bool,
        typer.Option(
            "--version-info",
            help="Show cached ground truth version information and exit",
        ),
    ] = False,
) -> None:
    """
    Normalize extracted CSV data against ground truth.

    Uses fuzzy matching to find and suggest corrections for names that don't
    exactly match ground truth data. Outputs corrected data and/or a report
    of all changes made.
    """
    # Handle version info flag
    if version_info:
        show_version_info()
        raise typer.Exit()

    if input_file.suffix.lower() != ".csv":
        typer.echo("Error: The input file must be a CSV.")
        raise typer.Exit(code=1)

    area_type = area.to_area()

    # Handle ground truth directory
    if ground_truth_dir is None:
        typer.echo("Loading ground truth data from remote cache...")
        try:
            ground_truth_dir = get_default_ground_truth_path(refresh_cache=refresh_cache)
            typer.echo(f"Using cached ground truth at: {ground_truth_dir}")
        except RemoteError as e:
            typer.echo(f"❌ Error loading remote ground truth: {e}")
            typer.echo("💡 Use --ground-truth to specify a local directory")
            raise typer.Exit(code=1)
    else:
        typer.echo(f"Loading ground truth data from {ground_truth_dir}...")

    # Load ground truth
    gt = GroundTruthIndex()
    try:
        gt.load_from_directory(ground_truth_dir)
    except ValueError as e:
        typer.echo(f"Error loading ground truth: {e}")
        raise typer.Exit(code=1)

    if not quiet:
        typer.echo(gt.summary())
        typer.echo("")

    typer.echo(f"Normalizing {input_file.name} as '{area_type}' data...")

    # Run normalization
    norm_report = normalize_csv(
        input_file,
        area_type,
        gt,
        confidence_threshold=confidence,
    )

    # Output results
    typer.echo("")
    typer.echo(norm_report.summary())

    # Determine output headers based on area type
    headers = _get_headers_for_area(area_type)

    # Write corrected CSV if requested
    if output:
        norm_report.write_corrected_csv(output, headers)
        typer.echo(f"\nCorrected CSV saved to: {output}")

    # Write report if requested
    if report:
        norm_report.write_report_csv(report)
        typer.echo(f"Normalization report saved to: {report}")

    # Show sample changes if not quiet
    if not quiet and norm_report.corrected_rows > 0:
        typer.echo("\nSample corrections:")
        shown = 0
        max_show = 10
        for row_norm in norm_report.normalizations:
            if row_norm.status == "corrected" and shown < max_show:
                for sug in row_norm.suggestions:
                    typer.echo(
                        f"  Row {row_norm.row_number}: '{sug.original}' -> "
                        f"'{sug.suggested}' ({sug.confidence:.1f}%)"
                    )
                shown += 1
        if norm_report.corrected_rows > max_show:
            typer.echo(f"  ... and {norm_report.corrected_rows - max_show} more corrections")

    # Show ambiguous rows if not quiet
    if not quiet and norm_report.ambiguous_rows > 0:
        typer.echo("\nAmbiguous rows (manual review needed):")
        shown = 0
        max_show = 5
        for row_norm in norm_report.normalizations:
            if row_norm.status == "ambiguous" and shown < max_show:
                typer.echo(f"  Row {row_norm.row_number}: Multiple possible matches")
                for sug in row_norm.suggestions[:3]:
                    typer.echo(f"    - '{sug.suggested}' ({sug.confidence:.1f}%)")
                shown += 1
        if norm_report.ambiguous_rows > max_show:
            typer.echo(f"  ... and {norm_report.ambiguous_rows - max_show} more ambiguous rows")

    # Exit with error if there are unresolved issues
    if norm_report.not_found_rows > 0 or norm_report.ambiguous_rows > 0:
        typer.echo(
            f"\nWarning: {norm_report.not_found_rows} rows not found, "
            f"{norm_report.ambiguous_rows} rows ambiguous."
        )
        if not output:
            typer.echo("Use --output to save corrected data.")
        raise typer.Exit(code=1)

    typer.echo("\nNormalization completed successfully.")


def _get_headers_for_area(area: Area) -> list[str]:
    """Get the expected CSV headers for an area type."""
    headers: dict[Area, list[str]] = {
        "province": ["code", "name"],
        "regency": ["code", "province_code", "name"],
        "district": ["code", "regency_code", "name"],
        "village": ["code", "district_code", "name"],
        "island": [
            "code",
            "regency_code",
            "coordinate",
            "is_populated",
            "is_outermost_small",
            "name",
        ],
    }
    return headers.get(area, [])


if __name__ == "__main__":
    app()

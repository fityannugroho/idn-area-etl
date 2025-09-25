import os
import signal
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import FrameType
import sys
from typing import Annotated

import camelot
from pypdf import PdfReader
from tqdm import tqdm
import typer

from idn_area_etl.config import AppConfig, ConfigError
from idn_area_etl.extractors import AreaExtractor, IslandExtractor, TableExtractor
from idn_area_etl.utils import (
    chunked,
    format_duration,
    parse_page_range,
    validate_page_range,
)

app = typer.Typer()

MAIN_PID = os.getpid()
interrupted = False


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
        Path | None,
        typer.Option(
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            help="Path to the configuration TOML file",
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

    typer.echo("\n🏁 Program started")
    start_time = time.time()

    try:
        config = AppConfig.load(config_path) if config_path else AppConfig.load()
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

                pbar.update(len(chunk))

    duration = time.time() - start_time

    if extracted_count == 0:
        typer.echo("⚠️ No matching data found.")
        typer.echo("Ensure the PDF file contains tables in the correct format.")
        raise typer.Exit(code=1)

    typer.echo(f"✅ Extraction completed in {format_duration(duration)}")
    typer.echo(f"🧾 Number of rows extracted: {extracted_count}")
    typer.echo(f"📁 Output files saved under: {destination.resolve()}")


if __name__ == "__main__":
    app()

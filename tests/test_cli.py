import os
from pathlib import Path
from typing import Any
import signal

import pandas as pd
import pytest
import typer

from idn_area_etl.cli import extract, version_option_callback, handle_sigint


class _StubTable:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df


def _df_area_min():
    return pd.DataFrame(
        [
            [
                "K O D E",
                "NAMA PROVINSI / KABUPATEN / KOTA",
                "JUMLAH",
                "",
                "N A M A / J U M L A H",
                "",
                "",
                "LUAS WILAYAH (Km2)",
                "K E T E R A N G A N",
            ],
            ["", "KAB", "KOTA", "KECAMATAN", "KELURAHAN", "D E S A", "", "", ""],
            [
                "11",
                "Aceh",
                "",
                "",
                "",
                "",
                "",
                "",
                "...",
            ],
            [
                "11.01",
                "Kabupaten Aceh Selatan",
                "18",
                "0",
                "260",
                "",
                "4.174,211",
                "...",
            ],
        ]
    )


def _df_island_min():
    return pd.DataFrame(
        [
            [
                "Kode Pulau",
                "Nama Provinsi, Kabupaten/Kota, Pulau",
                "Jumlah",
                "Koordinat",
                "Luas\n2\n(Km )",
                "BP/TBP",
                "Keterangan",
            ],
            ["11.01", "Kabupaten Aceh Selatan", "6", "", "", "", ""],
            [
                "11.01.40001",
                "Pulau Batukapal",
                "",
                "03°19'03.44\" U 097°07'41.73\" T",
                "0.0006",
                "TBP",
                "",
            ],
        ]
    )


class TestExtractFunction:
    """Integration-ish tests for the public extract() function."""

    def test_extract_writes_outputs_when_tables_match(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        # Stub PdfReader to avoid real PDF parsing
        class _StubReader:
            def __init__(self, *_: Any, **__: Any) -> None:
                self.pages = [object(), object()]  # 2 pages

        # Stub camelot.read_pdf to return our fake tables regardless of pages arg
        def _stub_read_pdf(_path: str, pages: str, flavor: str, parallel: bool):
            return [
                _StubTable(_df_area_min()),
                _StubTable(_df_island_min()),
            ]

        from idn_area_etl import cli as cli_mod

        monkeypatch.setattr(cli_mod, "PdfReader", _StubReader)
        monkeypatch.setattr(cli_mod.camelot, "read_pdf", _stub_read_pdf)

        pdf_file = tmp_path / "input.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")  # existence + suffix only
        dest = tmp_path / "out"

        extract(
            pdf_path=pdf_file,
            chunk_size=2,
            page_range=None,  # will use StubReader.pages -> [1, 2]
            output="result",
            destination=dest,
            parallel=False,
            version=None,
        )

        # Expect at least 2 kinds of outputs (area & island)
        assert (dest / "result.province.csv").exists()
        assert (dest / "result.regency.csv").exists()
        assert (dest / "result.island.csv").exists()

        # Quick content check
        assert "Aceh" in (dest / "result.province.csv").read_text(encoding="utf-8")
        assert "Kabupaten Aceh Selatan" in (dest / "result.regency.csv").read_text(encoding="utf-8")
        assert "Pulau Batukapal" in (dest / "result.island.csv").read_text(encoding="utf-8")

    def test_extract_uses_pdf_stem_when_output_empty_or_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        # Test that empty output or None falls back to pdf_path.stem
        class _StubReader:
            def __init__(self, *_: Any, **__: Any) -> None:
                self.pages = [object(), object()]  # 2 pages

        def _stub_read_pdf(_path: str, pages: str, flavor: str, parallel: bool):
            return [
                _StubTable(_df_area_min()),
                _StubTable(_df_island_min()),
            ]

        from idn_area_etl import cli as cli_mod

        monkeypatch.setattr(cli_mod, "PdfReader", _StubReader)
        monkeypatch.setattr(cli_mod.camelot, "read_pdf", _stub_read_pdf)

        pdf_file = tmp_path / "my_test_file.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        dest = tmp_path / "out"

        # Test with empty string output
        extract(
            pdf_path=pdf_file,
            chunk_size=2,
            page_range=None,
            output="",  # Empty string should fallback to pdf stem
            destination=dest,
            parallel=False,
            version=None,
        )

        # Should use pdf stem "my_test_file" as output name
        assert (dest / "my_test_file.province.csv").exists()
        assert (dest / "my_test_file.regency.csv").exists()
        assert (dest / "my_test_file.island.csv").exists()

        # Clean up for next test
        for f in dest.glob("*.csv"):
            f.unlink()

        # Test with None output (default)
        extract(
            pdf_path=pdf_file,
            chunk_size=2,
            page_range=None,
            output=None,  # None should also fallback to pdf stem
            destination=dest,
            parallel=False,
            version=None,
        )

        # Should also use pdf stem "my_test_file" as output name
        assert (dest / "my_test_file.province.csv").exists()
        assert (dest / "my_test_file.regency.csv").exists()
        assert (dest / "my_test_file.island.csv").exists()

    def test_extract_fails_when_output_only_whitespaces(self, tmp_path: Path):
        pdf_file = tmp_path / "input.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        dest = tmp_path / "out"

        with pytest.raises(typer.Exit) as e:
            extract(
                pdf_path=pdf_file,
                chunk_size=2,
                page_range=None,
                output="   ",  # Only whitespace
                destination=dest,
                parallel=False,
                version=None,
            )
        assert e.value.exit_code == 1  # "Invalid output name."

    def test_extract_fails_when_no_matching_tables(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        class _StubReader:
            def __init__(self, *_: Any, **__: Any) -> None:
                self.pages = [object()]  # 1 page

        def _stub_read_pdf(_path: str, pages: str, flavor: str, parallel: bool):
            # Table not recognized by any extractor
            return [_StubTable(pd.DataFrame([["Foo", "Bar"], ["1", "2"]]))]

        from idn_area_etl import cli as cli_mod

        monkeypatch.setattr(cli_mod, "PdfReader", _StubReader)
        monkeypatch.setattr(cli_mod.camelot, "read_pdf", _stub_read_pdf)

        pdf_file = tmp_path / "input.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")

        with pytest.raises(typer.Exit) as e:
            extract(
                pdf_path=pdf_file,
                chunk_size=1,
                page_range="1",
                output="none",
                destination=tmp_path,
                parallel=False,
                version=None,
            )
        assert e.value.exit_code == 1  # "No matching data found."

    def test_extract_rejects_bad_inputs(self, tmp_path: Path):
        # Non-PDF path
        not_pdf = tmp_path / "input.txt"
        not_pdf.write_text("hello")
        with pytest.raises(typer.Exit):
            extract(
                pdf_path=not_pdf,
                chunk_size=1,
                page_range=None,
                output="ok",
                destination=tmp_path,
                parallel=False,
                version=None,
            )

        # Bad page range
        pdf_file = tmp_path / "x.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        with pytest.raises(typer.Exit):
            extract(
                pdf_path=pdf_file,
                chunk_size=1,
                page_range="1,,3",
                output="ok",
                destination=tmp_path,
                parallel=False,
                version=None,
            )

    def test_extract_rejects_invalid_output_characters(self, tmp_path: Path):
        # Invalid characters in output name
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        invalid_chars = r'\/:*?"<>|'
        for char in invalid_chars:
            with pytest.raises(typer.Exit):
                extract(
                    pdf_path=pdf_file,
                    chunk_size=1,
                    page_range=None,
                    output=f"output{char}name",
                    destination=tmp_path,
                    parallel=False,
                    version=None,
                )

    def test_extract_rejects_file_as_destination(self, tmp_path: Path):
        # File path as destination (not directory)
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        dest_file = tmp_path / "dest.txt"
        dest_file.write_text("not a directory")

        with pytest.raises(typer.Exit):
            extract(
                pdf_path=pdf_file,
                chunk_size=1,
                page_range=None,
                output="ok",
                destination=dest_file,
                parallel=False,
                version=None,
            )

    def test_extract_handles_camelot_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        # Test error handling when camelot.read_pdf fails
        class _StubReader:
            def __init__(self, *_: Any, **__: Any) -> None:
                self.pages = [object()]

        def _stub_read_pdf_error(_path: str, pages: str, flavor: str, parallel: bool):
            raise Exception("Camelot parsing error")

        from idn_area_etl import cli as cli_mod

        monkeypatch.setattr(cli_mod, "PdfReader", _StubReader)
        monkeypatch.setattr(cli_mod.camelot, "read_pdf", _stub_read_pdf_error)

        pdf_file = tmp_path / "input.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        dest = tmp_path / "out"

        # Should still exit with code 1 due to no matching data found
        with pytest.raises(typer.Exit) as e:
            extract(
                pdf_path=pdf_file,
                chunk_size=1,
                page_range=None,
                output="result",
                destination=dest,
                parallel=False,
                version=None,
            )
        assert e.value.exit_code == 1

    def test_extract_handles_extractor_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        # Test error handling when extractor fails
        class _StubReader:
            def __init__(self, *_: Any, **__: Any) -> None:
                self.pages = [object()]

        def _stub_read_pdf(_path: str, pages: str, flavor: str, parallel: bool):
            return [_StubTable(_df_area_min())]

        from idn_area_etl import cli as cli_mod

        # Mock AreaExtractor to raise exception
        original_area_extractor = cli_mod.AreaExtractor

        class _ErrorAreaExtractor(original_area_extractor):
            def extract_and_write(self, df: pd.DataFrame) -> int:
                raise Exception("Extractor processing error")

        monkeypatch.setattr(cli_mod, "PdfReader", _StubReader)
        monkeypatch.setattr(cli_mod.camelot, "read_pdf", _stub_read_pdf)
        monkeypatch.setattr(cli_mod, "AreaExtractor", _ErrorAreaExtractor)

        pdf_file = tmp_path / "input.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")
        dest = tmp_path / "out"

        # Should continue processing despite extractor error but eventually exit with no data
        with pytest.raises(typer.Exit) as e:
            extract(
                pdf_path=pdf_file,
                chunk_size=1,
                page_range=None,
                output="result",
                destination=dest,
                parallel=False,
                version=None,
            )
        assert e.value.exit_code == 1


class TestSignalHandler:
    """Tests for the signal handler function."""

    def test_handle_sigint_sets_interrupted_flag(self, monkeypatch: pytest.MonkeyPatch):
        from idn_area_etl import cli as cli_mod

        # Reset interrupted flag
        cli_mod.interrupted = False

        # Mock os.getpid to return MAIN_PID
        monkeypatch.setattr(os, "getpid", lambda: cli_mod.MAIN_PID)

        # Mock typer.echo to capture output
        echo_calls: list[str] = []

        def mock_echo(msg: str) -> None:
            echo_calls.append(msg)

        monkeypatch.setattr(typer, "echo", mock_echo)

        # Call signal handler
        handle_sigint(signal.SIGINT, None)

        # Verify interrupted flag is set
        assert cli_mod.interrupted is True

        # Verify echo was called with abort message
        assert len(echo_calls) == 1
        assert "Aborted by user" in echo_calls[0]

    def test_handle_sigint_different_pid(self, monkeypatch: pytest.MonkeyPatch):
        from idn_area_etl import cli as cli_mod

        # Reset interrupted flag
        cli_mod.interrupted = False

        # Mock os.getpid to return different PID
        monkeypatch.setattr(os, "getpid", lambda: cli_mod.MAIN_PID + 1)

        # Mock typer.echo to capture output
        echo_calls: list[str] = []

        def mock_echo(msg: str) -> None:
            echo_calls.append(msg)

        monkeypatch.setattr(typer, "echo", mock_echo)

        # Call signal handler
        handle_sigint(signal.SIGINT, None)

        # Verify interrupted flag is set but no echo called
        assert cli_mod.interrupted is True
        assert len(echo_calls) == 0

    def test_extract_breaks_on_interrupt_branch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """
        Ensure the True-branch of `if interrupted: break` is executed.
        We flip the `interrupted` flag during the first chunk so the second chunk hits `break`.
        """
        from idn_area_etl import cli as cli_mod

        class _StubReader:
            def __init__(self, *_: object, **__: object) -> None:
                # 4 pages -> with chunk_size=1, we get multiple iterations
                self.pages = [object(), object(), object(), object()]

        # Reset flag & stub PdfReader
        cli_mod.interrupted = False
        monkeypatch.setattr(cli_mod, "PdfReader", _StubReader)

        # Stub camelot.read_pdf: on first call, flip `interrupted=True`
        call_count = {"n": 0}

        def _stub_read_pdf(_path: str, pages: str, flavor: str, parallel: bool):
            call_count["n"] += 1
            if call_count["n"] == 1:
                cli_mod.interrupted = True  # will affect the *next* loop iteration
            return [type("T", (), {"df": _df_area_min()})()]  # one table with minimal area df

        monkeypatch.setattr(cli_mod.camelot, "read_pdf", _stub_read_pdf)

        # Run extract
        pdf_file = tmp_path / "in.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%fake")  # existence + suffix check only
        dest = tmp_path / "out"

        try:
            cli_mod.extract(
                pdf_path=pdf_file,
                chunk_size=1,  # ensure many loop iterations
                page_range=None,
                output="x",
                destination=dest,
                parallel=False,
                version=None,
            )
        finally:
            # Always reset the global flag so other tests aren't affected
            cli_mod.interrupted = False

        # Assert that we actually stopped early: read_pdf called only once
        assert call_count["n"] == 1, "Expected to break on the 2nd iteration (after 1 read)."

        # And we still produced some output (so the command didn't exit with code=1)
        assert (dest / "x.province.csv").exists()


class TestVersionOptionCallback:
    """Tests for the public version_option_callback function."""

    def test_version_prints_and_exits_successfully(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ):
        from idn_area_etl import cli as cli_mod

        def _fake_version(_: str) -> str:
            return "1.2.3"

        monkeypatch.setattr(cli_mod, "version", _fake_version)
        with pytest.raises(typer.Exit) as e:
            version_option_callback(True)
        assert e.value.exit_code == 0
        assert "1.2.3" in capsys.readouterr().out

    def test_version_handles_missing_package(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ):
        from importlib.metadata import PackageNotFoundError
        from idn_area_etl import cli as cli_mod

        def _raise(_: str) -> None:
            raise PackageNotFoundError()

        monkeypatch.setattr(cli_mod, "version", _raise)
        with pytest.raises(typer.Exit) as e:
            version_option_callback(True)
        assert e.value.exit_code == 1
        assert "Version information not available" in capsys.readouterr().out

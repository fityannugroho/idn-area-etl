from pathlib import Path

import pytest

from idn_area_etl.writer import OutputWriter


def _read(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def test_context_manager_writes_and_flushes(tmp_path: Path) -> None:
    target = tmp_path / "out.csv"
    with OutputWriter(target, header=("a", "b")) as writer:
        writer.add([["1", "2"], ["3", "4"]])
        assert len(writer) == 2
        writer.flush()

    assert target.exists()
    lines = _read(target)
    assert lines[0] == "a,b"
    assert lines[1] == "1,2"
    assert lines[2] == "3,4"


def test_open_twice_raises_runtime_error(tmp_path: Path) -> None:
    target = tmp_path / "duplicate.csv"
    writer = OutputWriter(target)
    writer.open()
    try:
        with pytest.raises(RuntimeError):
            writer.open()
    finally:
        writer.close()


def test_close_without_open_is_noop(tmp_path: Path) -> None:
    target = tmp_path / "noop.csv"
    writer = OutputWriter(target)
    writer.close()
    assert not target.exists()


def test_flush_without_open_raises(tmp_path: Path) -> None:
    target = tmp_path / "flush.csv"
    writer = OutputWriter(target)
    writer.add([["1"]])
    with pytest.raises(RuntimeError):
        writer.flush()

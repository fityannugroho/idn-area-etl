import csv
from pathlib import Path
from types import TracebackType
from typing import Any, Iterable


class OutputWriter:
    """
    A simple writer interface for writing data to a CSV file with buffering.
    """

    def __init__(self, path: Path, *, header: Iterable[str] | None = None) -> None:
        self.path = path
        self.header = header
        self._buffer: list[Iterable[Any]] = []
        self._file_handler = None
        self._writer = None

    def __enter__(self) -> "OutputWriter":
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __len__(self) -> int:
        return len(self._buffer)

    def open(self) -> None:
        """Open the file for writing and initialize the CSV writer."""

        if self._file_handler is not None:
            raise RuntimeError("OutputWriter is already open")

        self._file_handler = open(
            self.path, mode="w", newline="", encoding="utf-8", buffering=1048576
        )
        self._writer = csv.writer(self._file_handler)

        if self.header:
            self._writer.writerow(self.header)

    def close(self) -> None:
        """Flush any remaining data in the buffer and close the file."""

        if self._file_handler is None:
            return

        self.flush()
        self._file_handler.close()
        self._file_handler = None
        self._writer = None

    def flush(self) -> None:
        """Flush any remaining data in the buffer to the file."""

        if not self._buffer:
            return
        if self._writer is None or self._file_handler is None:
            raise RuntimeError("OutputWriter is not open")

        self._writer.writerows(self._buffer)
        self._file_handler.flush()
        self._buffer.clear()

    def add(self, buffer: Iterable[Iterable[Any]]) -> None:
        """Add data to the buffer."""
        self._buffer.extend(buffer)

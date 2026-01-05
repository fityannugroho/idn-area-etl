import re
from dataclasses import dataclass
from typing import Iterator

from rapidfuzz import fuzz, process

# =========================
# Code format patterns and constants
# =========================

# Code length constants
PROVINCE_CODE_LENGTH = 2
REGENCY_CODE_LENGTH = 5
DISTRICT_CODE_LENGTH = 8
VILLAGE_CODE_LENGTH = 13

# Code validation patterns
# Province: "NN" (e.g., "11")
RE_PROVINCE_CODE = re.compile(r"^\d{2}$")
# Regency: "NN.NN" (e.g., "11.01")
RE_REGENCY_CODE = re.compile(r"^\d{2}\.\d{2}$")
# District: "NN.NN.NN" (e.g., "11.01.02")
RE_DISTRICT_CODE = re.compile(r"^\d{2}\.\d{2}\.\d{2}$")
# Village: "NN.NN.NN.NNNN" (e.g., "11.01.02.2001")
RE_VILLAGE_CODE = re.compile(r"^\d{2}\.\d{2}\.\d{2}\.\d{4}$")
# Island: "NN.NN.NNNNN" (e.g., "11.01.40001")
RE_ISLAND_CODE = re.compile(r"^\d{2}\.\d{2}\.\d{5}$")
# Coordinate format: "DD°MM'SS.SS" N/S DD°MM'SS.SS" E/W
RE_COORDINATE = re.compile(
    r"^\d{1,3}°\d{1,2}'\d{1,2}(?:\.\d+)?\"?\s+[NSEWUTB]+\s+"
    r"\d{1,3}°\d{1,2}'\d{1,2}(?:\.\d+)?\"?\s+[NSEWUTB]+$",
    re.IGNORECASE,
)

# =========================
# Text cleaning patterns
# =========================
RE_BEGIN_DIGITS_NEWLINE = re.compile(r"^\d+\n")
RE_END_DIGITS_NEWLINE = re.compile(r"\n\d+$")
RE_MULTINEWLINE = re.compile(r"\n+")
RE_BEGIN_DIGITS_SPACE = re.compile(r"^\d+\s+")
RE_DOUBLE_SPACE = re.compile(r"\s{2,}")


def _apply_regex_transformations(text: str) -> str:
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


def clean_name(name: str) -> str:
    text = name.strip().replace("\r", "").replace("\t", " ")
    return _apply_regex_transformations(text)


def fix_wrapped_name(name: str, max_line_length: int = 16) -> str:
    if not name:
        return ""
    if "\n" not in name:
        return name.rstrip()
    lines = name.split("\n")
    fixed_lines: list[str] = []
    for line in lines:
        stripped_line = line.rstrip()
        if not stripped_line:
            continue
        if fixed_lines:
            prev_line = fixed_lines[-1]
            first_char = stripped_line[0]
            is_lowercase_fragment = first_char.islower()
            if (
                len(prev_line) >= max_line_length
                and len(stripped_line) <= 3
                and prev_line[-1] not in " -"
                and is_lowercase_fragment
            ):
                fixed_lines[-1] += stripped_line
                continue
        fixed_lines.append(stripped_line)
    return "\n".join(fixed_lines)


def normalize_words(words: str) -> str:
    """
    Normalize when header/words parsed as single chars: "K o d e" -> "Kode"
    """
    s = words.strip()
    if not s:
        return ""
    tokens = s.split()
    for token in tokens:
        if len(token) > 1 and token not in ("/", "-"):
            return s
    return "".join(tokens)


def is_fuzzy_match(value: str, target: str, threshold: float = 80.0) -> bool:
    """
    Check if value matches target using fuzzy string matching (Ratio).
    Useful for full string matching (e.g. "Kode" vs "Code").
    """
    if value == target:
        return True
    return fuzz.ratio(value.lower(), target.lower()) >= threshold


def is_fuzzy_contained(needle: str, haystack: str, threshold: float = 80.0) -> bool:
    """
    Check if needle is contained in haystack using fuzzy string matching.
    Useful for finding substrings (e.g. "Nama" in "Nama Provinsi").
    """
    n = needle.lower()
    h = haystack.lower()

    if n in h:
        return True

    # If needle is longer than haystack, it can't be "contained" in the traditional sense.
    # We fall back to full ratio comparison to allow for slight length differences (e.g. typos)
    # but prevent "kode pulau" matching "kode" just because "kode" is a substring of "kode pulau".
    if len(n) > len(h):
        return fuzz.ratio(n, h) >= threshold

    return fuzz.partial_ratio(n, h) >= threshold


@dataclass
class MatchCandidate:
    """Represents a fuzzy match candidate with its score."""

    value: str
    score: float
    key: str | None = None  # Optional identifier (e.g., area code)


def fuzzy_search_top_n(
    query: str,
    choices: list[str],
    *,
    n: int = 5,
    threshold: float = 60.0,
    keys: list[str] | None = None,
) -> list[MatchCandidate]:
    """
    Search for top N fuzzy matches from a list of choices.

    Uses rapidfuzz's process.extract for efficient batch matching.

    Args:
        query: The string to search for
        choices: List of strings to search within
        n: Maximum number of results to return (default: 5)
        threshold: Minimum score threshold (0-100) to include in results
        keys: Optional list of keys (e.g., codes) corresponding to each choice

    Returns:
        List of MatchCandidate objects sorted by score (highest first)
    """
    if not query or not choices:
        return []

    # Use rapidfuzz's process.extract for efficient matching
    results = process.extract(
        query.lower(),
        [c.lower() for c in choices],
        scorer=fuzz.ratio,
        limit=n,
        score_cutoff=threshold,
    )

    candidates: list[MatchCandidate] = []
    for _, score, idx in results:
        # Get original (non-lowercased) value
        original_value = choices[idx]
        key = keys[idx] if keys else None
        candidates.append(MatchCandidate(value=original_value, score=score, key=key))

    return candidates


def chunked(iterable: list[int], size: int) -> Iterator[list[int]]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def validate_page_range(page_range: str) -> bool:
    pattern = r"^(\d+(-\d+)?)(,(\d+(-\d+)?))*$"
    return bool(re.match(pattern, page_range))


def parse_page_range(page_range: str, total_pages: int) -> list[int]:
    pages: set[int] = set()
    for part in page_range.split(","):
        if "-" in part:
            start, end = map(int, part.split("-"))
            pages.update(range(start, end + 1))
        else:
            pages.add(int(part))
    return sorted(p for p in pages if 1 <= p <= total_pages)


def format_duration(duration: float) -> str:
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    if minutes:
        return f"{int(minutes)}m {int(seconds)}s"
    return f"{seconds:.2f}s"


# =========================
# Coordinate normalization
# =========================
# Public API:
#   format_coordinate(coordinate) -> str
# Output: 'DD°MM\'SS.SS" N DDD°MM\'SS.SS" E'

# Map Indonesian hemisphere tokens to N/S/E/W
_HEMI_MAP = {
    "N": "N",
    "S": "S",
    "E": "E",
    "W": "W",
    "U": "N",
    "LU": "N",
    "T": "E",
    "BT": "E",
    "LS": "S",
    "B": "W",
    "BB": "W",
}
_HEMI_TOKEN_RE = re.compile(r"\b(LU|LS|BT|BB|[NSEWUTB])\b", re.IGNORECASE)


def _normalize_quotes(s: str) -> str:
    # Normalize smart quotes / primes to ASCII
    s = (
        s.replace("’", "'")
        .replace("‘", "'")
        .replace("′", "'")
        .replace("“", '"')
        .replace("”", '"')
        .replace("″", '"')
    )
    # Collapse duplicate quotes:  "" -> ",  '' -> '
    s = re.sub(r'"{2,}', '"', s)
    s = re.sub(r"'{2,}", "'", s)
    return s


def _normalize_spaces(s: str) -> str:
    """collapse any excessive spaces around tokens (keeps a single space between parts)"""
    return re.sub(r"\s+", " ", s).strip()


def _map_hemispheres(s: str) -> str:
    def repl(m: re.Match[str]) -> str:
        tok = m.group(1).upper()
        return _HEMI_MAP.get(tok, tok)

    return _HEMI_TOKEN_RE.sub(repl, s)


def _format_seconds_two_decimals(sec: str) -> str:
    # "3" -> "3.00", "3.4" -> "3.40", "3.444" -> "3.44"
    if "." in sec:
        whole, frac = sec.split(".", 1)
    else:
        whole, frac = sec, ""
    frac = (frac + "00")[:2]
    return f"{whole}.{frac}"


# One flexible pattern: optional leading hemi OR optional trailing hemi.
_COORD_RE = re.compile(
    r"""
    (?:(?P<h1>[NSEW])\s*)?                    # optional leading hemisphere
    (?P<deg>\d{1,3})\s*°\s*
    (?P<min>\d{1,2})\s*'\s*
    (?P<sec>\d{1,2}(?:\.\d+)?)\s*"?\s*        # seconds; optional double-quote in input
    (?P<h2>[NSEW])?                           # optional trailing hemisphere
    """,
    re.VERBOSE,
)


def format_coordinate(cell: str) -> str:
    """
    Canonicalize a coordinate string to:
      'DD°MM'SS.ss" N DD°MM'SS.ss" E'
    - Maps Indonesian hemispheres to N/S/E/W
    - Normalizes smart quotes and whitespace
    - Accepts hemisphere before or after the DMS block
    - Pads/truncates seconds to 2 decimals
    - Adds seconds quote if missing in input
    """
    if not cell or not cell.strip():
        return ""

    s = _normalize_spaces(_map_hemispheres(_normalize_quotes(cell)))

    lat: str | None = None
    lon: str | None = None

    for m in _COORD_RE.finditer(s):
        hemi = m.group("h1") or m.group("h2")
        if not hemi:
            continue
        deg, minutes, secs = m.group("deg"), m.group("min"), m.group("sec")
        secs = _format_seconds_two_decimals(secs)
        canonical = f"{deg}°{minutes}'{secs}\" {hemi}"

        if hemi in ("N", "S") and lat is None:
            lat = canonical
        elif hemi in ("E", "W") and lon is None:
            lon = canonical

    if lat and lon:
        return f"{lat} {lon}"

    # Fallback: return normalized text (hemispheres & quotes fixed, spaces collapsed)
    # This preserves 'abc' -> 'abc', and 'U T' -> 'N E'
    return s

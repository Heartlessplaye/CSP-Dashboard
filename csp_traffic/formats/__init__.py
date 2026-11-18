"""Parser registry with built-in JSON, NDJSON and CSV parsers."""

from .base import (
    ParseOutput,
    Parser,
    available_parsers,
    detect,
    get_parser,
    load_plugins,
    parser_mode,
    register,
)
from . import csv_parser, json_parser, ndjson_parser  # noqa: F401  (register built-ins)

__all__ = [
    "ParseOutput",
    "Parser",
    "available_parsers",
    "detect",
    "get_parser",
    "load_plugins",
    "parser_mode",
    "register",
]

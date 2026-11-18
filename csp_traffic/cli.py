"""Command line: `python -m csp_traffic run|once|validate-config`."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys

from .config import load_limits, load_settings, load_sources
from .formats import available_parsers, load_plugins
from .timefmt import ZoneFormatter, load_zone


def _setup_logging(zone_name: str = "UTC") -> None:
    """Log lines carry timestamps in the configured display timezone (with offset)."""
    handler = logging.StreamHandler()
    handler.setFormatter(ZoneFormatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", load_zone(zone_name)))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(os.environ.get("CSP_LOG_LEVEL", "INFO").upper())
    logging.getLogger("watchdog").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="csp-traffic", description="API traffic insights processor")
    parser.add_argument(
        "--settings",
        default=os.environ.get("CSP_SETTINGS", "config/settings.yaml"),
        help="settings YAML (default: $CSP_SETTINGS or config/settings.yaml)",
    )
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("run", help="watch the input directory and keep insights up to date (default)")
    sub.add_parser("once", help="process what is in the input directory now, write insights, exit")
    sub.add_parser("validate-config", help="check settings, sources.yaml and limits.json")
    args = parser.parse_args(argv)
    _setup_logging()
    settings = load_settings(args.settings)
    _setup_logging(settings.timezone)
    cmd = args.cmd or "run"

    if cmd == "validate-config":
        load_plugins(settings.plugins_dir)
        sources = load_sources(settings.sources_file)
        limits = load_limits(settings.limits_file)
        unknown = {n: s.parser for n, s in sources.sources.items() if s.parser and s.parser not in available_parsers()}
        if unknown:
            print(f"unknown parser(s): {unknown}; available: {available_parsers()}", file=sys.stderr)
            sys.exit(1)
        print(f"settings ok: {args.settings} (timezone {settings.timezone})")
        print(f"sources ok: {sorted(sources.sources)} (parsers available: {available_parsers()})")
        print(f"limits ok: default={limits.default.model_dump()} overrides={len(limits.clients)}")
        return

    from .engine import Engine

    engine = Engine(settings)
    if cmd == "once":
        engine.run_once()
        return

    def _graceful(signum, _frame):
        logging.getLogger(__name__).info("received %s", signal.Signals(signum).name)
        engine.stop()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)
    engine.run()


if __name__ == "__main__":
    main()

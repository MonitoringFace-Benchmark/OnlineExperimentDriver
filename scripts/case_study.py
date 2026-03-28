#!/usr/bin/env python3
"""Self-contained Wikimedia recentchange pipeline.

This script can either:
- stream live recentchange events from Wikimedia by default (no args), or
- read pre-fetched JSON lines from a file/stdin when arguments are provided,

then normalize each event to the compact JSON shape expected by the downstream
case-study processor:
    {"ts": <timestamp>, "user": <user>, "bot": <bool>, "type": <type>}

It mirrors the shell pipeline:
    curl -s https://stream.wikimedia.org/v2/stream/recentchange |
    grep data |
    sed 's/^data: //g' |
    jq -cM '{ts: .timestamp, user: .user, bot: .bot, type: .type}' |
    python3 case_study.py

By default (no arguments), the script fetches the live API stream itself and
prints processed records to stdout.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "Mozilla/5.0 (compatible; ExperimentDriver/1.0)"


@dataclass
class Config:
    log_path: Optional[str]
    dry_run: bool
    live: bool


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Process Wikimedia recentchange JSON lines")
    parser.add_argument("log", nargs="?", default=None, help="optional log file to append output to")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read stdin and print processed records without writing a log file",
        default=True
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="force live Wikimedia stream mode even if stdin is not a TTY",
    )
    ns = parser.parse_args()
    live = ns.live or (ns.log is None and sys.stdin.isatty())
    return Config(log_path=ns.log, dry_run=ns.dry_run, live=live)


def extract_event(line: str) -> Optional[dict]:
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None
    return {
        "ts": data.get("ts", data.get("timestamp")),
        "user": data.get("user"),
        "bot": data.get("bot"),
        "type": data.get("type"),
    }


def format_record(event: dict) -> str:
    e = event.get("type")
    ts = event.get("ts")
    u = event.get("user")
    b = event.get("bot")
    return f"{e}, tp={ts}, ts={ts}, user=\"{u}\", bot={b}"


def iter_live_stream_lines() -> Iterator[str]:
    request = urllib.request.Request(STREAM_URL, headers={"User-Agent": USER_AGENT, "Accept": "text/event-stream"})
    try:
        with urllib.request.urlopen(request) as response:
            for raw in response:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line.startswith("data: "):
                    continue
                payload = line.removeprefix("data: ")
                event = extract_event(payload)
                if event is None:
                    continue
                yield json.dumps(event, separators=(",", ":"))
    except urllib.error.HTTPError as exc:
        print(f"Fatal: live stream request failed: {exc}", file=sys.stderr)
        return
    except urllib.error.URLError as exc:
        print(f"Fatal: live stream request failed: {exc}", file=sys.stderr)
        return


def process_lines(lines: Iterable[str], cfg: Config) -> int:
    writer = None
    if cfg.log_path and not cfg.dry_run:
        writer = open(cfg.log_path, "w", encoding="utf-8")

    try:
        for raw in lines:
            event = extract_event(raw)
            if event is None:
                continue

            ts = event.get("ts")
            if ts is None:
                continue

            try:
                int(ts)
            except (TypeError, ValueError):
                continue

            res = format_record(event)
            print(res)
            if writer:
                print(res, file=writer)
    finally:
        if writer:
            writer.close()
    return 0


def main() -> int:
    cfg = parse_args()
    return process_lines(iter_live_stream_lines(), cfg)


if __name__ == "__main__":
    raise SystemExit(main())


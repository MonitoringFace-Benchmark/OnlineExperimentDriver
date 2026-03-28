#!/usr/bin/env python3
"""
Emit a fixed sequence of lines repeatedly.

Usage:
  python3 scripts/emit_lines.py           # infinite loop printing to stdout
  python3 scripts/emit_lines.py --count 1 # print the sequence once and exit
  python3 scripts/emit_lines.py --delay 0.2 --count 5
  python3/scripts/emit_lines.py --out output.txt --count 10  # append to file

By default the script prints to stdout. Use --out to append to a file instead.
"""
import time
import sys
import argparse

LINES = [
    "C, tp=0, ts=0",
    "B, tp=0, ts=0, x1=4, x2=10",
    "A, tp=1, ts=1, x1=1",
    "A, tp=1, ts=1, x1=3",
    "A, tp=2, ts=2, x0=1",
    "A, tp=2, ts=2, x0=2",
    "A, tp=2, ts=2, x0=3",
    "B, tp=2, ts=2, x1=4, x2=10",
    "A, tp=3, ts=3, x1=999",
]

def main():
    p = argparse.ArgumentParser(description="Emit a fixed sequence of lines repeatedly")
    p.add_argument("--delay", type=float, default=0.0, help="seconds to sleep between lines (default 0)")
    p.add_argument("--count", type=int, default=0, help="how many full passes to make (0 = infinite)")
    p.add_argument("--out", type=str, default=None, help="append output to this file instead of stdout")
    args = p.parse_args()

    writer = None
    if args.out:
        writer = open(args.out, "a")

    def emit_line(line):
        if writer:
            writer.write(line + "\n")
            writer.flush()
        else:
            print(line, flush=True)

    passes = 0
    try:
        if args.count <= 0:
            # infinite
            while True:
                for line in LINES:
                    emit_line(line)
                    if args.delay:
                        time.sleep(args.delay)
        else:
            while passes < args.count:
                for line in LINES:
                    emit_line(line)
                    if args.delay:
                        time.sleep(args.delay)
                passes += 1
    except KeyboardInterrupt:
        # graceful exit on Ctrl-C
        pass
    finally:
        if writer:
            writer.close()

if __name__ == '__main__':
    main()


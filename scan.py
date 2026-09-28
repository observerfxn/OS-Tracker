"""Refresh the public snapshot in CI; never run this inside Vercel Functions."""

import argparse
import json
from pathlib import Path

import scanner


PUBLIC = Path(__file__).parent / "public"
scanner.CACHE_FILE = str(PUBLIC / "scan_cache.json")
scanner.HISTORY_FILE = str(PUBLIC / "scan_history.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("quick", "deep"), default="quick")
    args = parser.parse_args()

    with open(scanner.CACHE_FILE, encoding="utf-8") as file:
        scanner.scan_cache = json.load(file)
    previous_count = len(scanner.scan_cache.get("collections", []))

    result = scanner.run_deep_scan(args.mode)
    count = len(result.get("collections", []))
    if count < max(20, previous_count // 4):
        raise SystemExit(
            f"Scan returned only {count} collections; keeping the last published data."
        )
    if not result.get("last_updated"):
        raise SystemExit("Scan did not produce a timestamp.")
    print(f"Ready to publish {count} collections.")


if __name__ == "__main__":
    main()

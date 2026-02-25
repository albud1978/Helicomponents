#!/usr/bin/env python3
import argparse
from pathlib import Path


OLD_LINE = "const { ZSTDDecompress } = require('simple-zstd');"
NEW_BLOCK = """let ZSTDDecompress;
try {
  ({ ZSTDDecompress } = require('simple-zstd'));
} catch (err) {
  ZSTDDecompress = null;
}
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Patch Superset webpack.proxy-config.js to make zstd optional")
    parser.add_argument("--superset-src", required=True, help="Path to apache/superset source root")
    args = parser.parse_args()

    target = Path(args.superset_src) / "superset-frontend/webpack.proxy-config.js"
    if not target.exists():
        raise FileNotFoundError(f"File not found: {target}")

    text = target.read_text(encoding="utf-8")
    if NEW_BLOCK in text:
        print(f"Already patched: {target}")
        return 0

    if OLD_LINE not in text:
        raise RuntimeError("Expected simple-zstd require line not found")

    target.write_text(text.replace(OLD_LINE, NEW_BLOCK), encoding="utf-8")
    print(f"Patched zstd optional load: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

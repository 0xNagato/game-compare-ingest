#!/usr/bin/env python3
"""Convert a GiantBomb-style JSON dump into newline-delimited JSON."""

import json
import sys
from pathlib import Path
from typing import Iterable, Union

JsonType = Union[dict, list]


def iter_records(payload: JsonType) -> Iterable[dict]:
    """Yield individual game records from object-or-array payloads."""
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, dict):
                yield value
            else:
                raise ValueError("Object payload must contain dict values")
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
            else:
                raise ValueError("Array payload must contain dict items")
    else:
        raise TypeError("Expected top-level JSON object or array")


def main() -> int:
    match sys.argv[1:]:
        case [src]:
            src_path = Path(src)
            dst_path = src_path.with_suffix(".ndjson")
        case [src, dst]:
            src_path = Path(src)
            dst_path = Path(dst)
        case _:
            print(
                "Usage: json_to_ndjson.py <source.json> [dest.ndjson]",
                file=sys.stderr,
            )
            return 1

    if not src_path.exists():
        print(f"Source file not found: {src_path}", file=sys.stderr)
        return 1

    payload = json.loads(src_path.read_text())

    with dst_path.open("w", encoding="utf-8") as fh:
        for record in iter_records(payload):
            json.dump(record, fh, ensure_ascii=False)
            fh.write("\n")

    print(f"Wrote {dst_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

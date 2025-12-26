#!/usr/bin/env python3
import sys
import json
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        print("usage: convert_to_ndjson.py <input.json> <output.ndjson>", file=sys.stderr)
        sys.exit(2)
    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])

    with src.open('r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, dict):
        # Prefer a primary array field if present (e.g., {"games": [ ... ]})
        if "games" in data and isinstance(data["games"], list):
            items = data["games"]
        else:
            # If exactly one list-valued field exists, treat it as the items
            list_fields = [v for v in data.values() if isinstance(v, list)]
            if len(list_fields) == 1:
                items = list_fields[0]
            else:
                # Fallback: emit each value (may not be what you want)
                items = list(data.values())
    elif isinstance(data, list):
        items = data
    else:
        print("unsupported top-level JSON type", file=sys.stderr)
        sys.exit(3)

    with dst.open('w', encoding='utf-8') as out:
        for obj in items:
            json.dump(obj, out, ensure_ascii=False, separators=(",", ":"))
            out.write("\n")

    print(f"wrote {dst} with {len(items)} lines")

if __name__ == "__main__":
    main()

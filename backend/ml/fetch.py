"""Build-time dependency fetcher; no dependency resolution at runtime."""

import hashlib
import json
import urllib.request
from pathlib import Path

root = Path(__file__).resolve().parent
for item in json.loads((root / "manifest.json").read_text())["artifacts"]:
    data = urllib.request.urlopen(item["url"], timeout=60).read()
    if hashlib.sha256(data).hexdigest() != item["sha256"]:
        raise RuntimeError(f"Dependency checksum mismatch: {item['filename']}")
    (root / item["filename"]).write_bytes(data)

import json
import re
import os
from pathlib import Path
from typing import Dict, Any

GENRE_JSON = "genres.json"
SONGS_DIR = Path("genre_songs")

SLUG_RE = re.compile(r"engenremap-([^.]+)\.html")


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\- ]+", "_", name).strip().replace(" ", "_")


def main():
    if not SONGS_DIR.exists():
        print(f"Directory {SONGS_DIR} does not exist. Run genre_songs_scrape first.")
        return

    with open(GENRE_JSON, "r", encoding="utf-8") as fp:
        genres: Dict[str, Dict[str, Any]] = json.load(fp)

    renamed = 0
    skipped = 0
    for genre, meta in genres.items():
        href = meta.get("href")
        m = SLUG_RE.search(href or "")
        if not m:
            skipped += 1
            continue
        slug = m.group(1)
        old_name = sanitize_filename(genre) + ".json"
        new_name = slug + ".json"
        old_path = SONGS_DIR / old_name
        new_path = SONGS_DIR / new_name
        # If new already exists, remove old duplicate if present
        if old_path.exists():
            if new_path.exists():
                # Duplicate, remove old
                old_path.unlink()
                continue
            old_path.rename(new_path)
            renamed += 1
    print(f"Renamed {renamed} files. Skipped {skipped}.")


if __name__ == "__main__":
    main() 
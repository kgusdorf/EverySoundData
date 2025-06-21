import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests
from bs4 import BeautifulSoup, NavigableString

BASE_URL = "https://everynoise.com/"
DEFAULT_GENRE_JSON = "genres.json"
OUTPUT_DIR = Path("genre_songs")
OUTPUT_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)

TITLE_RE = re.compile(r"^(.*?)\s*\"(.*?)\"$")


def parse_song_title(title_attr: str) -> str:
    """Convert 'e.g. Artist "Song"' into 'Artist - Song'"""
    if not title_attr:
        return ""
    title_attr = title_attr.strip()
    if title_attr.lower().startswith("e.g."):
        title_attr = title_attr[3:].lstrip(" .")
    m = TITLE_RE.match(title_attr)
    if m:
        artist, song = m.groups()
        return f"{artist.strip()} - {song.strip()}"
    return title_attr


def extract_songs_from_html(html_text: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    songs: List[Dict[str, Any]] = []
    for div in soup.select("div.genre.scanme"):
        preview_url = div.get("preview_url")
        if not preview_url:
            continue
        track_title = parse_song_title(div.get("title", ""))
        songs.append({
            "preview_url": preview_url,
            "preview_title": track_title,
        })
    return songs


def fetch_url(url: str, session: requests.Session, retries: int = 3, backoff: float = 1.0) -> Optional[str]:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            return r.text
        except Exception as exc:
            logger.warning("Error fetching %s: %s", url, exc)
            time.sleep(backoff * (2 ** attempt))
    return None


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\- ]+", "_", name).strip().replace(" ", "_")


def process_genres(genres_json: str, limit: Optional[int] = None):
    with open(genres_json, "r", encoding="utf-8") as fp:
        genre_data: Dict[str, Dict[str, Any]] = json.load(fp)

    session = requests.Session()
    processed = 0
    for genre, meta in genre_data.items():
        href = meta.get("href")
        if not href:
            logger.debug("Skipping %s (no href)", genre)
            continue
        url = href if href.startswith("http") else BASE_URL + href.lstrip("/")
        html_text = fetch_url(url, session)
        if html_text is None:
            logger.error("Failed to download %s", url)
            continue
        songs = extract_songs_from_html(html_text)

        # Determine filename from href pattern engenremap-<name>.html
        filename_slug = None
        m = re.search(r"engenremap-([^.]+)\.html", href or "")
        if m:
            filename_slug = m.group(1)
        else:
            filename_slug = sanitize_filename(genre)

        outfile = OUTPUT_DIR / f"{filename_slug}.json"
        with outfile.open("w", encoding="utf-8") as out_fp:
            json.dump(songs, out_fp, ensure_ascii=False, indent=2)
        logger.info("Saved %d songs for %s", len(songs), genre)
        processed += 1
        if limit and processed >= limit:
            break


def main():
    parser = argparse.ArgumentParser(description="Download song previews for each genre.")
    parser.add_argument("genres", nargs="?", default=DEFAULT_GENRE_JSON, help="Path to genres.json")
    parser.add_argument("--debug", action="store_true", help="Verbose logging")
    parser.add_argument("--limit", type=int, help="Process only first N genres (for testing)")
    args = parser.parse_args()

    logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.DEBUG if args.debug else logging.INFO)

    process_genres(args.genres, args.limit)


if __name__ == "__main__":
    main()

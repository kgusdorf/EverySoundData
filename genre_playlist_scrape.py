import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
except ImportError:
    raise SystemExit("Please install spotipy: pip install spotipy")

GENRE_JSON = "genres.json"
BASE_URL = "https://everynoise.com/"
PLAYLIST_DIR = Path("genre_songs_spotify")
PLAYLIST_DIR.mkdir(exist_ok=True)

# Where to store raw Spotify API responses when in --debug mode
DEBUG_DIR = Path("debug_api")

logger = logging.getLogger(__name__)

SLUG_RE = re.compile(r"engenremap-([^.]+)\.html")
PLAYLIST_RE = re.compile(r"open\.spotify\.com/playlist/([a-zA-Z0-9]+)")
TITLE_RE = re.compile(r"^(.*?) - (.*)$")


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\- ]+", "_", name).strip().replace(" ", "_")


def slug_from_href(href: str, genre_name: str) -> str:
    m = SLUG_RE.search(href)
    if m:
        return m.group(1)
    return sanitize_filename(genre_name)


def get_playlist_id_from_genre_page(html_text: str) -> Optional[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    a = soup.find("a", href=PLAYLIST_RE)
    if not a:
        return None
    m = PLAYLIST_RE.search(a["href"])
    if m:
        return m.group(1)
    return None


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


def create_spotify_client() -> spotipy.Spotify:
    # Load variables from .env (if present) so users don't have to export them manually
    load_dotenv(override=True)
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
    logger.debug("Using Spotify client_id: %s…", client_id[:8] if client_id else "<missing>")
    if not client_id or not client_secret:
        raise SystemExit("Set SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables.")
    auth_mgr = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(auth_manager=auth_mgr)


def build_preview_title(track: Dict[str, Any]) -> str:
    artists = ", ".join(a["name"] for a in track["artists"])
    return f"{artists} - {track['name']}"


def build_song_record(track: Dict[str, Any]) -> Dict[str, str]:
    """Return a dictionary with title, artist (comma-separated), and ISRC code."""
    return {
        "title": track.get("name", ""),
        "artist": ", ".join(a["name"] for a in track.get("artists", [])),
        "isrc": track.get("external_ids", {}).get("isrc", ""),
    }


def scrape_playlists(genres_json: str, limit: Optional[int] = None, debug: bool = False, workers: int = 4):
    with open(genres_json, "r", encoding="utf-8") as fp:
        genres: Dict[str, Dict[str, Any]] = json.load(fp)

    if debug:
        DEBUG_DIR.mkdir(exist_ok=True)

    # prepare list of genres to process respecting limit and skip existing
    tasks = []
    for genre_name, meta in genres.items():
        if limit is not None and len(tasks) >= limit:
            break
        href = meta.get("href")
        if not href:
            continue
        slug = slug_from_href(href, genre_name)
        outfile = PLAYLIST_DIR / f"{slug}.json"
        if outfile.exists():
            continue
        tasks.append((genre_name, href, slug))

    session = requests.Session()

    def worker(item):
        g_name, href_val, slug_val = item
        page_url = href_val if href_val.startswith("http") else BASE_URL + href_val.lstrip("/")
        html_text_local = fetch_url(page_url, session)
        if html_text_local is None:
            logger.error("Failed to download genre page %s", page_url)
            return
        playlist_id_local = get_playlist_id_from_genre_page(html_text_local)
        if not playlist_id_local:
            logger.info("No playlist found for %s", g_name)
            return

        # Create Spotify client inside thread for safety
        sp_local = create_spotify_client()
        try:
            playlist_local = sp_local.playlist_items(playlist_id_local, additional_types=("track",))
        except Exception as exc:
            logger.error("Failed to fetch playlist %s: %s", playlist_id_local, exc)
            return

        songs_local: List[Dict[str, str]] = []
        page_idx = 1
        pl_page = playlist_local
        while pl_page:
            for it in pl_page["items"]:
                tr = it.get("track")
                if not tr:
                    logger.debug("Skipped item without track object (maybe episode or unavailable)")
                    continue
                # Skip tracks that do not have an ISRC code and log the occurrence
                isrc_code = tr.get("external_ids", {}).get("isrc")
                if not isrc_code:
                    logger.warning(
                        "Skipping track without ISRC: %s - %s",
                        ", ".join(a["name"] for a in tr["artists"]),
                        tr.get("name", "<unknown>"),
                    )
                    continue
                songs_local.append(build_song_record(tr))
            if pl_page["next"]:
                if debug:
                    dbg_file = DEBUG_DIR / f"{slug_val}_page{page_idx}.json"
                    with dbg_file.open("w", encoding="utf-8") as df:
                        json.dump(pl_page, df, ensure_ascii=False, indent=2)
                    page_idx += 1
                pl_page = sp_local.next(pl_page)
            else:
                break

        if debug and songs_local:
            dbg_file = DEBUG_DIR / f"{slug_val}_page{page_idx}.json"
            if not dbg_file.exists():
                with dbg_file.open("w", encoding="utf-8") as df:
                    json.dump(pl_page, df, ensure_ascii=False, indent=2)

        if songs_local:
            out_file = PLAYLIST_DIR / f"{slug_val}.json"
            with out_file.open("w", encoding="utf-8") as of:
                json.dump(songs_local, of, ensure_ascii=False, indent=2)
            logger.info("Saved %d songs for %s", len(songs_local), g_name)

    # Run in parallel
    if workers <= 1 or not tasks:
        for t in tasks:
            worker(t)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(worker, t): t for t in tasks}
            for fut in as_completed(futures):
                _ = fut.result()


def main():
    parser = argparse.ArgumentParser(description="Scrape 'Sound of' Spotify playlists for each genre.")
    parser.add_argument("--genres", default=GENRE_JSON, help="Path to genres.json")
    parser.add_argument("--debug", action="store_true", help="Verbose logging")
    parser.add_argument("--limit", type=int, help="Process only first N genres (for testing)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default 4, 1=sequential)")
    args = parser.parse_args()

    logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.DEBUG if args.debug else logging.INFO)

    scrape_playlists(args.genres, args.limit, debug=args.debug, workers=args.workers)


if __name__ == "__main__":
    main() 
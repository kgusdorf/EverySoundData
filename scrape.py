import argparse
import logging
import json
import os
import re
import html
from typing import Dict, Any, Optional

try:
    from bs4 import BeautifulSoup, NavigableString
except ImportError as exc:
    logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.ERROR)
    logging.error("BeautifulSoup4 is required. Install it via `pip install beautifulsoup4`. (%s)", exc)
    raise SystemExit(1)

STYLE_COLOR_RE = re.compile(r"(?:background-)?color:\s*(#[0-9a-fA-F]{6})", re.IGNORECASE)
STYLE_POS_RE = re.compile(r"(top|left):\s*([0-9]+)px", re.IGNORECASE)

logger = logging.getLogger(__name__)

def _configure_logging(verbose: bool = False) -> None:
    """Configure global logging once."""
    if logger.hasHandlers():
        return  # already configured
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="[%(levelname)s] %(message)s", level=level)

def _parse_style(style: str) -> Dict[str, Any]:
    """Extract color, top and left from a style attribute string."""
    result: Dict[str, Any] = {}

    # Color: prefer background-color, fall back to color
    m_color = STYLE_COLOR_RE.search(style)
    if m_color:
        result["color"] = m_color.group(1).lower()

    # Position values
    for prop, value in STYLE_POS_RE.findall(style):
        result[prop] = int(value)

    return result


def _extract_genre_name(div) -> str:
    """Return the visible genre name text (excluding the » anchor)."""
    # The first NavigableString child of the div is the genre text.
    for child in div.children:
        if isinstance(child, NavigableString):
            name = child.strip()
            if name:
                return name
    # Fallback: use stripped text and remove trailing » if present
    text = div.get_text(strip=True)
    return text.rstrip("» ")


def _extract_preview_title(div) -> Optional[str]:
    """Derive the preview title from the title attribute if it starts with 'e.g.'"""
    title_attr = div.get("title")
    if not title_attr:
        return None
    title_attr = title_attr.strip()
    if title_attr.lower().startswith("e.g."):
        # Remove leading 'e.g.'
        title_attr = title_attr[3:]

    # Strip any leading dots/spaces that remain
    title_attr = title_attr.lstrip(" .")

    # Convert the common pattern 'Artist "Song"' to 'Artist - Song'
    m = re.match(r"^(.*?)\s*\"(.*?)\"$", title_attr)
    if m:
        artist, track = m.groups()
        title_attr = f"{artist.strip()} - {track.strip()}"

    return title_attr.strip()


def parse_file(html_path: str) -> Dict[str, Any]:
    """Parse the Everynoise HTML snapshot and return genre metadata mapping."""
    if not os.path.isfile(html_path):
        raise FileNotFoundError(f"Input file not found: {html_path}")

    with open(html_path, "r", encoding="utf-8") as fh:
        raw = fh.read()

    # The view-source snapshot is usually wrapped in a <table> where every
    # original source line is in <td class="line-content">. We first parse
    # that structure and recover the original lines.
    vs_soup = BeautifulSoup(raw, "html.parser")

    td_nodes = vs_soup.select("td.line-content")
    if td_nodes:
        # Extract text from each td (BeautifulSoup already unescapes &lt; etc.)
        original_lines = [td.get_text("", strip=False) for td in td_nodes]
        logger.debug("Detected view-source table with %d lines", len(original_lines))
        html_text = "\n".join(original_lines)
    else:
        # Not in view-source format, assume raw page
        html_text = raw

    soup = BeautifulSoup(html_text, "html.parser")

    genres: Dict[str, Any] = {}

    # The genre bubbles are divs with both 'genre' and 'scanme' classes.
    for div in soup.select("div.genre.scanme"):
        try:
            name = _extract_genre_name(div)
            if not name:
                logger.debug("Skipping unnamed genre div: %s", str(div)[:100])
                continue

            data: Dict[str, Any] = {}

            # Preview URL
            # preview_url = div.get("preview_url")
            # if preview_url:
            #     data["preview_url"] = preview_url

            # Genre page href (the «» link inside the div)
            nav = div.find("a", href=True)
            if nav is not None:
                data["href"] = nav["href"].strip()

            # Style parsing for color, top, left
            style = div.get("style", "")
            data.update(_parse_style(style))

            # Preview title
            # preview_title = _extract_preview_title(div)
            # if preview_title:
            #     data["preview_title"] = preview_title

            genres[name] = data
        except Exception as exc:
            logger.exception("Error parsing genre div: %s", exc)
            continue

    return genres


def main():
    parser = argparse.ArgumentParser(
        description="Parse an EveryNoise HTML snapshot and output genre metadata as JSON."
    )
    parser.add_argument(
        "html",
        nargs="?",
        default="view-source_https___everynoise.com.html",
        help="Path to the EveryNoise HTML file (default: view-source_https___everynoise.com.html)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="genres.json",
        help="Path to write the resulting JSON (default: genres.json)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging",
    )
    args = parser.parse_args()

    _configure_logging(verbose=args.debug)

    try:
        genres = parse_file(args.html)
    except Exception as exc:
        logger.exception("Fatal error while parsing file: %s", exc)
        raise SystemExit(1)

    with open(args.output, "w", encoding="utf-8") as out_fp:
        json.dump(genres, out_fp, ensure_ascii=False, indent=2)

    print(f"Wrote {len(genres)} genres to {args.output}")


if __name__ == "__main__":
    main()

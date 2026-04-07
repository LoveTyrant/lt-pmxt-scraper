import argparse
import json
import re
import sys
import time
from pathlib import Path

from scrapling.fetchers import Fetcher

BASE_URL = "https://archive.pmxt.dev/Polymarket"
PROJECT_DIR = Path(__file__).parent
MANIFEST_FILE = PROJECT_DIR / "downloaded.json"


def load_config() -> dict:
    config_path = PROJECT_DIR / "config.json"
    with open(config_path, "r") as f:
        return json.load(f)


def get_download_dir() -> Path:
    config = load_config()
    download_dir = Path(config["download_dir"])
    if not download_dir.is_absolute():
        download_dir = PROJECT_DIR / download_dir
    return download_dir


def load_manifest() -> set[str]:
    """Load the set of already-downloaded filenames."""
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_manifest(downloaded: set[str]) -> None:
    """Persist the set of downloaded filenames."""
    with open(MANIFEST_FILE, "w") as f:
        json.dump(sorted(downloaded), f, indent=2)


def get_total_pages(page) -> int:
    """Extract total page count from 'Page X of Y' text."""
    page_info = page.css(".page-info")
    if page_info:
        text = page_info[0].text
        parts = text.strip().split()
        for i, part in enumerate(parts):
            if part == "of" and i + 1 < len(parts):
                return int(parts[i + 1])
    return 1


def parse_size_to_bytes(size_str: str) -> float:
    """Convert a human-readable size string like '255.2 MB' or '1.2 GB' to bytes."""
    size_str = size_str.strip()
    match = re.match(r"([\d.]+)\s*(B|KB|MB|GB|TB)", size_str, re.IGNORECASE)
    if not match:
        return 0
    value = float(match.group(1))
    unit = match.group(2).upper()
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return value * multipliers.get(unit, 1)


def format_size(size_bytes: float) -> str:
    """Format bytes into a human-readable string."""
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.1f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes:.0f} B"


def scrape_parquet_links(page) -> list[tuple[str, str, float]]:
    """Extract (url, filename, size_bytes) tuples for .parquet files from a page.

    The page structure has entries like:
      <span>1. <a href="...">filename.parquet   </a>Mon, 06 Apr 2026 23:00 UTC    748.9 MB\n</span>
    """
    links = []
    pre = page.css("pre")
    raw_text = pre[0].html_content if pre else page.html_content

    for a in page.css("a"):
        href = a.attrib.get("href", "")
        if not href.endswith(".parquet"):
            continue
        filename = href.split("/")[-1]

        # Size appears after the </a> tag on the same line: ...   748.9 MB
        size_bytes = 0.0
        pattern = re.escape(filename) + r"\s*</a>.*?(\d+[\d.]*\s*(?:B|KB|MB|GB|TB))\s"
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            size_bytes = parse_size_to_bytes(match.group(1))

        links.append((href, filename, size_bytes))
    return links


def download_file(url: str, dest: Path) -> None:
    """Download a file with progress indication using streaming."""
    import urllib.request

    tmp = dest.with_suffix(".tmp")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            with open(tmp, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)  # 1 MB chunks
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        mb_down = downloaded / (1024 * 1024)
                        mb_total = total / (1024 * 1024)
                        print(
                            f"\r  {mb_down:.1f}/{mb_total:.1f} MB ({pct:.1f}%)",
                            end="",
                            flush=True,
                        )
            print()
        tmp.rename(dest)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def main():
    parser = argparse.ArgumentParser(description="Scrape and download Polymarket parquet files")
    parser.add_argument("--auto", action="store_true", help="Skip confirmation and start downloading immediately")
    args = parser.parse_args()

    download_dir = get_download_dir()
    download_dir.mkdir(parents=True, exist_ok=True)
    downloaded = load_manifest()

    print(f"Already downloaded: {len(downloaded)} files")
    print(f"Fetching page 1 to determine total pages...")

    page = Fetcher.get(f"{BASE_URL}?page=1")
    total_pages = get_total_pages(page)
    print(f"Total pages: {total_pages}")

    # Collect all parquet links across all pages
    all_links: list[tuple[str, str, float]] = []

    # Process page 1 (already fetched)
    all_links.extend(scrape_parquet_links(page))

    for page_num in range(2, total_pages + 1):
        print(f"Scanning page {page_num}/{total_pages}...")
        page = Fetcher.get(f"{BASE_URL}?page={page_num}")
        all_links.extend(scrape_parquet_links(page))
        time.sleep(0.5)

    total_files = len(all_links)
    total_size = sum(size for _, _, size in all_links)
    print(f"\nFound {total_files} total parquet files ({format_size(total_size)})")

    # Filter to only new files
    new_links = [(url, name, size) for url, name, size in all_links if name not in downloaded]
    new_size = sum(size for _, _, size in new_links)

    print(f"New files to download: {len(new_links)} ({format_size(new_size)})")

    if not new_links:
        print("Everything is up to date!")
        return

    # Show download summary before starting
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"  Total files on site:    {total_files} ({format_size(total_size)})")
    print(f"  Already downloaded:     {len(downloaded)}")
    print(f"  Files to download:      {len(new_links)}")
    print(f"  Data to download:       {format_size(new_size)}")
    print("=" * 60)

    # Prompt user to confirm (unless --auto)
    if not args.auto:
        try:
            answer = input("\nProceed with download? [y/N]: ").strip().lower()
            if answer not in ("y", "yes"):
                print("Download cancelled.")
                return
        except (EOFError, KeyboardInterrupt):
            print("\nDownload cancelled.")
            return

    # Download new files
    downloaded_count = 0
    for i, (url, filename, size) in enumerate(new_links, 1):
        dest = download_dir / filename
        size_str = f" ({format_size(size)})" if size else ""
        print(f"\n[{i}/{len(new_links)}] Downloading {filename}{size_str}")
        try:
            download_file(url, dest)
            downloaded.add(filename)
            save_manifest(downloaded)
            downloaded_count += 1
            print(f"  Saved to {dest}")
        except Exception as e:
            print(f"  ERROR downloading {filename}: {e}", file=sys.stderr)
            continue

    print(f"\nDone! Downloaded {downloaded_count} new files.")
    print(f"Total files tracked: {len(downloaded)}")


if __name__ == "__main__":
    main()

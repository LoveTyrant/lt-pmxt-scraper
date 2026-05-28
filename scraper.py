import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from scrapling.fetchers import Fetcher

ARCHIVE_ROOT = "https://archive.pmxt.dev"
PROJECT_DIR = Path(__file__).parent


@dataclass(frozen=True)
class Source:
    """A single downloadable dataset on the archive.

    ``listing_url`` is the paginated HTML index that is scraped for links.
    ``download_url`` is the R2 host the files actually live on; it is used to
    reconstruct file URLs during reverify (the listing host does not return a
    Content-Length, the R2 host does). ``manifest`` is the per-source manifest
    filename.

    Two subdirectories shape where files land (see ``get_download_dir``):
    ``platform_subdir`` keeps platforms apart when they share a single
    ``download_dir`` (it is skipped when the platform has its own override), and
    ``version_subdir`` keeps Polymarket v1/v2 apart and always applies.
    """

    key: str
    platform: str
    listing_url: str
    download_url: str
    platform_subdir: str
    version_subdir: str
    manifest: str


# Polymarket is versioned (v1/v2); the other platforms publish a single flat dataset.
# v1 keeps the legacy root layout and ``downloaded.json`` manifest for backward compatibility.
SOURCES: dict[str, Source] = {
    "polymarket-v1": Source(
        key="polymarket-v1",
        platform="polymarket",
        listing_url=f"{ARCHIVE_ROOT}/Polymarket/v1",
        download_url="https://r2.pmxt.dev",
        platform_subdir="",
        version_subdir="",
        manifest="downloaded.json",
    ),
    "polymarket-v2": Source(
        key="polymarket-v2",
        platform="polymarket",
        listing_url=f"{ARCHIVE_ROOT}/Polymarket/v2",
        download_url="https://r2v2.pmxt.dev",
        platform_subdir="",
        version_subdir="v2",
        manifest="downloaded_v2.json",
    ),
    "kalshi": Source(
        key="kalshi",
        platform="kalshi",
        listing_url=f"{ARCHIVE_ROOT}/Kalshi",
        download_url="https://r2kalshi.pmxt.dev",
        platform_subdir="kalshi",
        version_subdir="",
        manifest="downloaded_kalshi.json",
    ),
    "limitless": Source(
        key="limitless",
        platform="limitless",
        listing_url=f"{ARCHIVE_ROOT}/Limitless",
        download_url="https://r2limitless.pmxt.dev",
        platform_subdir="limitless",
        version_subdir="",
        manifest="downloaded_limitless.json",
    ),
    "opinion": Source(
        key="opinion",
        platform="opinion",
        listing_url=f"{ARCHIVE_ROOT}/Opinion",
        download_url="https://r2opinion.pmxt.dev",
        platform_subdir="opinion",
        version_subdir="",
        manifest="downloaded_opinion.json",
    ),
}

PLATFORMS = ("polymarket", "kalshi", "limitless", "opinion")
PLATFORM_CHOICES = (*PLATFORMS, "all")
SUPPORTED_VERSIONS = ("v1", "v2")
DEFAULT_VERSION = "v2"
DEFAULT_PLATFORMS = ("polymarket",)


def resolve_sources(platforms: list[str], version: str) -> list[Source]:
    """Map selected platforms (and the Polymarket version) to Source objects.

    Expands ``all``, applies ``version`` only to Polymarket, and de-duplicates
    while preserving the order the platforms were requested in.
    """
    expanded: list[str] = []
    for platform in platforms:
        if platform == "all":
            expanded.extend(PLATFORMS)
        else:
            expanded.append(platform)

    sources: list[Source] = []
    seen: set[str] = set()
    for platform in expanded:
        key = f"polymarket-{version}" if platform == "polymarket" else platform
        if key in seen:
            continue
        seen.add(key)
        sources.append(SOURCES[key])
    return sources


def load_config() -> dict:
    config_path = PROJECT_DIR / "config.json"
    with open(config_path, "r") as f:
        return json.load(f)


def _resolve_dir(value: str) -> Path:
    """Resolve a configured path, treating relative paths as project-relative."""
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_DIR / path
    return path


def get_download_dir(config: dict, source: Source) -> Path:
    """Resolve the download directory for a source.

    By default everything hangs off the single ``download_dir``, with each
    platform in its own subdirectory (Polymarket v1 stays at the root for the
    pre-v2 layout, v2 in a ``v2/`` subdirectory). The optional ``download_dirs``
    map overrides the base directory per platform (keyed by platform name:
    ``polymarket``, ``kalshi``, ``limitless``, ``opinion``); an override is used
    as-is without the platform subdirectory. The Polymarket v1/v2 split always
    applies, even under an override, so their filenames cannot collide.
    """
    overrides = config.get("download_dirs", {})
    if source.platform in overrides:
        base = _resolve_dir(overrides[source.platform])
    else:
        base = _resolve_dir(config["download_dir"])
        if source.platform_subdir:
            base = base / source.platform_subdir
    if source.version_subdir:
        base = base / source.version_subdir
    return base


def load_manifest(source: Source) -> dict[str, int]:
    """Load the manifest of downloaded files mapping filename to size in bytes.

    Handles the legacy format (list of filenames) by assigning size 0.
    """
    manifest_file = PROJECT_DIR / source.manifest
    if manifest_file.exists():
        with open(manifest_file, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {name: 0 for name in data}
        return data
    return {}


def save_manifest(downloaded: dict[str, int], source: Source) -> None:
    """Persist the manifest of downloaded files."""
    manifest_file = PROJECT_DIR / source.manifest
    with open(manifest_file, "w") as f:
        json.dump(dict(sorted(downloaded.items())), f, indent=2)


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


def download_file(url: str, dest: Path, label: str = "") -> int:
    """Download a file with progress indication using streaming. Returns expected size."""
    import urllib.request

    prefix = f"  [{label}]" if label else " "
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
                            f"\r{prefix} {mb_down:.1f}/{mb_total:.1f} MB ({pct:.1f}%)",
                            end="",
                            flush=True,
                        )
            print()
        tmp.rename(dest)
        return total
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def reverify_manifest(source: Source, download_dir: Path) -> None:
    """Fetch Content-Length via HEAD requests for all manifest entries and update sizes.

    Deletes local files that don't match the expected size.
    """
    import urllib.request

    downloaded = load_manifest(source)
    if not downloaded:
        print("Manifest is empty, nothing to reverify.")
        return

    print(f"Reverifying {len(downloaded)} files against server ({source.key})...")
    bad_files = []
    updated = 0

    for i, filename in enumerate(list(downloaded), 1):
        url = f"{source.download_url}/{filename}"
        print(f"\r  [{i}/{len(downloaded)}] Checking {filename}...", end="", flush=True)
        try:
            req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req) as resp:
                expected_size = int(resp.headers.get("Content-Length", 0))
        except Exception as e:
            print(f"\n  WARNING: Could not fetch size for {filename}: {e}")
            continue

        if expected_size == 0:
            continue

        if downloaded[filename] != expected_size:
            downloaded[filename] = expected_size
            updated += 1

        local_path = download_dir / filename
        if local_path.exists() and local_path.stat().st_size != expected_size:
            bad_files.append((filename, f"expected {format_size(expected_size)}, got {format_size(local_path.stat().st_size)}"))
            local_path.unlink()
            del downloaded[filename]

    print()
    if updated:
        print(f"Updated {updated} manifest entries with correct sizes.")
    if bad_files:
        print(f"Deleted {len(bad_files)} bad file(s):")
        for filename, reason in bad_files:
            print(f"  - {filename} ({reason})")
    if not updated and not bad_files:
        print("All files verified OK.")

    save_manifest(downloaded, source)


def main():
    parser = argparse.ArgumentParser(description="Scrape and download prediction-market parquet files from archive.pmxt.dev")
    parser.add_argument("--auto", action="store_true", help="Skip confirmation and start downloading immediately")
    parser.add_argument("--monitor", action="store_true", help="Keep running and periodically check for new files")
    parser.add_argument("--reverify", action="store_true", help="Fetch expected sizes from server via HEAD requests and update manifest")
    parser.add_argument(
        "--platform",
        nargs="+",
        choices=PLATFORM_CHOICES,
        default=list(DEFAULT_PLATFORMS),
        metavar="PLATFORM",
        help=(
            "Which platform(s) to download: "
            f"{', '.join(PLATFORM_CHOICES)} (default: {' '.join(DEFAULT_PLATFORMS)}). "
            "Pass several, or 'all' for every platform."
        ),
    )
    parser.add_argument(
        "--version",
        choices=SUPPORTED_VERSIONS,
        default=DEFAULT_VERSION,
        help=f"Polymarket archive dataset version (default: {DEFAULT_VERSION}); ignored for other platforms",
    )
    args = parser.parse_args()

    config = load_config()
    sources = resolve_sources(args.platform, args.version)
    max_workers = config["max_concurrent_downloads"]
    monitor_interval = config["monitor_interval_minutes"]

    print(f"Selected: {', '.join(s.key for s in sources)}")

    if args.reverify:
        for source in sources:
            download_dir = get_download_dir(config, source)
            print(f"\n=== {source.key} ===")
            reverify_manifest(source, download_dir)
        return

    if args.monitor:
        print(f"Monitor mode: checking every {monitor_interval} minutes (Ctrl+C to stop)")
        while True:
            for source in sources:
                download_dir = get_download_dir(config, source)
                download_dir.mkdir(parents=True, exist_ok=True)
                print(f"\n=== {source.key} ({source.listing_url}) ===")
                print(f"Download directory: {download_dir}")
                _run_once(source, download_dir, max_workers, auto=True)
            print(f"\nNext check in {monitor_interval} minutes...")
            try:
                time.sleep(monitor_interval * 60)
            except KeyboardInterrupt:
                print("\nMonitor stopped.")
                return
    else:
        for source in sources:
            download_dir = get_download_dir(config, source)
            download_dir.mkdir(parents=True, exist_ok=True)
            print(f"\n=== {source.key} ({source.listing_url}) ===")
            print(f"Download directory: {download_dir}")
            _run_once(source, download_dir, max_workers, auto=args.auto)


def verify_downloads(downloaded: dict[str, int], download_dir: Path, source: Source) -> tuple[dict[str, int], int]:
    """Verify local files match their recorded sizes. Remove mismatches."""
    bad_files = []
    updated = False
    for filename, expected_size in list(downloaded.items()):
        local_path = download_dir / filename
        if not local_path.exists():
            bad_files.append((filename, "missing"))
            continue
        if expected_size == 0:
            actual_size = local_path.stat().st_size
            if actual_size > 0:
                downloaded[filename] = actual_size
                updated = True
            continue
        actual_size = local_path.stat().st_size
        if actual_size != expected_size:
            bad_files.append((filename, f"expected {format_size(expected_size)}, got {format_size(actual_size)}"))
            local_path.unlink()

    if bad_files:
        print(f"Found {len(bad_files)} bad/missing file(s):")
        for filename, reason in bad_files:
            print(f"  - {filename} ({reason})")
        for filename, _ in bad_files:
            del downloaded[filename]
        updated = True

    if updated:
        save_manifest(downloaded, source)

    return downloaded, len(bad_files)


def _run_once(source: Source, download_dir: Path, max_workers: int, *, auto: bool) -> None:
    """Scan for new files and download them."""
    base_url = source.listing_url
    downloaded = load_manifest(source)
    downloaded, redownload_count = verify_downloads(downloaded, download_dir, source)

    print(f"Already downloaded: {len(downloaded)} files")
    print(f"Fetching page 1 to determine total pages...")

    page = Fetcher.get(f"{base_url}?page=1")
    total_pages = get_total_pages(page)
    print(f"Total pages: {total_pages}")

    # Collect all parquet links across all pages
    all_links: list[tuple[str, str, float]] = []

    # Process page 1 (already fetched)
    all_links.extend(scrape_parquet_links(page))

    for page_num in range(2, total_pages + 1):
        print(f"Scanning page {page_num}/{total_pages}...")
        page = Fetcher.get(f"{base_url}?page={page_num}")
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
    new_count = len(new_links) - redownload_count
    print(f"  Total files on site:    {total_files} ({format_size(total_size)})")
    print(f"  Already downloaded:     {len(downloaded)}")
    print(f"  New files:              {new_count}")
    print(f"  Redownloading (bad):    {redownload_count}")
    print(f"  Total to download:      {len(new_links)} ({format_size(new_size)})")
    print("=" * 60)

    # Prompt user to confirm (unless auto)
    if not auto:
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
    manifest_lock = threading.Lock()

    def _download_one(index: int, url: str, filename: str, size: float) -> bool:
        nonlocal downloaded_count
        dest = download_dir / filename
        size_str = f" ({format_size(size)})" if size else ""
        label = f"{index}/{len(new_links)}"
        print(f"\n[{label}] Downloading {filename}{size_str}")
        try:
            expected_size = download_file(url, dest, label=label)
            with manifest_lock:
                downloaded[filename] = expected_size or dest.stat().st_size
                save_manifest(downloaded, source)
                downloaded_count += 1
            print(f"  [{label}] Saved to {dest}")
            return True
        except Exception as e:
            print(f"  [{label}] ERROR downloading {filename}: {e}", file=sys.stderr)
            return False

    if max_workers > 1:
        print(f"\nDownloading with {max_workers} concurrent workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_download_one, i, url, name, size): name
            for i, (url, name, size) in enumerate(new_links, 1)
        }
        for future in as_completed(futures):
            future.result()  # propagate any unexpected exceptions

    print(f"\nDone! Downloaded {downloaded_count} new files.")
    print(f"Total files tracked: {len(downloaded)}")


if __name__ == "__main__":
    main()

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from scrapling.fetchers import Fetcher

ROOT_URL = "https://archive.pmxt.dev/Polymarket"
PROJECT_DIR = Path(__file__).parent
SUPPORTED_VERSIONS = ("v1", "v2")


def get_base_url(version: str) -> str:
    """Return the archive URL for the requested dataset version."""
    return ROOT_URL if version == "v1" else f"{ROOT_URL}/{version}"


def get_manifest_file(version: str) -> Path:
    """Return the manifest path for the requested dataset version.

    v1 uses the legacy ``downloaded.json`` filename for backward compatibility.
    """
    if version == "v1":
        return PROJECT_DIR / "downloaded.json"
    return PROJECT_DIR / f"downloaded_{version}.json"


def load_config() -> dict:
    config_path = PROJECT_DIR / "config.json"
    with open(config_path, "r") as f:
        return json.load(f)


def get_download_dir(config: dict, version: str) -> Path:
    """Resolve the download directory for the requested dataset version.

    v1 writes directly to the configured directory (preserving existing layouts);
    newer versions get a dedicated subdirectory so their files can't collide.
    """
    download_dir = Path(config["download_dir"])
    if not download_dir.is_absolute():
        download_dir = PROJECT_DIR / download_dir
    if version != "v1":
        download_dir = download_dir / version
    return download_dir


def load_manifest(version: str) -> dict[str, int]:
    """Load the manifest of downloaded files mapping filename to size in bytes.

    Handles the legacy format (list of filenames) by assigning size 0.
    """
    manifest_file = get_manifest_file(version)
    if manifest_file.exists():
        with open(manifest_file, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {name: 0 for name in data}
        return data
    return {}


def save_manifest(downloaded: dict[str, int], version: str) -> None:
    """Persist the manifest of downloaded files."""
    manifest_file = get_manifest_file(version)
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


def reverify_manifest(download_dir: Path, version: str) -> None:
    """Fetch Content-Length via HEAD requests for all manifest entries and update sizes.

    Deletes local files that don't match the expected size.
    """
    import urllib.request

    base_url = get_base_url(version)
    downloaded = load_manifest(version)
    if not downloaded:
        print("Manifest is empty, nothing to reverify.")
        return

    print(f"Reverifying {len(downloaded)} files against server ({version})...")
    bad_files = []
    updated = 0

    for i, filename in enumerate(list(downloaded), 1):
        url = f"{base_url}/{filename}"
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

    save_manifest(downloaded, version)


def main():
    parser = argparse.ArgumentParser(description="Scrape and download Polymarket parquet files")
    parser.add_argument("--auto", action="store_true", help="Skip confirmation and start downloading immediately")
    parser.add_argument("--monitor", action="store_true", help="Keep running and periodically check for new files")
    parser.add_argument("--reverify", action="store_true", help="Fetch expected sizes from server via HEAD requests and update manifest")
    parser.add_argument(
        "--version",
        choices=SUPPORTED_VERSIONS,
        default="v1",
        help="Which archive dataset version to download (default: v1)",
    )
    args = parser.parse_args()

    config = load_config()
    version = args.version
    download_dir = get_download_dir(config, version)
    download_dir.mkdir(parents=True, exist_ok=True)
    max_workers = config["max_concurrent_downloads"]
    monitor_interval = config["monitor_interval_minutes"]

    print(f"Archive version: {version} ({get_base_url(version)})")
    print(f"Download directory: {download_dir}")

    if args.reverify:
        reverify_manifest(download_dir, version)
        return

    if args.monitor:
        print(f"Monitor mode: checking every {monitor_interval} minutes (Ctrl+C to stop)")
        while True:
            _run_once(download_dir, max_workers, version, auto=True)
            print(f"\nNext check in {monitor_interval} minutes...")
            try:
                time.sleep(monitor_interval * 60)
            except KeyboardInterrupt:
                print("\nMonitor stopped.")
                return
    else:
        _run_once(download_dir, max_workers, version, auto=args.auto)


def verify_downloads(downloaded: dict[str, int], download_dir: Path, version: str) -> tuple[dict[str, int], int]:
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
        save_manifest(downloaded, version)

    return downloaded, len(bad_files)


def _run_once(download_dir: Path, max_workers: int, version: str, *, auto: bool) -> None:
    """Scan for new files and download them."""
    base_url = get_base_url(version)
    downloaded = load_manifest(version)
    downloaded, redownload_count = verify_downloads(downloaded, download_dir, version)

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
                save_manifest(downloaded, version)
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

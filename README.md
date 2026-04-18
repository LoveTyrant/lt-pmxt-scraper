# lt-pmxt-scraper

Scrapes and downloads Polymarket orderbook `.parquet` files from [archive.pmxt.dev](https://archive.pmxt.dev/Polymarket?page=1).

Supports both dataset versions published by the archive:

- **v1** — the original dump at [archive.pmxt.dev/Polymarket](https://archive.pmxt.dev/Polymarket?page=1)
- **v2** — the newer dump at [archive.pmxt.dev/Polymarket/v2](https://archive.pmxt.dev/Polymarket/v2?page=1)

Tracks previously downloaded files per version so subsequent runs only grab new ones.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
cd lt-pmxt-scraper
cp config.example.json config.json
uv sync
```

## Configuration

Edit `config.json` to configure the scraper. All settings are required:

```json
{
  "download_dir": "downloads",
  "max_concurrent_downloads": 1,
  "monitor_interval_minutes": 30
}
```

| Key | Default | Description |
|---|---|---|
| `download_dir` | `"downloads"` | Absolute or relative path (resolved from the project directory) |
| `max_concurrent_downloads` | `1` | Number of files to download simultaneously |
| `monitor_interval_minutes` | `30` | How often `--monitor` rechecks for new files (in minutes) |

## Usage

```bash
uv run python scraper.py
```

Use `--auto` to skip the confirmation prompt and start downloading immediately:

```bash
uv run python scraper.py --auto
```

Use `--monitor` to keep the scraper running and periodically check for new files:

```bash
uv run python scraper.py --monitor
```

Use `--reverify` to check all downloaded files against the server's expected file sizes. This sends a HEAD request for each file in the manifest and deletes any local files that don't match:

```bash
uv run python scraper.py --reverify
```

Use `--version` to pick which archive dataset to pull. Defaults to `v1`; pass `v2` for the newer dump:

```bash
uv run python scraper.py --version v2
```

See [Dataset versions](#dataset-versions) below for how v1 and v2 are kept separate on disk. `--version` combines with every other flag (`--auto`, `--monitor`, `--reverify`) — for example:

```bash
uv run python scraper.py --version v2 --monitor
uv run python scraper.py --version v2 --reverify
```

The scraper will:

1. Verify existing downloads for the selected version against expected file sizes (delete and requeue any mismatches)
2. Scan all pages on the archive site for that version
3. Collect every `.parquet` file link and its size
4. Compare against the version's manifest (`downloaded.json` for v1, `downloaded_v2.json` for v2) to find new files
5. Show a download summary with new files, redownloads, and total size
6. Ask for confirmation before downloading (unless `--auto` or `--monitor`)
7. Download files using the configured number of concurrent workers

## Dataset versions

The archive publishes two parallel datasets. The scraper treats them as independent so you can download either or both without mixing files:

| Version | Archive URL | Download location | Manifest |
|---|---|---|---|
| `v1` (default) | `https://archive.pmxt.dev/Polymarket` | `download_dir/` | `downloaded.json` |
| `v2` | `https://archive.pmxt.dev/Polymarket/v2` | `download_dir/v2/` | `downloaded_v2.json` |

Notes:

- v1 writes directly to `download_dir` (preserving the pre-v2 layout), while v2 is isolated in a `v2/` subdirectory so filenames cannot collide.
- Each version has its own manifest, its own integrity checks, and its own `--monitor` loop.
- To keep both versions current on the same machine, run the scraper twice — once per version (e.g. two `--monitor` processes, or two scheduled `--auto` runs).

## Download integrity

The manifest for the selected version (`downloaded.json` for v1, `downloaded_v2.json` for v2) tracks each file's expected size (from the server's `Content-Length` header). On every run, local files are checked against their expected sizes. If a file is missing or has a size mismatch (e.g. from an interrupted download), it is deleted and redownloaded automatically.

For existing manifest entries that predate this feature, run `--reverify` once per version to backfill the expected sizes from the server:

```bash
uv run python scraper.py --reverify              # v1
uv run python scraper.py --version v2 --reverify # v2
```

## Files

| File | Description |
|---|---|
| `scraper.py` | Main scraper script |
| `config.example.json` | Example configuration (copy to `config.json`) |
| `config.json` | Your local download directory configuration (git-ignored) |
| `downloaded.json` | v1 manifest of already-downloaded files (auto-generated, git-ignored) |
| `downloaded_v2.json` | v2 manifest of already-downloaded files (auto-generated, git-ignored) |
| `pyproject.toml` | Project metadata and dependencies |

## Disclaimer

This tool is provided as-is for personal and research use. It is not affiliated
with, endorsed by, or sponsored by Polymarket or archive.pmxt.dev.

Users are solely responsible for ensuring their use of this tool complies with
all applicable laws, regulations, and third-party terms of service. The authors
assume no responsibility or liability for any data loss, inaccuracies, service
disruptions, or other consequences arising from the use of this software.

Use at your own risk. See [LICENSE.txt](LICENSE.txt) for full terms.

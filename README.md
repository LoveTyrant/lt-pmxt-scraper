# lt-pmxt-scraper

Scrapes and downloads Polymarket orderbook `.parquet` files from [archive.pmxt.dev](https://archive.pmxt.dev/Polymarket?page=1).

Tracks previously downloaded files so subsequent runs only grab new ones.

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

The scraper will:

1. Scan all pages on the archive site
2. Collect every `.parquet` file link and its size
3. Compare against `downloaded.json` to find new files
4. Show a download summary with total size
5. Ask for confirmation before downloading (unless `--auto` or `--monitor`)
6. Download files using the configured number of concurrent workers

The manifest (`downloaded.json`) is saved after each successful download, so interrupted runs pick up where they left off.

## Files

| File | Description |
|---|---|
| `scraper.py` | Main scraper script |
| `config.example.json` | Example configuration (copy to `config.json`) |
| `config.json` | Your local download directory configuration (git-ignored) |
| `downloaded.json` | Manifest of already-downloaded files (auto-generated, git-ignored) |
| `pyproject.toml` | Project metadata and dependencies |

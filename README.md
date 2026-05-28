# lt-pmxt-scraper

Scrapes and downloads prediction-market orderbook `.parquet` files from [archive.pmxt.dev](https://archive.pmxt.dev/).

Supports every platform published by the archive:

- **Polymarket** — [archive.pmxt.dev/Polymarket](https://archive.pmxt.dev/Polymarket/v2?page=1), published in two dataset versions:
  - **v1** — the original dump at [/Polymarket/v1](https://archive.pmxt.dev/Polymarket/v1?page=1)
  - **v2** — the newer dump at [/Polymarket/v2](https://archive.pmxt.dev/Polymarket/v2?page=1)
- **Kalshi** — [archive.pmxt.dev/Kalshi](https://archive.pmxt.dev/Kalshi?page=1)
- **Limitless** — [archive.pmxt.dev/Limitless](https://archive.pmxt.dev/Limitless?page=1)
- **Opinion** — [archive.pmxt.dev/Opinion](https://archive.pmxt.dev/Opinion?page=1)

Pick a single platform, several, or all four. Each platform (and each Polymarket
version) is tracked independently so subsequent runs only grab new files.

## Setup

Requires Python 3.14+ and [uv](https://docs.astral.sh/uv/).

```bash
cd lt-pmxt-scraper
cp config.example.json config.json
uv sync
```

## Configuration

Edit `config.json` to configure the scraper:

```json
{
  "download_dir": "downloads",
  "max_concurrent_downloads": 1,
  "monitor_interval_minutes": 30
}
```

| Key | Required | Default | Description |
|---|---|---|---|
| `download_dir` | yes | `"downloads"` | Base directory for all platforms. Absolute or relative path (resolved from the project directory) |
| `download_dirs` | no | `{}` | Optional per-platform directory overrides (see below) |
| `max_concurrent_downloads` | yes | `1` | Number of files to download simultaneously |
| `monitor_interval_minutes` | yes | `30` | How often `--monitor` rechecks for new files (in minutes) |

### Per-platform download directories

By default every platform is stored under the single `download_dir` (each in its
own subdirectory — see [Platforms and versions](#platforms-and-versions)). To send
one or more platforms somewhere else entirely — e.g. a different drive — add a
`download_dirs` map keyed by platform name (`polymarket`, `kalshi`, `limitless`,
`opinion`):

```json
{
  "download_dir": "D:/pmxt",
  "download_dirs": {
    "kalshi": "K:/data/kalshi",
    "limitless": "L:/data/limitless"
  },
  "max_concurrent_downloads": 3,
  "monitor_interval_minutes": 30
}
```

- Only listed platforms are overridden; everything else falls back to `download_dir`. With the config above, Kalshi goes to `K:/data/kalshi`, Limitless to `L:/data/limitless`, and Polymarket/Opinion stay under `D:/pmxt`.
- An override is the platform's directory **as-is** — the per-platform subdirectory (e.g. `kalshi/`) is not appended on top of it.
- Overriding `polymarket` still keeps v1 and v2 apart: v1 goes to the override directory, v2 to a `v2/` subdirectory under it (so their identically-named files can't collide).

To keep every platform in its own folder under a shared base, override each one:

```json
{
  "download_dir": "D:/pmxt",
  "download_dirs": {
    "polymarket": "D:/pmxt/polymarket",
    "kalshi": "D:/pmxt/kalshi",
    "limitless": "D:/pmxt/limitless",
    "opinion": "D:/pmxt/opinion"
  },
  "max_concurrent_downloads": 3,
  "monitor_interval_minutes": 30
}
```

The `polymarket` entry matters here: without it, Polymarket v1 writes straight to
the `download_dir` root (alongside the other platforms' folders) rather than into
its own `polymarket/` folder.

## Usage

By default the scraper downloads Polymarket (v2):

```bash
uv run python scraper.py
```

Use `--platform` to choose which platform(s) to download. Pass one, several, or
`all`. Choices: `polymarket`, `kalshi`, `limitless`, `opinion`, `all`.

```bash
uv run python scraper.py --platform kalshi                 # just Kalshi
uv run python scraper.py --platform kalshi limitless       # two platforms
uv run python scraper.py --platform all                    # everything
```

`--platform` combines with every other flag. Each selected platform is scanned,
confirmed, and downloaded in turn (its own summary and prompt), so `--auto` is
handy when selecting more than one:

```bash
uv run python scraper.py --platform all --auto
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

Use `--version` to pick which **Polymarket** dataset to pull (it is ignored for the
other platforms). Defaults to `v2`; pass `v1` for the original dump:

```bash
uv run python scraper.py --version v1
```

See [Platforms and versions](#platforms-and-versions) below for how each dataset is kept separate on disk. `--version` combines with every other flag (`--platform`, `--auto`, `--monitor`, `--reverify`) — for example:

```bash
uv run python scraper.py --version v1 --monitor
uv run python scraper.py --platform polymarket --version v1 --reverify
```

For each selected platform the scraper will:

1. Verify existing downloads against their expected file sizes (delete and requeue any mismatches)
2. Scan all pages on the archive site for that platform
3. Collect every `.parquet` file link and its size
4. Compare against the platform's manifest to find new files
5. Show a download summary with new files, redownloads, and total size
6. Ask for confirmation before downloading (unless `--auto` or `--monitor`)
7. Download files using the configured number of concurrent workers

When multiple platforms are selected, steps 1–7 run for each one in turn.

## Platforms and versions

Each platform (and each Polymarket version) is an independent dataset: its own
download subdirectory under `download_dir`, its own manifest, and its own
integrity checks. This means nothing collides, and you can pull any combination
without mixing files.

| Platform | Archive URL | Download location | Manifest |
|---|---|---|---|
| Polymarket `v1` | `https://archive.pmxt.dev/Polymarket/v1` | `download_dir/` | `downloaded.json` |
| Polymarket `v2` (default) | `https://archive.pmxt.dev/Polymarket/v2` | `download_dir/v2/` | `downloaded_v2.json` |
| Kalshi | `https://archive.pmxt.dev/Kalshi` | `download_dir/kalshi/` | `downloaded_kalshi.json` |
| Limitless | `https://archive.pmxt.dev/Limitless` | `download_dir/limitless/` | `downloaded_limitless.json` |
| Opinion | `https://archive.pmxt.dev/Opinion` | `download_dir/opinion/` | `downloaded_opinion.json` |

The "Download location" column shows the default layout; any platform can be redirected elsewhere with [`download_dirs`](#per-platform-download-directories).

Notes:

- Polymarket v1 writes directly to `download_dir` (preserving the pre-v2 layout); every other dataset is isolated in its own subdirectory so filenames cannot collide.
- Polymarket publishes two parallel versions (`v1`, `v2`) selected with `--version`; the other platforms publish a single flat dataset, so `--version` does not apply to them.
- To keep multiple datasets current on the same machine, either run with `--platform ... --monitor`, or schedule separate `--auto` runs.

## Download integrity

Each platform's manifest tracks every file's expected size (from the server's `Content-Length` header). On every run, local files are checked against their **recorded** sizes. If a file is missing or has a size mismatch (e.g. from an interrupted download), it is deleted and redownloaded automatically. This check is local-only — it never re-contacts the server — so normal runs won't redownload files just because they changed upstream.

`--reverify` is the explicit "re-check against the server" pass. It sends a HEAD request for each file in the manifest (to the R2 host that actually stores the files), updates any stale recorded sizes, and deletes + requeues any local file whose size no longer matches the server:

```bash
uv run python scraper.py --reverify                          # polymarket v2
uv run python scraper.py --version v1 --reverify             # polymarket v1
uv run python scraper.py --platform all --reverify           # every platform
```

> **Heads-up:** the archive backfills older hourly snapshots — a file can grow by
> several MB on the server in the days after it first appears, then stabilize.
> Because `--reverify` compares against the live server, it will treat those grown
> files as mismatches and requeue them, which can trigger a sizable redownload.
> This is by design (re-fetching changed files is the point of `--reverify`);
> ordinary runs and `--monitor` are unaffected.

## Files

| File | Description |
|---|---|
| `scraper.py` | Main scraper script |
| `config.example.json` | Example configuration (copy to `config.json`) |
| `config.json` | Your local download directory configuration (git-ignored) |
| `downloaded*.json` | Per-platform manifests of already-downloaded files (auto-generated, git-ignored) |
| `pyproject.toml` | Project metadata and dependencies |

## Disclaimer

This tool is provided as-is for personal and research use. It is not affiliated
with, endorsed by, or sponsored by Polymarket, Kalshi, Limitless, Opinion, or
archive.pmxt.dev.

Users are solely responsible for ensuring their use of this tool complies with
all applicable laws, regulations, and third-party terms of service. The authors
assume no responsibility or liability for any data loss, inaccuracies, service
disruptions, or other consequences arising from the use of this software.

Use at your own risk. See [LICENSE.txt](LICENSE.txt) for full terms.

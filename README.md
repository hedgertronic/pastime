# Pastime for Python <!-- omit in toc -->

Tools for acquiring and analyzing baseball data from sources such as [Statcast](https://baseballsavant.mlb.com), the [MLB Stats API](https://statsapi.mlb.com), and the [Chadwick Bureau register](https://github.com/chadwickbureau/register).

The core is stdlib-only and returns raw data: CSV endpoints (Statcast search, leaderboards, lookup) return `list[dict]` where **every value is a string** (`""` for empty cells), and JSON endpoints (the MLB Stats API) return the raw `dict`/`list`. DataFrame conversion (polars/pandas) and progress bars (rich) are optional extras.

## Contents <!-- omit in toc -->

- [Installation](#installation)
- [Player ID lookup](#player-id-lookup)
- [Statcast pitch-level search](#statcast-pitch-level-search)
- [Leaderboards](#leaderboards)
- [MLB Stats API](#mlb-stats-api)
- [DataFrames (optional)](#dataframes-optional)
- [Command line](#command-line)
- [v1 Migration Notes](#v1-migration-notes)

## Installation

`pastime` uses [`uv`](https://docs.astral.sh/uv/) for environment and package management.

Install the core (no third-party dependencies):

```
uv pip install pastime
```

Install with optional extras:

```
uv pip install "pastime[polars]"     # to_frame(rows, backend="polars")
uv pip install "pastime[pandas]"      # to_frame(rows, backend="pandas")
uv pip install "pastime[progress]"    # rich progress bars on long pulls
uv pip install "pastime[all]"         # all runtime extras
```

For local development:

```
uv sync --all-extras --dev
uv run ruff check src tests
uv run pytest
```

## Player ID lookup

The Chadwick Bureau register (~6 MB) is fetched on demand and cached under your user cache directory (`$XDG_CACHE_HOME` or `~/.cache`, then `pastime/chadwick_people.csv`). No manual download step is required.

```python
from pastime import lookup

lookup.lookup(name="Trout")             # list[dict] of matching players
lookup.mlbam_to_fangraphs(545361)       # cross-reference IDs
lookup.refresh()                        # force a re-download of the register
```

## Statcast pitch-level search

`statcast_search` returns one `list[dict]` row per pitch. Ranges longer than 5 days are auto-chunked into 1-day requests (defending Savant's ~30,000-row cap) and fetched concurrently.

```python
from pastime.statcast import statcast_search, search_game, get_pitcher_arsenal

# Every pitch Gerrit Cole (MLBAM 543037) threw in the first week of the 2024 season.
pitches = statcast_search(
    start_date="2024-03-28",
    end_date="2024-04-03",
    player_type="pitcher",
    player_id=543037,
)
print(len(pitches), pitches[0]["pitch_type"], pitches[0]["release_speed"])

# Multi-value filters use Savant's pipe convention (not URL-encoded).
fastballs_sliders = statcast_search(
    start_date="2024-04-01",
    end_date="2024-04-30",
    player_id=543037,
    hfPT="FF|SL|",
)

# A single game by game_pk (find one via mlb.get_schedule), or a pitcher's
# arsenal summary over a range.
game = search_game(game_pk=...)  # an MLB game_pk, e.g. from mlb.get_schedule(...)
arsenal = get_pitcher_arsenal(543037, "2024-04-01", "2024-04-30")  # one row per pitch type
```

Add Alan Nathan's derived spin-physics columns explicitly (opt-in — search does not add them automatically):

```python
from pastime.statcast import add_spin_columns

enriched = add_spin_columns(pitches)   # induced/Magnus movement, spin efficiency, tilt, ...
```

## Leaderboards

Every leaderboard has a typed wrapper; `get_leaderboard(slug, ...)` is the generic, registry-checked entry point. Discover what's available with `list_leaderboards()` and `describe_leaderboard(slug)`.

```python
from pastime.statcast import (
    get_expected_statistics,
    get_leaderboard,
    get_percentile_rankings,
    list_leaderboards,
)

# A typed wrapper.
xstats = get_expected_statistics(year=2024, type="batter", min_pa=300)

# A single player's career percentile rankings (server-side filter).
trout_pctl = get_percentile_rankings(player_type="batter", player_id=545361)

# The generic entry point for any of the ~42 registered boards.
framing = get_leaderboard("catcher-framing", year=2024)
print(list_leaderboards(category="catching"))
```

### Bat tracking (the `year=` gotcha)

The three Hawk-Eye bat-tracking boards (2023+) **ignore the standard `year=` param** and silently return the current season. The wrappers handle this for you — but the season type you pass decides the result shape:

```python
from pastime.statcast import get_bat_tracking, get_swing_timing_miss_distance

# camelCase range (seasonStart/seasonEnd): a (start, end) tuple spans the range and
# returns ONE aggregated row per player — NO year column.
combined = get_bat_tracking(season=(2023, 2024), type="batter", min_swings=100)

# season[] array: a LIST is a true multi-select and returns one row per player
# PER year — it HAS a year column.
per_year = get_swing_timing_miss_distance(season=[2023, 2024], type="batter")
# per_year rows carry a "year" key; combined rows do not.
```

### HTML-backed leaderboards

Some Baseball Savant boards do not expose CSV and are scraped from inline JSON:
`get_park_factors`, `get_hot_stove`, and `get_rolling_windows`. These are covered
by offline parser fixtures and live smoke tests.

`get_top_performers` currently raises `SavantError`: Savant serves that page with
an empty inline `var data = {};` payload and renders the visible cards directly in
HTML. Failing loudly is intentional so callers do not mistake an empty dict for a
valid empty result.

## v1 Migration Notes

Pastime 1.0 intentionally narrows the public surface into three stable package
areas:

- `pastime.statcast` for Savant search, leaderboards, and derived physics.
- `pastime.mlb` for MLB Stats API pass-through wrappers.
- `pastime.lookup` for Chadwick Bureau ID cross-reference.

Older pre-1.0 modules such as `pastime.download`, `pastime.field`,
`pastime.query`, `pastime.statcast.query`, `pastime.statcast.base`,
`pastime.statcast.analysis`, and `pastime.statcast.leaderboard` were removed.
Use `pastime.statcast.search`, `pastime.statcast.leaderboards`,
`pastime.statcast.physics`, and `pastime.mlb` instead.

See [MIGRATION.md](MIGRATION.md) and [CHANGELOG.md](CHANGELOG.md) for the full
release notes.

## MLB Stats API

`pastime.mlb` is a pass-through port of `statsapi.mlb.com`; every function returns the raw JSON `dict`/`list`.

```python
from pastime import mlb

person = mlb.get_person(545361)                       # Mike Trout bio
schedule = mlb.get_schedule(date="2024-07-16")        # a day's games
roster = mlb.get_roster(team_id=108)                  # LA Angels roster
raw = mlb.mlb_api("/api/v1/teams", {"sportId": 1})    # low-level escape hatch
```

## DataFrames (optional)

The core returns `list[dict]`. Convert explicitly with `to_frame`, which imports the backend lazily (install the matching extra first).

```python
from pastime import to_frame
from pastime.statcast import get_expected_statistics

rows = get_expected_statistics(year=2024, type="batter", min_pa=300)
df = to_frame(rows, backend="polars")   # or backend="pandas"
```

## Command line

A single `pastime` console script exposes four subcommands. `--format` defaults to `csv` for `lookup`/`search`/`leaderboard` and `json` for `mlb`; `-o/--output` writes to a file instead of stdout.

```
# Player ID lookup
pastime lookup --name "Gerrit Cole"

# Pitch-level search (comma-separated values are pipe-joined into Savant's filter)
pastime search --start 2024-04-01 --end 2024-04-07 --player-id 543037 --hfPT=FF,SL

# Leaderboards: list them, then pull one
pastime leaderboard --list --category pitching
pastime leaderboard catcher-framing --season 2024

# Multi-year works on the bat-tracking boards (which use season arrays/ranges)
pastime leaderboard bat-tracking/swing-timing-miss-distance --season 2023,2024

# MLB Stats API (defaults to JSON output)
pastime mlb get_schedule --date=2024-07-16
pastime mlb --list
```

Both `search` and `leaderboard`/`mlb` accept arbitrary `--field=value` (or `--field value`) passthrough; only `search` pipe-joins comma-separated values.

# Pastime for Python <!-- omit in toc -->

Python tools for baseball data from [Statcast](https://baseballsavant.mlb.com) and the [MLB Stats API](https://statsapi.mlb.com).

Pastime is a small data-access library for researchers, analysts, and developers who want raw baseball data without committing to a DataFrame stack. The core package is stdlib-only. Statcast CSV endpoints return `list[dict]` with string values, MLB Stats API endpoints return raw JSON `dict` payloads, and optional extras add DataFrame conversion or progress bars when you want them.

## Contents <!-- omit in toc -->

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Statcast](#statcast)
  - [Pitch-Level Search](#pitch-level-search)
  - [Search Helpers](#search-helpers)
  - [Leaderboards](#leaderboards)
  - [Bat Tracking](#bat-tracking)
  - [HTML-Backed Leaderboards](#html-backed-leaderboards)
  - [Derived Spin Physics](#derived-spin-physics)
- [MLB Stats API](#mlb-stats-api)
  - [Low-Level API](#low-level-api)
  - [People](#people)
  - [Teams](#teams)
  - [Games And Schedule](#games-and-schedule)
  - [Stats](#stats)
  - [League And Baseball Metadata](#league-and-baseball-metadata)
  - [Miscellaneous Endpoints](#miscellaneous-endpoints)
- [Player ID Lookup](#player-id-lookup)
- [DataFrames](#dataframes)
- [Command Line](#command-line)
- [v1 Migration Notes](#v1-migration-notes)

## Installation

Install the core package:

```bash
pip install pastime
```

Or with [`uv`](https://docs.astral.sh/uv/):

```bash
uv add pastime
```

Install optional extras when you need DataFrames or progress bars:

```bash
uv add "pastime[polars]"     # to_frame(rows, backend="polars")
uv add "pastime[pandas]"      # to_frame(rows, backend="pandas")
uv add "pastime[progress]"    # rich progress bars on long pulls
uv add "pastime[all]"         # all runtime extras
```

For local development:

```bash
uv sync --all-extras --dev
uv run ruff check src tests
uv run pytest
```

## Getting Started

Pastime keeps each source under its own namespace:

```python
from pastime import mlb, statcast

pitches = statcast.statcast_search(
    start_date="2024-03-28",
    end_date="2024-04-03",
    player_type="pitcher",
    player_id=543037,
)

schedule = mlb.get_schedule(date="2024-07-16")
```

The library returns raw data by design:

- Statcast CSV endpoints return `list[dict]`; every value is a string, with `""` for empty cells.
- HTML-backed Statcast endpoints return parsed JSON from Baseball Savant pages.
- MLB Stats API endpoints return the raw JSON `dict`.
- DataFrame conversion is explicit through `to_frame(...)`.

## Statcast

The `pastime.statcast` namespace wraps Baseball Savant search, leaderboard, and derived physics workflows.

### Pitch-Level Search

`statcast_search(...)` fetches pitch-level Baseball Savant CSV data. Ranges longer than five days are split into one-day requests and fetched concurrently to avoid Savant's row cap.

```python
from pastime.statcast import statcast_search

# Every pitch Gerrit Cole (MLBAM 543037) threw in the first week of 2024.
pitches = statcast_search(
    start_date="2024-03-28",
    end_date="2024-04-03",
    player_type="pitcher",
    player_id=543037,
)

print(len(pitches), pitches[0]["pitch_type"], pitches[0]["release_speed"])
```

Important options:

- `player_type`: `"pitcher"` or `"batter"`.
- `player_id`: MLBAM player ID.
- `team`: team abbreviation/name, resolved through Pastime constants.
- `home_road`: home/away filter for team searches.
- `level`: `"mlb"` or `"milb"`.
- arbitrary Savant filters such as `hfPT`, `hfBBT`, `hfGT`, or `game_date_gt`.

Multi-value filters use Baseball Savant's pipe convention:

```python
fastballs_sliders = statcast_search(
    start_date="2024-04-01",
    end_date="2024-04-30",
    player_id=543037,
    hfPT="FF|SL|",
)
```

### Search Helpers

Convenience helpers build common Statcast search filters:

- `search_game(game_pk, level="mlb", **filters)`: all pitches for one MLB or MiLB game.
- `search_matchup(pitcher_id=..., batter_id=..., start_date=..., end_date=...)`: pitcher/batter matchups.
- `search_team(team, start_date=..., end_date=..., home_road=None, level="mlb")`: team-filtered searches.
- `get_pitcher_arsenal(player_id, start_date, end_date, level="mlb")`: aggregated pitch arsenal rows.
- `aggregate_pitcher_arsenal(rows)`: aggregate already-fetched pitch rows.

```python
from pastime.statcast import get_pitcher_arsenal, search_game

game = search_game(game_pk=747220)
arsenal = get_pitcher_arsenal(543037, "2024-04-01", "2024-04-30")
```

### Leaderboards

Pastime includes a registry of Baseball Savant leaderboards with typed wrappers plus a generic fetcher.

Discovery:

- `list_leaderboards(category=None)`: list registered leaderboard slugs.
- `describe_leaderboard(slug)`: inspect category, params, year behavior, key columns, and notes.
- `get_leaderboard(slug, **params)`: generic, registry-checked fetcher.

Common typed wrappers include:

- Batting: `get_expected_statistics`, `get_exit_velocity_barrels`, `get_batted_ball`, `get_home_runs`, `get_percentile_rankings`, `get_custom_leaderboard`.
- Pitching: `get_pitch_arsenals`, `get_pitch_arsenal_stats`, `get_pitch_movement`, `get_spin_direction`, `get_active_spin`, `get_pitch_tempo`, `get_game_scores`.
- Fielding: `get_outs_above_average`, `get_directional_oaa`, `get_fielding_run_value`, `get_arm_strength`, `get_arm_angles`, `get_catch_probability`, `get_outfield_jump`.
- Catching: `get_catcher_framing`, `get_catcher_throwing`, `get_catcher_blocking`, `get_catcher_stance`, `get_poptime`.
- Running: `get_sprint_speed`, `get_running_splits`, `get_baserunning`, `get_baserunning_run_value`, `get_basestealing_run_value`, `get_pitcher_running_game`.
- Other: `get_year_to_year`, `get_abs_challenges`, `get_timer_infractions`, `get_swing_take`.

```python
from pastime.statcast import (
    get_expected_statistics,
    get_leaderboard,
    get_percentile_rankings,
    list_leaderboards,
)

xstats = get_expected_statistics(year=2024, type="batter", min_pa=300)
trout_pctl = get_percentile_rankings(player_type="batter", player_id=545361)
framing = get_leaderboard("catcher-framing", year=2024)

print(list_leaderboards(category="catching"))
```

### Bat Tracking

The Hawk-Eye bat-tracking boards use newer Baseball Savant season parameters. The wrappers handle those differences for you:

- `get_bat_tracking`
- `get_swing_path_attack_angle`
- `get_swing_timing_miss_distance`

```python
from pastime.statcast import get_bat_tracking, get_swing_timing_miss_distance

# seasonStart/seasonEnd range: one aggregated row per player over the span.
combined = get_bat_tracking(season=(2023, 2024), type="batter", min_swings=100)

# season[] array: one row per player per selected season.
per_year = get_swing_timing_miss_distance(season=[2023, 2024], type="batter")
```

### HTML-Backed Leaderboards

Some Baseball Savant pages do not expose CSV. Pastime parses their inline JSON:

- `get_park_factors`
- `get_hot_stove`
- `get_rolling_windows`

`get_top_performers` currently raises `SavantError`: Savant serves that page with an empty inline `var data = {};` payload and renders the visible cards directly in HTML. Failing loudly is intentional so callers do not mistake an empty response for valid data.

### Derived Spin Physics

`add_spin_columns(...)` adds Alan Nathan-style derived spin physics columns to Statcast pitch rows. It is opt-in; search does not mutate or enrich rows automatically.

```python
from pastime.statcast import add_spin_columns, axis_to_clock

enriched = add_spin_columns(pitches)
clock = axis_to_clock(225)
```

Added columns include induced/Magnus movement estimates, transverse acceleration, spin efficiency, inferred spin components, and clock-face tilt where inputs are available.

## MLB Stats API

The `pastime.mlb` namespace is a pass-through wrapper for `statsapi.mlb.com`. Every function returns raw JSON and accepts the most common endpoint parameters. For unsupported or newly discovered routes, use `mlb_api(path, params)`.

### Low-Level API

```python
from pastime import mlb

raw = mlb.mlb_api("/api/v1/teams", {"sportId": 1})
```

### People

Player/person endpoints:

- `get_people(person_ids, hydrate=None, fields=None)`
- `get_person(person_id, hydrate=None, fields=None)`
- `search_players(name, sport_id=1, active_status=None, fields=None)`
- `find_player(name, sport_id=1)`
- `resolve_player_id(name, sport_id=1)`
- `get_player_changes(...)`
- `get_free_agents(season, order=None)`
- `get_player_stats_all_sports(person_id, group, stats, ...)`

```python
person = mlb.get_person(545361)
matches = mlb.search_players("Mike Trout")
```

### Teams

Team directory, roster, team stats, and team-specific endpoints:

- `get_teams`, `get_team`, `get_team_history`
- `get_team_stats`, `get_team_leaders`
- `get_team_affiliates`, `get_roster`
- `get_team_specific_leaders`, `get_team_specific_stats`, `get_team_alumni`
- `resolve_team_id`

```python
teams = mlb.get_teams(sport_id=1)
roster = mlb.get_roster(team_id=108)
```

### Games And Schedule

Schedule, live game feed, per-game views, and postseason helpers:

- `get_schedule`
- `get_gamefeed`, `get_gamefeed_diffpatch`, `get_gamefeed_timestamps`
- `get_boxscore`, `get_linescore`, `get_playbyplay`, `get_game_content`
- `get_context_metrics`, `get_win_probability`
- `get_color_feed`, `get_color_diffpatch`, `get_color_timestamps`
- `get_game_changes`
- `get_postseason_schedule`, `get_postseason_series`, `get_tied_games`

```python
schedule = mlb.get_schedule(date="2024-07-16")
feed = mlb.get_gamefeed(game_pk=747220)
box = mlb.get_boxscore(game_pk=747220)
```

### Stats

League/player/team stats helpers:

- `build_stats_hydrate`
- `get_stats`
- `get_stat_leaders`
- `get_streaks`

```python
leaders = mlb.get_stat_leaders(leader_categories="homeRuns", season=2024)
stats = mlb.get_stats(stats="season", group="hitting", season=2024)
```

### League And Baseball Metadata

Discovery endpoints return valid parameter values and metadata used by the other wrappers:

- Stat/game metadata: `get_stat_types`, `get_stat_groups`, `get_game_types`, `get_game_status`, `get_baseball_stats`, `get_metrics`
- Field/game descriptors: `get_positions`, `get_situation_codes`, `get_pitch_types`, `get_hit_trajectories`, `get_event_types`, `get_schedule_event_types`
- Environment/job/platform descriptors: `get_wind_direction`, `get_sky`, `get_job_types`, `get_languages`, `get_platforms`
- Organization descriptors: `get_league_leader_types`, `get_sports_discovery`, `get_roster_types`, `get_standings_types`

```python
pitch_types = mlb.get_pitch_types()
positions = mlb.get_positions()
```

### Miscellaneous Endpoints

Other Stats API wrappers include:

- Draft/prospects/awards: `get_draft`, `get_draft_prospects`, `get_award_recipients`
- Transactions/standings/attendance: `get_transactions`, `get_standings`, `get_attendance`
- Venues/leagues/divisions/seasons/sports: `get_venues`, `get_venue`, `get_leagues`, `get_league`, `get_divisions`, `get_seasons`, `get_season`, `get_sports`, `get_sport`, `get_sport_players`
- Jobs/broadcasts/conferences: `get_umpires`, `get_official_scorers`, `get_datacasters`, `get_broadcasts`, `get_conferences`
- Specialty endpoints: `get_high_low`, `get_home_run_derby`, `get_derby_bracket`, `get_derby_pool`, `get_game_pace`, `get_uniforms_game`, `get_uniforms_team`

```python
transactions = mlb.get_transactions(start_date="2024-07-01", end_date="2024-07-31")
venues = mlb.get_venues()
```

## Player ID Lookup

`pastime.lookup` provides player ID cross-reference helpers backed by the Chadwick Bureau register. The register is fetched on demand and cached under your user cache directory (`$XDG_CACHE_HOME` or `~/.cache`, then `pastime/chadwick_people.csv`).

```python
from pastime import lookup

lookup.lookup(name="Trout")
lookup.mlbam_to_fangraphs(545361)
lookup.mlbam_to_bbref(545361)
lookup.fangraphs_to_mlbam(15640)
lookup.bbref_to_mlbam("troum001")
lookup.refresh()
```

## DataFrames

The core returns raw rows. Convert explicitly with `to_frame`, which imports the backend lazily.

```python
from pastime import to_frame
from pastime.statcast import get_expected_statistics

rows = get_expected_statistics(year=2024, type="batter", min_pa=300)
df = to_frame(rows, backend="polars")   # or backend="pandas"
```

## Command Line

The `pastime` console script exposes four subcommands. `--format` defaults to `csv` for `lookup`, `search`, and `leaderboard`, and `json` for `mlb`. `-o/--output` writes to a file instead of stdout.

```bash
# Player ID lookup
pastime lookup --name "Gerrit Cole"

# Pitch-level Statcast search
pastime search --start 2024-04-01 --end 2024-04-07 --player-id 543037 --hfPT=FF,SL

# Leaderboards
pastime leaderboard --list --category pitching
pastime leaderboard catcher-framing --season 2024
pastime leaderboard bat-tracking/swing-timing-miss-distance --season 2023,2024

# MLB Stats API
pastime mlb get_schedule --date=2024-07-16
pastime mlb --list
```

`search`, `leaderboard`, and `mlb` accept arbitrary `--field=value` passthrough arguments. Only `search` pipe-joins comma-separated values for Baseball Savant filters.

## v1 Migration Notes

Pastime 1.0 intentionally narrows the public surface into three stable package areas:

- `pastime.statcast` for Baseball Savant search, leaderboards, and derived physics.
- `pastime.mlb` for MLB Stats API pass-through wrappers.
- `pastime.lookup` for player ID cross-reference helpers.

Older pre-1.0 modules such as `pastime.download`, `pastime.field`, `pastime.query`, `pastime.statcast.query`, `pastime.statcast.base`, `pastime.statcast.analysis`, and `pastime.statcast.leaderboard` were removed. Use `pastime.statcast.search`, `pastime.statcast.leaderboards`, `pastime.statcast.physics`, and `pastime.mlb` instead.

See [MIGRATION.md](MIGRATION.md) and [CHANGELOG.md](CHANGELOG.md) for the full release notes.

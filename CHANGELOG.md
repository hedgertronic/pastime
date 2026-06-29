# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0]

### Added

- `fungo.mlb`, a typed pass-through wrapper surface for MLB Stats API
  endpoints.
- Baseball Savant leaderboard registry with typed wrappers and explicit
  season-parameter handling for bat-tracking boards.
- Statcast pitch-level search helpers for games, teams, matchups, and pitcher
  arsenal aggregation.
- Nathan-style derived spin physics columns via `fungo.statcast.physics`.
- Chadwick Bureau player lookup and ID converters with local cache refresh.
- `fungo` CLI with lookup, search, leaderboard, and MLB subcommands.
- Optional pandas/polars DataFrame conversion through `fungo.to_frame`.
- `py.typed` marker for inline type annotations.
- GitHub Actions CI across Python 3.12-3.14 with ruff, mypy, offline pytest
  coverage, package build, and wheel install smoke tests.

### Changed

- Packaged with Hatchling, uv, PEP 621 metadata, and runtime-only extras.
- The public API is organized around `fungo.statcast`, `fungo.mlb`, and
  `fungo.lookup`.
- Live tests are opt-in; default test runs are fully offline.

### Removed

- Removed pre-1.0 experimental modules: `fungo.download`, `fungo.field`,
  `fungo.query`, `fungo.statcast.analysis`, `fungo.statcast.base`,
  `fungo.statcast.leaderboard`, and `fungo.statcast.query`.

### Notes

- `fungo.statcast.get_top_performers` raises `SavantError` because Baseball
  Savant currently serves that page with an empty inline JSON payload and
  renders the cards directly in HTML.

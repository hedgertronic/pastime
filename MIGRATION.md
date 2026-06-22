# Pastime 1.0 Migration Notes

Pastime 1.0 is a cleanup release that stabilizes the package around explicit
subpackages instead of the older experimental module layout.

## Public Surface

Use these imports:

- `pastime.statcast` for Baseball Savant pitch search, leaderboards, and derived
  spin physics.
- `pastime.mlb` for MLB Stats API pass-through wrappers.
- `pastime.lookup` for Chadwick Bureau player ID lookup.
- `pastime.to_frame` for optional pandas/polars conversion.

## Removed Pre-1.0 Modules

The following modules were removed:

- `pastime.download`
- `pastime.field`
- `pastime.query`
- `pastime.statcast.analysis`
- `pastime.statcast.base`
- `pastime.statcast.leaderboard`
- `pastime.statcast.query`

Use these replacements:

- `pastime.statcast.search` for pitch-level Savant search.
- `pastime.statcast.leaderboards` for Savant leaderboard wrappers.
- `pastime.statcast.physics` for derived spin/movement columns.
- `pastime.mlb` for MLB Stats API endpoints.

## Known Savant Limitation

`pastime.statcast.get_top_performers` raises `SavantError` in 1.0 because
Baseball Savant currently serves the page with an empty inline JSON payload
(`var data = {};`) and renders the cards directly in HTML. Returning `{}` would
hide upstream drift, so the wrapper fails loudly until Savant exposes a stable
machine-readable payload again.

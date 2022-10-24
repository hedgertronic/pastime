import polars as pl

from pastime.statcast.query import LeaderboardQuery


URL = "https://baseballsavant.mlb.com/leaderboard"


def exit_velocity(
    player_type: str = "batters",
    position: str | None = None,
    team: str | None = None,
    season: str | int | None = None,
    minimum_bbe: str = "qualified",
) -> pl.DataFrame:
    return query(
        leaderboard_name="exit_velocity",
        player_type=player_type,
        position=position,
        team=team,
        season=season,
        minimum_bbe=minimum_bbe,
    )


def expected_stats(
    player_type: str = "batters",
    position: str | None = None,
    team: str | None = None,
    season: str | int = 2022,
    minimum_bip: str = "qualified",
) -> pl.DataFrame:
    return query(
        leaderboard_name="expected_stats",
        player_type=player_type,
        position=position,
        team=team,
        season=season,
        minimum_bip=minimum_bip,
    )


def percentile_rankings(
    player_type: str = "batters",
    team: str | None = None,
    season: str | int = 2022,
) -> pl.DataFrame:
    return query(
        leaderboard_name="percentile_rankings",
        player_type=player_type,
        team=team,
        season=season,
    )


def swing_take(
    season: str | None = None,
    player_type: str = "batters",
    group: str = "all",
    subgroup: str | None = None,
    team: str | None = None,
    min_pitches: str = "qualified",
) -> pl.DataFrame:
    return query(
        leaderboard_name="swing_take",
        season=season,
        player_type=player_type,
        group=group,
        subgroup=subgroup,
        team=team,
        min_pitches=min_pitches,
    )


def pitch_arsenals(
    season: str | int = 2022,
    metric: str = "average speed",
    handedness: str | None = None,
    min_pitches: str | int = 250,
) -> pl.DataFrame:
    return query(
        leaderboard_name="pitch_arsenals",
        season=season,
        metric=metric,
        handedness=handedness,
        min_pitches=min_pitches,
    )


def pitch_arsenal_stats(
    player_type: str = "pitcher",
    pitch_type: str | None = None,
    team: str | None = None,
    season: str | int = 2022,
    min_pa: str | int = 10,
) -> pl.DataFrame:
    return query(
        leaderboard_name="pitch_arsenal_stats",
        player_type=player_type,
        pitch_type=pitch_type,
        team=team,
        season=season,
        min_pa=min_pa,
    )


def home_runs(
    season: str | int = 2022,
    player_type: str = "batters",
    team: str | None = None,
    min_hr: str | int = 0,
) -> pl.DataFrame:
    return query(
        leaderboard_name="home_runs",
        season=season,
        player_type=player_type,
        team=team,
        min_hr=min_hr,
    )


def year_to_year_changes(
    player_type: str | None = None,
    stat_type: str = "hard hit %",
    year_pair: str = "2021-2022",
) -> pl.DataFrame:
    return query(
        leaderboard_name="year_to_year_changes",
        player_type=player_type,
        stat_type=stat_type,
        year_pair=year_pair,
    )


def pitch_tempo(
    player_type: str = "pitcher",
    min_pitches: str = "qualified",
    team: str | None = None,
    start_season: str | int = 2022,
    end_season: str | int = 2022,
    split_seasons: bool = False,
) -> pl.DataFrame:
    return query(
        leaderboard_name="pitch_tempo",
        player_type=player_type,
        min_pitches=min_pitches,
        team=team,
        start_season=start_season,
        end_season=end_season,
        split_seasons="yes" if split_seasons else "no",
        with_team_only="1",
    )


def pitch_movement(
    season: str | int = 2022,
    pitch_type: str = "FF",
    team: str | None = None,
    handedness: str | None = None,
    min_pitches: str = "qualified",
) -> pl.DataFrame:
    return query(
        leaderboard_name="pitch_movement",
        season=season,
        pitch_type=pitch_type,
        team=team,
        handedness=handedness,
        min_pitches=min_pitches,
    )


def active_spin(
    season: str = "2022 spin based",
    handedness: str | None = None,
    min_pitches: str | int = 50,
) -> pl.DataFrame:
    return query(
        leaderboard_name="active_spin",
        season=season,
        handedness=handedness,
        min_pitches=min_pitches,
    )


def spin_direction_comparison(
    season: str | int = 2022,
    team: str | None = None,
    pitch_types: str = "FF/CH",
    min_pitches: str | int = 100,
    pov: str = "pitcher",
) -> pl.DataFrame:
    return query(
        leaderboard_name="spin_direction_comparison",
        season=season,
        team=team,
        pitch_types=pitch_types,
        min_pitches=min_pitches,
        pov=pov,
    )


def spin_direction(
    season: str | int = 2022,
    team: str | None = None,
    pitch_type: str = "FF",
    handedness: str | None = None,
    min_pitches: str | int = 100,
    pov: str = "pitcher",
) -> pl.DataFrame:
    return query(
        leaderboard_name="spin_direction",
        season=season,
        team=team,
        pitch_type=pitch_type,
        handedness=handedness,
        min_pitches=min_pitches,
        pov=pov,
    )


def oaa(
    player_type: str = "fielder",
    min_attempts: str = "qualified",
    position: str | None = None,
    roles: str | list[str] | None = None,
    team: str | None = None,
    time_range: str = "year",
    start_season: str | int = 2022,
    end_season: str | int = 2022,
    split_seasons: bool = False,
) -> pl.DataFrame:
    return query(
        leaderboard_name="oaa",
        player_type=player_type,
        min_attempts=min_attempts,
        position=position,
        roles=roles,
        team=team,
        time_range=time_range,
        start_season=start_season,
        end_season=end_season,
        split_seasons="yes" if split_seasons else "no",
        visual="hide",
    )


def of_directional_oaa(
    min_opportunities: str = "qualified",
    team: str | None = None,
    season: str | int = 2022,
) -> pl.DataFrame:
    return query(
        leaderboard_name="of_directional_oaa",
        min_opportunities=min_opportunities,
        team=team,
        season=season,
    )


def of_catch_probability(
    by_team: bool = False,
    min_opportunities: str = "qualified",
    play_type: str = "all plays",
    season: str | int = 2022,
) -> pl.DataFrame:
    return query(
        leaderboard_name="of_catch_probability",
        type="team" if by_team else "player",
        min_opportunities=min_opportunities,
        play_type=play_type,
        season=season,
    )


def outfield_jump(
    season: str | int = 2022,
    min_attempts: str = "qualifed",
) -> pl.DataFrame:
    return query(
        leaderboard_name="outfield_jump",
        season=season,
        min_attempts=min_attempts,
    )


def poptime(
    team: str | None = None,
    season: str | int = 2022,
    min_attempts_2b: str | int = 5,
    min_attempts_3b: str | int = 0,
) -> pl.DataFrame:
    return query(
        leaderboard_name="poptime",
        team=team,
        season=season,
        min_attempts_2b=min_attempts_2b,
        min_attempts_3b=min_attempts_3b,
    )


def framing(
    player_type: str = "catcher",
    team: str | None = None,
    season: str | int = 2022,
    min_pitches: str = "qualified",
) -> pl.DataFrame:
    return query(
        leaderboard_name="framing",
        player_type=player_type,
        team=team,
        season=season,
        min_pitches=min_pitches,
    )


def arm_strength(
    position: str | None = None,
    team: str | None = None,
    season: str | int = 2022,
    min_throws: str | int = 100,
) -> pl.DataFrame:
    return query(
        leaderboard_name="arm_strength",
        position=position,
        team=team,
        season=season,
        min_throws=min_throws,
    )


def sprint_speed(
    position: str | None = None,
    team: str | None = None,
    start_season: str | int = 2022,
    end_season: str | int = 2022,
    min_opportunities: str | int = 10,
) -> pl.DataFrame:
    return query(
        leaderboard_name="sprint_speed",
        position=position,
        team=team,
        start_season=start_season,
        end_season=end_season,
        min_opportunities=min_opportunities,
    )


def running_splits(
    position: str | None = None,
    team: str | None = None,
    season: str | int = 2022,
    bat_side: str | None = None,
    min_opportunities: str | int = 5,
    percentile: bool = False,
) -> pl.DataFrame:
    return query(
        leaderboard_name="running_splits",
        position=position,
        team=team,
        season=season,
        bat_side=bat_side,
        min_opportunities=min_opportunities,
        type="percent" if percentile else "raw",
    )


def park_factors(
    factor_type: str = "year",
    season: str | int = 2022,
    bat_side: str | None = None,
    condition: str = "all",
    three_year_rolling: bool = False,
    stat: str = "woba",
) -> pl.DataFrame:
    return query(
        leaderboard_name="park_factors",
        factor_type=factor_type,
        season=season,
        bat_side=bat_side,
        condition=condition,
        three_year_rolling="" if three_year_rolling else "no",
        stat=stat,
    )


def query(leaderboard_name: str, **kwargs):
    result = LeaderboardQuery(
        url=URL, collection_name=leaderboard_name, **kwargs
    ).request()

    return pl.read_csv(result, parse_dates=True, ignore_errors=True).fill_nan(None)

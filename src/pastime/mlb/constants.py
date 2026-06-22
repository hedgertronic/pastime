"""MLB Stats API constants.

Module-level dicts/lists mirroring the MLB Stats API enumeration tables
(sport IDs, game types, stat types, positions, leaders, team IDs, ...).
"""

from __future__ import annotations

#####################################################################
# Sports / levels
#####################################################################

SPORT_IDS: dict[int, dict[str, str]] = {
    1: {"code": "mlb", "name": "Major League Baseball", "abbreviation": "MLB"},
    11: {"code": "aaa", "name": "Triple-A", "abbreviation": "AAA"},
    12: {"code": "aax", "name": "Double-A", "abbreviation": "AA"},
    13: {"code": "afa", "name": "High-A", "abbreviation": "A+"},
    14: {"code": "afx", "name": "Single-A", "abbreviation": "A"},
    16: {"code": "rok", "name": "Rookie", "abbreviation": "ROK"},
    17: {"code": "win", "name": "Winter Leagues", "abbreviation": "WIN"},
    21: {"code": "min", "name": "Minor League Baseball", "abbreviation": "Minors"},
    22: {"code": "bbc", "name": "College Baseball", "abbreviation": "College"},
    23: {"code": "ind", "name": "Independent Leagues", "abbreviation": "IND"},
    31: {
        "code": "jml",
        "name": "Nippon Professional Baseball",
        "abbreviation": "NPB",
    },
    32: {
        "code": "kor",
        "name": "Korean Baseball Organization",
        "abbreviation": "KOR",
    },
    51: {"code": "int", "name": "International Baseball", "abbreviation": "INT"},
    52: {"code": "oly", "name": "Olympic Baseball", "abbreviation": "OLY"},
    61: {"code": "nlb", "name": "Negro League Baseball", "abbreviation": "NLB"},
    586: {"code": "hsb", "name": "High School Baseball", "abbreviation": "H.S."},
}

SPORT_ID_BY_ABBREV: dict[str, int] = {
    meta["abbreviation"]: sid for sid, meta in SPORT_IDS.items()
}

# Fan-out recipe — the sport IDs to iterate through for full MLB+MiLB
# coverage (gotcha #1: sportId inside stats(...) is scalar-only).
HYDRATE_SPORT_IDS: tuple[int, ...] = (1, 11, 12, 13, 14, 16)


#####################################################################
# Game types
#####################################################################

GAME_TYPES: dict[str, str] = {
    "S": "Spring Training",
    "R": "Regular Season",
    "F": "Wild Card",
    "D": "Division Series",
    "L": "League Championship Series",
    "W": "World Series",
    "C": "Championship",
    "N": "Nineteenth Century Series",
    "P": "Playoffs",
    "A": "All-Star Game",
    "I": "Intrasquad",
    "E": "Exhibition",
}


#####################################################################
# Stats
#####################################################################

STAT_TYPES: list[str] = [
    "projected",
    "projectedRos",
    "yearByYear",
    "yearByYearAdvanced",
    "yearByYearPlayoffs",
    "season",
    "standard",
    "advanced",
    "career",
    "careerRegularSeason",
    "careerAdvanced",
    "seasonAdvanced",
    "careerStatSplits",
    "careerPlayoffs",
    "gameLog",
    "playLog",
    "pitchLog",
    "metricLog",
    "metricAverages",
    "pitchArsenal",
    "outsAboveAverage",
    "expectedStatistics",
    "sabermetrics",
    "sprayChart",
    "tracking",
    "vsPlayer",
    "vsPlayerTotal",
    "vsPlayer5Y",
    "vsTeam",
    "vsTeam5Y",
    "vsTeamTotal",
    "lastXGames",
    "byDateRange",
    "byDateRangeAdvanced",
    "byMonth",
    "byMonthPlayoffs",
    "byDayOfWeek",
    "byDayOfWeekPlayoffs",
    "homeAndAway",
    "homeAndAwayPlayoffs",
    "winLoss",
    "winLossPlayoffs",
    "rankings",
    "rankingsByYear",
    "statsSingleSeason",
    "statsSingleSeasonAdvanced",
    "hotColdZones",
    "availableStats",
    "opponentsFaced",
    "gameTypeStats",
    "firstYearStats",
    "lastYearStats",
    "statSplits",
    "statSplitsAdvanced",
    "atGameStart",
    "vsOpponents",
    "sabermetricsMultiTeam",
    "projected_Zips",
    "projected_ZipsRos",
    "projected_Zips2YR",
    "projected_Zips3YR",
]

STAT_GROUPS: list[str] = [
    "hitting",
    "pitching",
    "fielding",
    "catching",
    "running",
    "game",
    "team",
    "streak",
]


#####################################################################
# Roster / standings
#####################################################################

ROSTER_TYPES: dict[str, str] = {
    "active": "Active roster",
    "fullRoster": "Full roster (active + inactive)",
    "fullSeason": "Full season roster",
    "40Man": "40-man roster",
    "nonRosterInvitees": "Non-roster invitees",
    "allTime": "All-time roster",
    "depthChart": "Depth chart",
    "gameday": "Gameday roster",
    "coach": "Coaching staff",
}

STANDINGS_TYPES: list[str] = [
    "regularSeason",
    "wildCard",
    "divisionLeaders",
    "wildCardWithLeaders",
    "firstHalf",
    "secondHalf",
    "springTraining",
    "postseason",
    "byDivision",
    "byConference",
    "byLeague",
    "byOrganization",
    "currentHalf",
]


#####################################################################
# Positions
#####################################################################

POSITIONS: list[dict[str, str]] = [
    {"abbrev": "P", "code": "1", "name": "Pitcher", "type": "Pitcher"},
    {"abbrev": "C", "code": "2", "name": "Catcher", "type": "Catcher"},
    {"abbrev": "1B", "code": "3", "name": "First Base", "type": "Infielder"},
    {"abbrev": "2B", "code": "4", "name": "Second Base", "type": "Infielder"},
    {"abbrev": "3B", "code": "5", "name": "Third Base", "type": "Infielder"},
    {"abbrev": "SS", "code": "6", "name": "Shortstop", "type": "Infielder"},
    {"abbrev": "LF", "code": "7", "name": "Left Field", "type": "Outfielder"},
    {"abbrev": "CF", "code": "8", "name": "Center Field", "type": "Outfielder"},
    {"abbrev": "RF", "code": "9", "name": "Right Field", "type": "Outfielder"},
    {"abbrev": "DH", "code": "10", "name": "Designated Hitter", "type": "Hitter"},
    {"abbrev": "PH", "code": "11", "name": "Pinch Hitter", "type": "Hitter"},
    {"abbrev": "PR", "code": "12", "name": "Pinch Runner", "type": "Runner"},
]

POSITION_BY_ABBREV: dict[str, str] = {p["abbrev"]: p["code"] for p in POSITIONS}


#####################################################################
# Pitch types
#####################################################################

PITCH_TYPES: dict[str, str] = {
    "FF": "Four-seam Fastball",
    "SI": "Sinker",
    "FC": "Cutter",
    "SL": "Slider",
    "ST": "Sweeper",
    "CU": "Curveball",
    "KC": "Knuckle Curve",
    "CH": "Changeup",
    "FS": "Splitter",
    "FO": "Forkball",
    "SC": "Screwball",
    "KN": "Knuckleball",
    "EP": "Eephus",
    "CS": "Slow Curve",
    "SV": "Slurve",
    "GY": "Gyroball",
    "FT": "Two-seam Fastball",
    "FA": "Fastball (generic)",
    "IN": "Intentional Ball",
    "PO": "Pitchout",
    "AB": "Automatic Ball",
    "AS": "Automatic Strike",
    "NP": "No Pitch",
    "UN": "Unknown",
}


#####################################################################
# Situation codes (for statSplits)
#####################################################################

SITUATION_CODES: dict[str, dict[str, str]] = {
    "game_context": {
        "h": "Home Games",
        "a": "Away Games",
        "d": "Day Games",
        "n": "Night Games",
        "g": "On Grass",
        "t": "On Turf",
    },
    "month": {
        str(i): m
        for i, m in enumerate(
            [
                "",
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ]
        )
        if i > 0
    },
    "timeframe": {
        "h0": "Season To Date",
        "h1": "First Half",
        "h2": "Second Half",
    },
    "count": {f"p{i}": f"Count code p{i}" for i in range(12)},
    "runners": {
        "ro": "Runners On",
        "ri": "Runners In Scoring Position",
        "re": "Bases Empty",
        "r1": "Runner on 1st",
        "r2": "Runner on 2nd",
        "r3": "Runner on 3rd",
        "r12": "Runners on 1st & 2nd",
        "r13": "Runners on 1st & 3rd",
        "r23": "Runners on 2nd & 3rd",
        "r123": "Bases Loaded",
    },
    "outs": {
        "o0": "0 Outs",
        "o1": "1 Out",
        "o2": "2 Outs",
    },
    "handedness": {
        "vr": "vs Right-Handed Pitcher",
        "vl": "vs Left-Handed Pitcher",
    },
    "inning": {
        **{f"i{i}": f"Inning {i}" for i in range(1, 10)},
        "ie": "Extra Innings",
    },
}


#####################################################################
# League leader categories
#####################################################################

LEADER_CATEGORIES: list[str] = [
    "assists",
    "shutouts",
    "homeRuns",
    "sacrificeBunts",
    "sacrificeFlies",
    "runs",
    "groundoutToFlyoutRatio",
    "stolenBases",
    "battingAverage",
    "groundOuts",
    "numberOfPitches",
    "onBasePercentage",
    "caughtStealing",
    "groundIntoDoublePlays",
    "totalBases",
    "earnedRunAverage",
    "fieldingPercentage",
    "walksAndHitsPerInningPitched",
    "flyouts",
    "hitByPitches",
    "gamesPlayed",
    "walks",
    "sluggingPercentage",
    "onBasePlusSlugging",
    "runsBattedIn",
    "triples",
    "extraBaseHits",
    "hits",
    "atBats",
    "strikeouts",
    "doubles",
    "totalPlateAppearances",
    "intentionalWalks",
    "wins",
    "losses",
    "saves",
    "wildPitch",
    "airOuts",
    "balk",
    "blownSaves",
    "catcherEarnedRunAverage",
    "catchersInterference",
    "chances",
    "completeGames",
    "doublePlays",
    "earnedRun",
    "errors",
    "gamesFinished",
    "gamesStarted",
    "hitBatsman",
    "hitsPer9Inn",
    "holds",
    "innings",
    "inningsPitched",
    "outfieldAssists",
    "passedBalls",
    "pickoffs",
    "pitchesPerInning",
    "putOuts",
    "rangeFactorPerGame",
    "rangeFactorPer9Inn",
    "saveOpportunities",
    "stolenBasePercentage",
    "strikeoutsPer9Inn",
    "strikeoutWalkRatio",
    "throwingErrors",
    "totalBattersFaced",
    "triplePlays",
    "walksPer9Inn",
    "winPercentage",
]


#####################################################################
# Team IDs (MLB — all 30)
#####################################################################

TEAM_IDS: dict[str, int] = {
    "Angels": 108,
    "Astros": 117,
    "Athletics": 133,
    "Blue Jays": 141,
    "Brewers": 158,
    "Braves": 144,
    "Cardinals": 138,
    "Cubs": 112,
    "Dodgers": 119,
    "Giants": 137,
    "Mariners": 136,
    "Mets": 121,
    "Phillies": 143,
    "Rangers": 140,
    "Reds": 113,
    "Orioles": 110,
    "Red Sox": 111,
    "White Sox": 145,
    "Guardians": 114,
    "Tigers": 116,
    "Twins": 142,
    "Yankees": 147,
    "D-backs": 109,
    "Diamondbacks": 109,
    "Rockies": 115,
    "Padres": 135,
    "Marlins": 146,
    "Nationals": 120,
    "Pirates": 134,
    "Rays": 139,
    "Royals": 118,
    # Full market names
    "Los Angeles Angels": 108,
    "Houston Astros": 117,
    "Oakland Athletics": 133,
    "Toronto Blue Jays": 141,
    "Milwaukee Brewers": 158,
    "Atlanta Braves": 144,
    "St. Louis Cardinals": 138,
    "Chicago Cubs": 112,
    "Los Angeles Dodgers": 119,
    "San Francisco Giants": 137,
    "Seattle Mariners": 136,
    "New York Mets": 121,
    "Philadelphia Phillies": 143,
    "Texas Rangers": 140,
    "Cincinnati Reds": 113,
    "Baltimore Orioles": 110,
    "Boston Red Sox": 111,
    "Chicago White Sox": 145,
    "Cleveland Guardians": 114,
    "Detroit Tigers": 116,
    "Minnesota Twins": 142,
    "New York Yankees": 147,
    "Arizona Diamondbacks": 109,
    "Colorado Rockies": 115,
    "San Diego Padres": 135,
    "Miami Marlins": 146,
    "Washington Nationals": 120,
    "Pittsburgh Pirates": 134,
    "Tampa Bay Rays": 139,
    "Kansas City Royals": 118,
}

TEAM_IDS_BY_ABBREV: dict[str, int] = {
    "LAA": 108,
    "HOU": 117,
    "OAK": 133,
    "ATH": 133,
    "TOR": 141,
    "MIL": 158,
    "ATL": 144,
    "STL": 138,
    "CHC": 112,
    "LAD": 119,
    "SF": 137,
    "SFG": 137,
    "SEA": 136,
    "NYM": 121,
    "PHI": 143,
    "TEX": 140,
    "CIN": 113,
    "BAL": 110,
    "BOS": 111,
    "CWS": 145,
    "CHW": 145,
    "CLE": 114,
    "DET": 116,
    "MIN": 142,
    "NYY": 147,
    "ARI": 109,
    "AZ": 109,
    "COL": 115,
    "SD": 135,
    "SDP": 135,
    "MIA": 146,
    "WSH": 120,
    "WAS": 120,
    "PIT": 134,
    "TB": 139,
    "TBR": 139,
    "KC": 118,
    "KCR": 118,
}


#####################################################################
# League / Division IDs
#####################################################################

LEAGUE_IDS: dict[int, str] = {
    103: "American League",
    104: "National League",
    114: "Cactus League",
    115: "Grapefruit League",
}

DIVISION_IDS: dict[int, dict[str, str]] = {
    200: {"name": "AL West", "abbrev": "ALW"},
    201: {"name": "AL East", "abbrev": "ALE"},
    202: {"name": "AL Central", "abbrev": "ALC"},
    203: {"name": "NL West", "abbrev": "NLW"},
    204: {"name": "NL East", "abbrev": "NLE"},
    205: {"name": "NL Central", "abbrev": "NLC"},
}


#####################################################################
# Play event types
#####################################################################

EVENT_TYPES: dict[str, list[str]] = {
    "hits": ["single", "double", "triple", "home_run"],
    "outs": [
        "field_out",
        "strikeout",
        "force_out",
        "grounded_into_double_play",
        "double_play",
        "fielders_choice",
        "fielders_choice_out",
        "strikeout_double_play",
        "triple_play",
        "strikeout_triple_play",
        "grounded_into_triple_play",
        "sac_fly",
        "sac_bunt",
        "sac_fly_double_play",
        "sac_bunt_double_play",
    ],
    "walks_hbp": ["walk", "intent_walk", "hit_by_pitch"],
    "errors": ["field_error", "error"],
    "baserunning": [
        "stolen_base_2b",
        "stolen_base_3b",
        "stolen_base_home",
        "caught_stealing_2b",
        "caught_stealing_3b",
        "caught_stealing_home",
        "pickoff_1b",
        "pickoff_2b",
        "pickoff_3b",
        "pickoff_error_1b",
        "pickoff_error_2b",
        "pickoff_error_3b",
        "pickoff_caught_stealing_2b",
        "pickoff_caught_stealing_3b",
        "pickoff_caught_stealing_home",
        "wild_pitch",
        "passed_ball",
        "balk",
        "other_advance",
        "runner_double_play",
    ],
    "other": [
        "game_advisory",
        "catcher_interf",
        "fan_interference",
        "batter_interference",
        "pitcher_step_off",
        "batter_timeout",
        "mound_visit",
        "no_pitch",
        "other_out",
        "ejection",
    ],
}


#####################################################################
# Transaction codes
#####################################################################

TRANSACTION_CODES: dict[str, str] = {
    "TR": "Trade",
    "SGN": "Signed",
    "SFA": "Signed as Free Agent",
    "DES": "Designated for Assignment",
    "DFA": "Declared Free Agency",
    "REL": "Released",
    "OPT": "Optioned",
    "CU": "Recalled",
    "CLW": "Claimed Off Waivers",
    "PUR": "Purchase",
    "DR": "Drafted",
    "R5": "Rule 5 Selection",
    "RET": "Retired",
    "SC": "Status Change",
    "ASG": "Assigned",
    "OUT": "Outrighted",
    "WA": "Waived",
    "RE": "Reinstated",
    "SU": "Suspension",
    "NC": "New Contract",
    "NUM": "Number Change",
}


#####################################################################
# Statcast / Tracking metrics
#####################################################################

STATCAST_METRICS: list[dict[str, str]] = [
    {"name": "releaseSpinRate", "group": "pitching", "unit": "RPM"},
    {"name": "releaseExtension", "group": "pitching", "unit": "FT"},
    {"name": "releaseSpeed", "group": "pitching", "unit": "MPH"},
    {"name": "effectiveSpeed", "group": "pitching", "unit": "MPH"},
    {"name": "deliveryTime", "group": "pitching", "unit": "SEC"},
    {"name": "launchSpeed", "group": "hitting,pitching", "unit": "MPH"},
    {"name": "launchAngle", "group": "hitting,pitching", "unit": "DEG"},
    {"name": "generatedSpeed", "group": "hitting,pitching", "unit": "MPH"},
    {"name": "distance", "group": "hitting", "unit": "FT"},
    {"name": "travelDistance", "group": "hitting", "unit": "FT"},
    {"name": "hrDistance", "group": "hitting", "unit": "FT"},
    {"name": "maxHeight", "group": "hitting", "unit": "FT"},
    {"name": "travelTime", "group": "hitting", "unit": "SEC"},
    {"name": "hangTime", "group": "hitting", "unit": "SEC"},
    {"name": "launchSpinRate", "group": "hitting", "unit": "RPM"},
    {"name": "barreledBall", "group": "hitting", "unit": "String"},
    {"name": "hitTrajectory", "group": "hitting", "unit": "String"},
    {"name": "homeRunXBallparks", "group": "hitting", "unit": "FT"},
]


#####################################################################
# Environment enums
#####################################################################

HIT_TRAJECTORIES: list[str] = [
    "ground_ball",
    "line_drive",
    "fly_ball",
    "popup",
    "bunt_grounder",
    "bunt_popup",
    "bunt_line_drive",
]

WIND_DIRECTIONS: list[str] = [
    "Calm",
    "In From CF",
    "In From LF",
    "In From RF",
    "L To R",
    "R To L",
    "Out To CF",
    "Out To LF",
    "Out To RF",
    "None",
    "Varies",
]

SKY_CONDITIONS: list[str] = [
    "Clear",
    "Cloudy",
    "Dome",
    "Drizzle",
    "Overcast",
    "Partly Cloudy",
    "Rain",
    "Roof Closed",
    "Snow",
    "Sunny",
]


#####################################################################
# Game statuses
#####################################################################

GAME_STATUSES: list[dict[str, str]] = [
    {"abstract": "Preview", "code": "S", "detailed": "Scheduled"},
    {"abstract": "Preview", "code": "P", "detailed": "Pre-Game"},
    {"abstract": "Live", "code": "PW", "detailed": "Warmup"},
    {"abstract": "Live", "code": "I", "detailed": "In Progress"},
    {"abstract": "Final", "code": "F", "detailed": "Final"},
    {"abstract": "Final", "code": "FT", "detailed": "Final: Tied"},
    {"abstract": "Final", "code": "FR", "detailed": "Final: Completed Early: Rain"},
    {"abstract": "Final", "code": "FG", "detailed": "Final: Game Over"},
    {"abstract": "Preview", "code": "DR", "detailed": "Postponed: Rain"},
    {"abstract": "Preview", "code": "DI", "detailed": "Postponed"},
]


#####################################################################
# Schedule event types
#####################################################################

SCHEDULE_EVENT_TYPES: dict[str, str] = {
    "A": "All-Star Weekend Event",
    "T": "Team Event",
    "E": "Exhibition",
    "Z": "Postseason Games",
    "Y": "Spring Training Games",
    "W": "Pitchers & Catchers Report",
    "X": "Full Squad Reports",
    "B": "Ballpark Tours",
    "D": "Tracking Data Events",
    "O": "Other",
    "C": "Cultural Events",
    "F": "Festival",
    "K": "Kids & Family",
    "M": "Music",
}

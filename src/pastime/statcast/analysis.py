from typing import cast

import numpy as np
import polars as pl


#######################################################################################
# SPIN CONSTANTS


# Environmental constant
K = 0.005383

# Standard gravity in ft/s^2
G = 32.174

# Distance of the pitcher's mound in feet
MOUND_DISTANCE = 60.5

# Distance of the initial Statcast measurement in feet
STATCAST_INITIAL_MEASUREMENT = 50.0

# Number of inches in a foot
INCHES_PER_FOOT = 12.0

# Length of the plate in feet
PLATE_LENGTH = 17.0 / INCHES_PER_FOOT

# Theoretical height of a parallel release point
ECKERSLEY_LINE = 2.85


COMPUTATION_COLUMNS = [
    "release_speed",
    "release_spin_rate",
    "release_extension",
    "release_pos_x",
    "release_pos_z",
    "vx0",
    "vy0",
    "vz0",
    "ax",
    "ay",
    "az",
    "plate_x",
    "plate_z",
]


#######################################################################################
# ARSENAL CONSTANTS


ARSENAL_GROUP_BY_COLUMNS = [
    "pitcher",
    "player_name",
    "game_year",
    "p_throws",
    "pitch_type",
]


ARSENAL_COLUMNS = [
    "pitcher",
    "player_name",
    "game_year",
    "p_throws",
    "pitch_type",
    "release_speed",
    "release_pos_x",
    "release_pos_z",
    "pfx_x",
    "pfx_z",
    "plate_x",
    "plate_z",
    "vx0",
    "vy0",
    "vz0",
    "ax",
    "ay",
    "az",
    "hit_distance_sc",
    "launch_speed",
    "launch_angle",
    "effective_speed",
    "release_spin_rate",
    "release_extension",
    "release_pos_y",
    "estimated_ba_using_speedangle",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
    "babip_value",
    "iso_value",
    "spin_axis",
    "delta_home_win_exp",
    "delta_run_exp",
    "count",
    "usage",
    "release_angle",
    "bauer_units",
    "induced_horz_break",
    "induced_vert_break",
    "decimal_tilt",
    "tilt",
    "spin_direction",
    "spin_efficiency",
    "gyro_angle",
]


#######################################################################################
# PUBLIC FUNCTIONS

# Ahem: "player_name" might not be pitcher but still want it to work.
def pitcher_arsenal(data: pl.DataFrame) -> pl.DataFrame:
    if data.is_empty():
        return data

    # Removes intentional balls
    data = data.filter(pl.col("pitch_type") != "AB")

    grouped = data.groupby(ARSENAL_GROUP_BY_COLUMNS)

    mean = grouped.mean()
    count = grouped.count()

    joined = mean.join(count, on=ARSENAL_GROUP_BY_COLUMNS)

    joined = joined.with_column(
        (pl.col("count") / pl.col("count").sum() * 100).alias("usage")
    )

    if "decimal_tilt" in joined:
        joined = joined.with_column(
            _decimal_tilt_to_tilt(joined["decimal_tilt"]).alias("tilt")
        )

    return joined[[column for column in ARSENAL_COLUMNS if column in joined]]


def spin_columns(data: pl.DataFrame) -> pl.DataFrame:
    if data.is_empty():
        return data

    spin = data[COMPUTATION_COLUMNS]

    if len([s.name for s in spin if s.null_count() != spin.height]) != len(
        spin.columns
    ):
        return data

    spin = _compute_release_angle(spin)
    spin = _compute_bauer_units(spin)

    data = data.with_columns([spin["release_angle"], spin["bauer_units"]])

    return _nathan_fields(data)


def _nathan_fields(data: pl.DataFrame) -> pl.DataFrame:
    nathan = data[COMPUTATION_COLUMNS]

    nathan = _compute_release_point_y(nathan)
    nathan = _compute_release_time(nathan)
    nathan = _compute_velocity_components_at_release(nathan)
    nathan = _compute_flight_time(nathan)
    nathan = _compute_induced_movement(nathan)
    nathan = _compute_average_velocity_components(nathan)
    nathan = _compute_average_velocity(nathan)
    nathan = _compute_average_drag(nathan)
    nathan = _compute_magnus_acceleration_components(nathan)
    nathan = _compute_magnus_acceleration(nathan)
    nathan = _compute_magnus_movement(nathan)
    nathan = _compute_drag_coefficient(nathan)
    nathan = _compute_lift_coefficient(nathan)
    nathan = _compute_spin_factor(nathan)
    nathan = _compute_transverse_spin(nathan)
    nathan = _compute_transverse_spin_components(nathan)
    nathan = _compute_phi(nathan)
    nathan = _compute_tilt_and_spin_direction(nathan)
    nathan = _compute_spin_efficiency(nathan)
    nathan = _compute_gyro_angle(nathan)

    data = data.with_columns(
        [
            nathan["x_mvt"].alias("induced_horz_break"),
            nathan["z_mvt"].alias("induced_vert_break"),
            nathan["decimal_tilt"],
            nathan["tilt"],
            nathan["spin_direction"],
            nathan["spin_efficiency"],
            nathan["theta"].alias("gyro_angle"),
        ]
    )

    return data.fill_nan(None)


#######################################################################################
# COMPUTATION FUNCTIONS


def _compute_release_angle(data: pl.DataFrame) -> pl.DataFrame:
    release_pos_x = data["release_pos_x"]
    release_pos_z = data["release_pos_z"]

    release_angle = np.degrees(
        np.arctan2((release_pos_z + -ECKERSLEY_LINE), abs(release_pos_x))
    )

    return data.with_column(release_angle.alias("release_angle"))


def _compute_bauer_units(data: pl.DataFrame) -> pl.DataFrame:
    release_spin_rate = data["release_spin_rate"]
    release_speed = data["release_speed"]

    bauer_units = release_spin_rate / release_speed

    return data.with_column(bauer_units.alias("bauer_units"))


def _compute_release_point_y(data: pl.DataFrame) -> pl.DataFrame:
    release_extension = data["release_extension"]

    release_pos_y = MOUND_DISTANCE - release_extension

    return data.with_column(release_pos_y.alias("release_pos_y"))


def _compute_release_time(data: pl.DataFrame) -> pl.DataFrame:
    vy0 = data["vy0"]
    ay = data["ay"]
    release_pos_y = data["release_pos_y"]

    tR = _time_in_air(vy0, ay, release_pos_y, STATCAST_INITIAL_MEASUREMENT, False)

    return data.with_column(tR.alias("tR"))


def _compute_velocity_components_at_release(data: pl.DataFrame) -> pl.DataFrame:
    vx0 = data["vx0"]
    vy0 = data["vy0"]
    vz0 = data["vz0"]

    ax = data["ax"]
    ay = data["ay"]
    az = data["az"]

    tR = data["tR"]

    vxR = vx0 + (ax * tR)
    vyR = vy0 + (ay * tR)
    vzR = vz0 + (az * tR)

    return data.with_columns([vxR.alias("vxR"), vyR.alias("vyR"), vzR.alias("vzR")])


def _compute_flight_time(data: pl.DataFrame) -> pl.DataFrame:
    vyR = data["vyR"]
    ay = data["ay"]
    release_pos_y = data["release_pos_y"]

    tf = _time_in_air(vyR, ay, release_pos_y, PLATE_LENGTH, True)

    return data.with_column(tf.alias("tf"))


def _compute_induced_movement(data: pl.DataFrame) -> pl.DataFrame:
    plate_x = data["plate_x"]
    plate_z = data["plate_z"]

    vxR = data["vxR"]
    vyR = data["vyR"]
    vzR = data["vzR"]

    release_pos_x = data["release_pos_x"]
    release_pos_y = data["release_pos_y"]
    release_pos_z = data["release_pos_z"]

    tf = data["tf"]

    x_mvt = -(
        (plate_x - release_pos_x - (vxR / vyR) * (PLATE_LENGTH - release_pos_y))
        * INCHES_PER_FOOT
    )
    z_mvt = (
        plate_z
        - release_pos_z
        - (vzR / vyR) * (PLATE_LENGTH - release_pos_y)
        + (0.5 * G * (tf**2))
    ) * INCHES_PER_FOOT

    return data.with_columns([x_mvt.alias("x_mvt"), z_mvt.alias("z_mvt")])


def _compute_average_velocity_components(data: pl.DataFrame) -> pl.DataFrame:
    vxR = data["vxR"]
    vyR = data["vyR"]
    vzR = data["vzR"]

    ax = data["ax"]
    ay = data["ay"]
    az = data["az"]

    tf = data["tf"]

    vxbar = ((2 * vxR) + (ax * tf)) / 2
    vybar = ((2 * vyR) + (ay * tf)) / 2
    vzbar = ((2 * vzR) + (az * tf)) / 2

    return data.with_columns(
        [vxbar.alias("vxbar"), vybar.alias("vybar"), vzbar.alias("vzbar")]
    )


def _compute_average_velocity(data: pl.DataFrame) -> pl.DataFrame:
    vxbar = data["vxbar"]
    vybar = data["vybar"]
    vzbar = data["vzbar"]

    vbar = _n_component_mean(vxbar, vybar, vzbar)

    return data.with_column(vbar.alias("vbar"))


def _compute_average_drag(data: pl.DataFrame) -> pl.DataFrame:
    ax = data["ax"]
    ay = data["ay"]
    az = data["az"]

    vxbar = data["vxbar"]
    vybar = data["vybar"]
    vzbar = data["vzbar"]

    vbar = data["vbar"]

    adrag = -((ax * vxbar) + (ay * vybar) + ((az + G) * vzbar)) / vbar

    return data.with_column(adrag.alias("adrag"))


def _compute_magnus_acceleration_components(data: pl.DataFrame) -> pl.DataFrame:
    ax = data["ax"]
    ay = data["ay"]
    az = data["az"]

    vxbar = data["vxbar"]
    vybar = data["vybar"]
    vzbar = data["vzbar"]

    adrag = data["adrag"]

    vbar = data["vbar"]

    amagx = ax + (adrag * vxbar / vbar)
    amagy = ay + (adrag * vybar / vbar)
    amagz = az + (adrag * vzbar / vbar) + G

    return data.with_columns(
        [amagx.alias("amagx"), amagy.alias("amagy"), amagz.alias("amagz")]
    )


def _compute_magnus_acceleration(data: pl.DataFrame) -> pl.DataFrame:
    amagx = data["amagx"]
    amagy = data["amagy"]
    amagz = data["amagz"]

    amag = _n_component_mean(amagx, amagy, amagz)

    return data.with_column(amag.alias("amag"))


def _compute_magnus_movement(data: pl.DataFrame) -> pl.DataFrame:
    amagx = data["amagx"]
    amagz = data["amagz"]

    tf = data["tf"]

    Mx = 0.5 * amagx * (tf**2) * INCHES_PER_FOOT
    Mz = 0.5 * amagz * (tf**2) * INCHES_PER_FOOT

    return data.with_columns([Mx.alias("Mx"), Mz.alias("Mz")])


def _compute_drag_coefficient(data: pl.DataFrame) -> pl.DataFrame:
    adrag = data["adrag"]
    vbar = data["vbar"]

    Cd = adrag / ((vbar**2) * K)

    return data.with_column(Cd.alias("Cd"))


def _compute_lift_coefficient(data: pl.DataFrame) -> pl.DataFrame:
    amag = data["amag"]
    vbar = data["vbar"]

    Cl = amag / ((vbar**2) * K)

    return data.with_column(Cl.alias("Cl"))


def _compute_spin_factor(data: pl.DataFrame) -> pl.DataFrame:
    Cl = data["Cl"]

    Cl_adj = (
        pl.when(cast(pl.Expr, Cl >= 0.336)).then(np.nan).otherwise(cast(pl.Expr, Cl))
    )

    S = 0.1666 * np.log(0.336 / (0.336 - Cl_adj))

    return data.with_column(S.alias("S"))


def _compute_transverse_spin(data: pl.DataFrame) -> pl.DataFrame:
    S = data["S"]
    vbar = data["vbar"]

    spinT = 78.92 * S * vbar

    return data.with_column(spinT.alias("spinT"))


def _compute_transverse_spin_components(data: pl.DataFrame) -> pl.DataFrame:
    spinT = data["spinT"]

    vxbar = data["vxbar"]
    vybar = data["vybar"]
    vzbar = data["vzbar"]

    amagx = data["amagx"]
    amagy = data["amagy"]
    amagz = data["amagz"]

    vbar = data["vbar"]

    amag = data["amag"]

    spinTx = spinT * ((vybar * amagz) - (vzbar * amagy)) / (amag * vbar)
    spinTy = spinT * ((vzbar * amagx) - (vxbar * amagz)) / (amag * vbar)
    spinTz = spinT * ((vxbar * amagy) - (vybar * amagx)) / (amag * vbar)

    return data.with_columns(
        [spinTx.alias("spinTx"), spinTy.alias("spinTy"), spinTz.alias("spinTz")]
    )


def _compute_phi(data: pl.DataFrame) -> pl.DataFrame:
    amagz = data["amagz"]
    amagx = data["amagx"]

    arctan = np.arctan2(amagz, -amagx)

    phi = (
        pl.when(cast(pl.Expr, amagz > 0))
        .then(cast(pl.Expr, arctan))
        .otherwise(cast(pl.Expr, 360 + arctan * 180 / np.pi))
    )

    return data.with_column(phi.alias("phi"))


def _compute_tilt_and_spin_direction(data: pl.DataFrame) -> pl.DataFrame:
    phi = data["phi"]

    decimal_tilt = 3 - (1 / 30) * phi
    decimal_tilt_adj = (
        pl.when(cast(pl.Expr, decimal_tilt <= 0))
        .then(decimal_tilt + 12)
        .otherwise(cast(pl.Expr, decimal_tilt))
    )
    data = data.with_column(decimal_tilt_adj.alias("decimal_tilt"))

    tilt = _decimal_tilt_to_tilt(data["decimal_tilt"])
    data = data.with_column(tilt.alias("tilt"))

    spin_direction = _tilt_to_spin_direction(data["tilt"])
    data = data.with_column(spin_direction.alias("spin_direction"))

    return data


# Maybe adjust spin efficiency to match distribution found on "active spin"
# leaderboards?
def _compute_spin_efficiency(data: pl.DataFrame) -> pl.DataFrame:
    spinT = data["spinT"]
    release_spin_rate = data["release_spin_rate"]

    spin_efficiency = spinT / release_spin_rate

    return data.with_column(spin_efficiency.alias("spin_efficiency"))


def _compute_gyro_angle(data: pl.DataFrame) -> pl.DataFrame:
    spin_efficiency = data["spin_efficiency"]

    spin_efficiency_adj = (
        pl.when(cast(pl.Expr, ((1.0 >= spin_efficiency) & (spin_efficiency >= -1.0))))
        .then(spin_efficiency)
        .otherwise(-1.0)
    )

    theta = (
        pl.when(spin_efficiency_adj > 0)
        .then(spin_efficiency_adj.arccos() * 180 / np.pi)
        .otherwise(np.nan)
    )

    return data.with_column(theta.alias("theta"))


#######################################################################################
# HELPER FUNCTIONS


def _time_in_air(
    velocity, acceleration, position, adjustment: float, positive: bool = True
) -> pl.Series:
    if positive:
        direction_factor = 1
    else:
        direction_factor = -1

    return (
        -velocity
        - np.sqrt(
            (velocity**2)
            - (2 * acceleration * (direction_factor * (position - adjustment)))
        )
    ) / acceleration


def _n_component_mean(*args) -> pl.Series:
    return np.sqrt(sum(component**2 for component in args))


def _decimal_tilt_to_tilt(decimal_tilt: pl.Series, minutes_round: int = 5) -> pl.Expr:
    hours = decimal_tilt.floor().cast(int)
    minutes = minutes_round * ((decimal_tilt * 60 % 60) / minutes_round).cast(int)

    hours_adj = (
        pl.when(cast(pl.Expr, minutes == 60))
        .then((hours + 1) % 12)
        .otherwise(cast(pl.Expr, hours))
    )
    minutes_adj = (
        pl.when(cast(pl.Expr, minutes == 60)).then(0).otherwise(cast(pl.Expr, minutes))
    )

    minutes_formatted = minutes_adj.apply("{:0>2}".format)

    return hours_adj.cast(str) + ":" + minutes_formatted.cast(str)


def _tilt_to_spin_direction(tilt: pl.Series) -> pl.Series:
    tilt_time = tilt.str.strptime(pl.Time, fmt="%H:%M")

    hours = tilt_time.dt.hour()
    minutes = tilt_time.dt.minute()

    return ((hours * 30) + (minutes / 2) + 180) % 360

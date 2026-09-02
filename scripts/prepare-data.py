#!/usr/bin/env python

from datetime import datetime, timedelta
from math import tau
from pathlib import Path
from typing import Annotated

import polars as pl
from pydantic import BaseModel, Field
from typer import Exit, Option, Typer

app = Typer()


class TrashnetClass(BaseModel):
    label: str
    folder: Path
    entries: list[Path] = Field(default_factory=list)


@app.command()
def trash(
    input_folder: Annotated[
        Path,
        Option(
            "-i",
            help="Path to folder containing trashnet data.",
        ),
    ] = Path("data/archive/trashnet"),
    output_pat: Annotated[
        str, Option("-o", help="Pattern to the output .csv file.")
    ] = "data/archive/trash-data-{}.csv",
    limit: Annotated[
        int | None, Option(help="Limits the number of samples for small scale tests.")
    ] = None,
) -> None:
    """Prepare trashnet data into .csv format for use in simulations.

    The input folder is expected to contain one subfolder per target class,
    and each target class subfolder should contain only image files.
    """
    classes = read_trash_classes(input_folder)
    if limit is not None:
        limit_entries(classes, limit)
    output_path = Path(output_pat.format("all" if limit is None else str(limit)))
    dump_trash_data(output_path, classes)


def read_trash_classes(input_folder: Path) -> list[TrashnetClass]:
    if not input_folder.is_dir():
        print(f"error: {input_folder} is not a dir!")
        raise Exit(1)

    img_ext = {".jpg", ".jpeg", ".png"}
    return [
        TrashnetClass(
            label=entry.name,
            folder=entry,
            entries=[
                file
                for file in entry.iterdir()
                if file.is_file() and file.suffix in img_ext
            ],
        )
        for entry in input_folder.iterdir()
    ]


def limit_entries(classes: list[TrashnetClass], limit: int) -> None:
    n_per_class = limit // len(classes)
    for cls in classes:
        cls.entries = cls.entries[:n_per_class]


def dump_trash_data(output_file: Path, classes: list[TrashnetClass]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w") as f:
        f.write("image,category\n")
        for cls in classes:
            for entry in cls.entries:
                f.write(f"{str(entry)},{cls.label.capitalize()}\n")


@app.command()
def bikes(
    *,
    input_folder: Annotated[
        Path,
        Option(
            "-i",
            help="Path to folder containing bike data.",
        ),
    ] = Path("data/archive/nyc-citibike"),
    start_time: Annotated[datetime, Option("--start", help="")] = datetime(2024, 1, 1),
    test_threshold: Annotated[datetime, Option("--test", help="")] = datetime(
        2025, 1, 1
    ),
    end_time: Annotated[datetime, Option("--end", help="")] = datetime(2025, 2, 1),
    output_pat: Annotated[
        str, Option("-o", help="Pattern to the output .parquet file.")
    ] = "data/archive/bikes-data-{}.parquet",
    local_tz: Annotated[str, Option("--tz", help="Local time zone")] = "Etc/GMT+4",
) -> None:
    if not start_time < test_threshold < end_time:
        msg = (
            "time parameters should respect start < test < end"
            f" (got {start_time}, {test_threshold}, {end_time})"
        )
        raise ValueError(msg)

    base_df = (
        prepare_bss_events_data(
            input_folder, start_time, test_threshold, end_time, output_pat, local_tz
        )
        .with_columns(hourslot=pl.col("started_at").dt.truncate("1h"))
        .group_by(
            "start_station_id",
            "hourslot",
        )
        .agg(
            demand=pl.len(),
            latitude=pl.first("start_lat"),
            longitude=pl.first("start_lng"),
        )
        .select(
            pl.col("start_station_id").alias("located_at"),
            pl.col("hourslot"),
            pl.col("demand"),
            pl.col("latitude").cast(pl.Float32).alias("latitude"),
            pl.col("longitude").cast(pl.Float32).alias("longitude"),
            pl.col("hourslot")
            .map_elements(is_us_holiday, pl.Boolean)
            .cast(pl.Float32)
            .alias("is_holiday"),
            (pl.col("hourslot").dt.weekday() >= 6).cast(pl.Float32).alias("is_weekend"),
            pl.col("hourslot").dt.month().alias("month"),
            pl.col("hourslot").dt.hour().alias("hour"),
        )
    )

    meteo_df = prepare_meteorological_data(
        input_folder, start_time, test_threshold, end_time, output_pat, local_tz
    )

    df = (
        base_df.join(meteo_df, on="hourslot", coalesce=True)
        .drop("weather_code")
        .filter(pl.all_horizontal(pl.col("*").is_not_null()))
        .collect()
        .lazy()
    )

    cyclical_features = {
        "hour": 24.0,
        "month": 12.0,
        "latitude": 360,
        "longitude": 360,
    }
    categorical_features = {
        # "weather_code": list(range(1, 28)),
    }
    feature_scales = {
        "temperature": 100.0,
        "humidity": 100.0,
        "precipitation": 10.0,
        "wind_speed": 100.0,
        "pressure": 1000.0,
        "cloud_coverage": 8.0,
    }
    if cyclical_features:
        df = transform_cyclical(df, cyclical_features)
    if categorical_features:
        df = transform_categorical(df, categorical_features)
    if feature_scales:
        df = transform_norm(df, feature_scales)
    train_df = df.filter(pl.col("hourslot").dt.date() < test_threshold.date())
    test_df = df.filter(pl.col("hourslot").dt.date() >= test_threshold.date())
    train_df.sink_parquet(output_pat.format("train-all"))
    test_df.sink_parquet(output_pat.format("test-all"))


def transform_cyclical(df: pl.LazyFrame, ft: dict[str, float]) -> pl.LazyFrame:
    return df.with_columns(
        ((pl.col(c) / n) * tau).sin().cast(pl.Float32).alias(c) for c, n in ft.items()
    )


def transform_categorical(df: pl.LazyFrame, ft: dict[str, list[int]]) -> pl.LazyFrame:
    return df.with_columns(
        (pl.col(c) == pl.lit(i)).cast(pl.Float32).alias(f"{c}_{i}")
        for c, ns in ft.items()
        for i in ns
    ).drop(*ft.keys())


def transform_norm(df: pl.LazyFrame, ft: dict[str, float]) -> pl.LazyFrame:
    return df.with_columns((pl.col(c) / s).alias(c) for c, s in ft.items())


def prepare_bss_events_data(
    input_folder: Path,
    start_time: datetime,
    test_threshold: datetime,
    end_time: datetime,
    output_pat: str,
    local_tz: str,
) -> pl.LazyFrame:
    id_cols = ["start_station_id", "end_station_id"]
    df = (
        pl.concat(
            (pl.scan_parquet(f) for f in input_folder.glob("*tripdata*.parquet")),
            how="vertical",
        )
        .with_columns(started_at=pl.col("started_at").dt.replace_time_zone(local_tz))
        .filter(
            pl.col("started_at").dt.date() >= start_time.date(),
            pl.col("started_at").dt.date() < end_time.date(),
            pl.all_horizontal(pl.col(c) != "NULL" for c in id_cols),
            pl.col("start_station_id") != pl.lit("LA Metro Demo 1"),
        )
        .select(
            pl.col("started_at"),
            pl.col("start_station_id"),
            pl.col("start_lat").cast(pl.Float64).alias("start_lat"),
            pl.col("start_lng").cast(pl.Float64).alias("start_lng"),
        )
        .collect()
        .lazy()
    )
    train_df = df.filter(pl.col("started_at").dt.date() < test_threshold.date())
    test_df = df.filter(pl.col("started_at").dt.date() >= test_threshold.date())
    _ = (
        train_df.select(
            pl.col("start_station_id").alias("id"),
            pl.col("start_lat").alias("lat"),
            pl.col("start_lng").alias("lng"),
        )
        .unique("id")
        .join(test_df.select(pl.col("start_station_id").alias("id")).unique(), on="id")
        .sink_parquet(output_pat.format("stations"))
    )
    train_df.sink_parquet(output_pat.format("events-train-all"))
    test_df.sink_parquet(output_pat.format("events-test-all"))
    return df


def prepare_meteorological_data(
    input_folder: Path,
    start_time: datetime,
    test_threshold: datetime,
    end_time: datetime,
    output_pat: str,
    local_tz: str,
) -> pl.LazyFrame:
    schema = [
        ("year", pl.UInt32),
        ("month", pl.UInt32),
        ("day", pl.UInt32),
        ("hour", pl.UInt32),
        ("temp", pl.Float32),
        ("temp_source", pl.String),
        ("rhum", pl.Float32),
        ("rhum_source", pl.String),
        ("prcp", pl.Float32),
        ("prcp_source", pl.String),
        ("wdir", pl.Float32),
        ("wdir_source", pl.String),
        ("wspd", pl.Float32),
        ("wspd_source", pl.String),
        ("wpgt", pl.Float32),
        ("wpgt_source", pl.String),
        ("pres", pl.Float32),
        ("pres_source", pl.String),
        ("cldc", pl.Float32),
        ("cldc_source", pl.String),
        ("coco", pl.UInt8),
        ("coco_source", pl.String),
    ]
    df = (
        pl.concat(
            (
                pl.scan_csv(f, schema=dict(schema))
                for f in input_folder.glob("*weather*.csv")
            ),
            how="vertical",
        )
        .select(
            pl.datetime("year", "month", "day", "hour", 0, 0, 0, time_zone="UTC")
            .dt.convert_time_zone(local_tz)
            .alias("hourslot"),
            pl.col("temp").alias("temperature"),
            pl.col("rhum").alias("humidity"),
            pl.col("prcp").alias("precipitation"),
            pl.col("wspd").alias("wind_speed"),
            pl.col("pres").alias("pressure"),
            pl.col("cldc").alias("cloud_coverage"),
            pl.col("coco").alias("weather_code"),
        )
        .filter(
            pl.col("hourslot").dt.date() >= start_time.date(),
            pl.col("hourslot").dt.date() < end_time.date(),
        )
        .collect()
        .lazy()
    )
    train_df = df.filter(pl.col("hourslot").dt.date() < test_threshold.date())
    test_df = df.filter(pl.col("hourslot").dt.date() >= test_threshold.date())
    train_df.sink_parquet(output_pat.format("weather-train-all"))
    test_df.sink_parquet(output_pat.format("weather-test-all"))
    return df


def is_us_holiday(ts: datetime) -> bool | None:
    if not isinstance(ts, datetime):
        return None

    month = ts.month
    day = ts.day
    weekday = ts.weekday()
    occurrence = (day - 1) // 7
    is_election_day = month == 11 and weekday == 1 and 2 <= day <= 8
    return (
        is_election_day
        or (month, day) in FIXED_HOLIDAYS
        or (month, weekday, occurrence) in NTH_WEEKDAY_HOLIDAYS
        or (is_last_weekday_of_month(ts) and (month, weekday) in LAST_WEEKDAY_HOLIDAYS)
    )


def is_last_weekday_of_month(dt: datetime) -> bool:
    next_dt = dt + timedelta(days=7)
    return next_dt.month != dt.month


FIXED_HOLIDAYS = {
    (1, 1),  # New Year's Day
    (2, 12),  # Lincoln's Birthday
    (6, 19),  # Juneteenth
    (7, 4),  # Independence Day
    (11, 11),  # Veterans Day
    (12, 25),  # Christmas
}

NTH_WEEKDAY_HOLIDAYS = {
    (1, 0, 2),  # MLK Day (3rd Monday of Jan)
    (2, 0, 2),  # Washington's Birthday
    (9, 0, 0),  # Labor Day
    (10, 0, 1),  # Columbus Day
    (11, 3, 3),  # Thanksgiving (4th Thursday)
}

LAST_WEEKDAY_HOLIDAYS = {
    (5, 0),  # Memorial Day (last monday)
}


if __name__ == "__main__":
    app()

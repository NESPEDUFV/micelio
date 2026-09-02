#!/usr/bin/env python

from pathlib import Path
from typing import Annotated

import polars as pl
from typer import Argument, Typer

app = Typer()


@app.command()
def bikes(
    *,
    folder: Annotated[
        Path,
        Argument(help="Path to folder containing bike data."),
    ] = Path("data/archive/nyc-citibike"),
) -> None:
    schema = [
        ("ride_id", pl.String),
        ("rideable_type", pl.String),
        ("started_at", pl.String),
        ("ended_at", pl.String),
        ("start_station_name", pl.String),
        ("start_station_id", pl.String),
        ("end_station_name", pl.String),
        ("end_station_id", pl.String),
        ("start_lat", pl.Float64),
        ("start_lng", pl.Float64),
        ("end_lat", pl.Float64),
        ("end_lng", pl.Float64),
        ("member_casual", pl.String),
    ]
    dt_cols = ["started_at", "ended_at"]

    for csv in folder.glob("*tripdata*.csv"):
        print(csv)
        pl.scan_csv(csv, schema=dict(schema)).with_columns(
            pl.col(c).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f").alias(c)
            for c in dt_cols
        ).sink_parquet(csv.with_suffix(".parquet"))


if __name__ == "__main__":
    app()

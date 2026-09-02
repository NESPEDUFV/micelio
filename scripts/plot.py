#!/usr/bin/env python
import re
from dataclasses import dataclass
from datetime import datetime
from math import isnan
from pathlib import Path
from types import TracebackType
from typing import Annotated, Any, Self

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pydantic import BaseModel
from typer import Argument, Option, Typer


class Plot:
    fig: Figure | None
    ax: Axes | None

    def __init__(
        self,
        out: Path,
        nrows: int = 1,
        ncols: int = 1,
        figsize: tuple[int, int] = (6, 4),
        dpi: int = 200,
    ):
        self.out = out
        self.figsize = figsize
        self.nrows = nrows
        self.ncols = ncols
        self.dpi = dpi

    def __enter__(self) -> tuple[Figure, Axes | list[Axes]]:
        fig, ax = plt.subplots(
            nrows=self.nrows,
            ncols=self.ncols,
            figsize=self.figsize,
            dpi=self.dpi,
            constrained_layout=True,
        )
        self.fig = fig
        self.ax = ax
        return fig, ax

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        if exc_val:
            return
        match (self.nrows, self.ncols):
            case (1, 1):
                self.ax.grid(alpha=0.3)
            case (1, _) | (_, 1):
                for ax in self.ax:
                    ax.grid(alpha=0.3)
            case _:
                for axs in self.ax:
                    for ax in axs:
                        ax.grid(alpha=0.3)

        self.out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(self.out, bbox_inches="tight")
        plt.clf()


@dataclass(kw_only=True)
class MetricData:
    n_edge_trash_nodes: int
    n_edge_bikes_nodes: int
    bytes_df: pl.DataFrame
    latency_df: pl.DataFrame
    accuracy_df: pl.DataFrame
    mse_df: pl.DataFrame
    training_t_df: pl.DataFrame
    task_t_df: pl.DataFrame


sns.set_theme("paper")
app = Typer()

START_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[(\w+)App\](?:\[(\w+)\])? start",
    flags=re.IGNORECASE,
)
BYTES_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[metrics/Connection\]\[peer=([\d.]+):\d+\] (sent|received) bytes: (\d+)",
    flags=re.IGNORECASE,
)
LATENCY_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[metrics/Connection\]\[peer=([\d.]+):\d+\] latency \(s\): ([\d.]+)",
    flags=re.IGNORECASE,
)
TRAINING_T_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[metrics/FlClient\] training time \(s\): ([\d.]+)",
    flags=re.IGNORECASE,
)
ACCURACY_PAT = re.compile(
    r"\[([-:.\d ]*?) UTC\] \[FlAlgorithm\]\[trash:TrashCategorization\]\[round #(\d+)\] global metric: ([\d.]+)",
    flags=re.IGNORECASE,
)
MSE_PAT = re.compile(
    r"\[([-:.\d ]*?) UTC\] \[FlAlgorithm\]\[bikes:BikeShareTask\]\[round #(\d+)\] global metric: ([\d.]+)",
    flags=re.IGNORECASE,
)
START_TIME = datetime(2025, 1, 1, 0, 0, 0)  # noqa: DTZ001
TASK_TRIGGER_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[UserApp\] task triggered", flags=re.IGNORECASE
)
TASK_FINISH_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[UserApp\]\[\w+\] task finished",
    flags=re.IGNORECASE,
)
INIT_FINISH_PAT = re.compile(
    r"^\[\s*(\d+)\]\[([-:.\d ]*?) UTC\] \[FogApp\] sim:FogNode\d+ finish",
    flags=re.IGNORECASE,
)

TRASH_CL_MAP = {
    "cardboard": "papelão",
    "glass": "vidro",
    "metal": "metal",
    "paper": "papel",
    "plastic": "plástico",
    "trash": "outros",
}


def safe_strptime(dt_str: str) -> datetime:
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")  # noqa: DTZ007
    except ValueError:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")  # noqa: DTZ007
    return dt


def timestamp_to_delta(dt_str: str) -> float:
    return (safe_strptime(dt_str) - START_TIME).total_seconds()


@app.command()
def sim_metrics(
    inputs: Annotated[list[Path], Argument(help="Paths to log or csv files.")],
    out: Annotated[Path, Option(help="Path to output dir")] = "data/img",
    bikes_actual_data: Annotated[
        Path, Option("--bikes-test", help="Path to actual bike test data.")
    ] = "data/archive/bikes-data-s2-test-all.parquet",
) -> None:
    out.mkdir(parents=True, exist_ok=True)
    logs = (f for f in inputs if f.suffix == ".log")
    trash_parquets = (
        f
        for f in inputs
        if (f.suffix == ".parquet" or f.suffix == ".csv") and "trash" in f.name
    )
    bikes_parquets = (f for f in inputs if f.suffix == ".parquet" and "bikes" in f.name)
    data = [read_logs(log) for log in logs]
    trash_preds_df = pl.concat(map(read_trash_outputs, trash_parquets))
    bikes_preds_df = pl.concat(map(read_bikes_outputs, bikes_parquets))
    bytes_df = pl.concat(d.bytes_df for d in data if not d.bytes_df.is_empty())
    latency_df = pl.concat(d.latency_df for d in data if not d.latency_df.is_empty())
    _training_t_df = pl.concat(
        d.training_t_df for d in data if not d.training_t_df.is_empty()
    )
    accuracy_df = pl.concat(d.accuracy_df for d in data if not d.accuracy_df.is_empty())
    mse_df = pl.concat(d.mse_df for d in data if not d.mse_df.is_empty())
    task_t_df = pl.concat(d.task_t_df for d in data if not d.task_t_df.is_empty())
    plot_bytes(out, bytes_df)
    # tabulate_bytes(out, bytes_df)
    plot_latency(out, latency_df)
    tabulate_latency(out, latency_df)
    tabulate_fed_cent_latency(out, latency_df)
    plot_fed_cent_latency(out, latency_df)
    plot_task_time(out, task_t_df)
    plot_accuracy(out, accuracy_df)
    plot_mse(out, mse_df)
    trash_cl_metrics = classification_metrics(trash_preds_df, list(TRASH_CL_MAP))
    print(metrics_to_latex(trash_cl_metrics))
    plot_test_accuracy(out, trash_preds_df)
    plot_test_bikes_metrics(out, bikes_preds_df, bikes_actual_data)


def read_logs(log: Path) -> MetricData:
    bytes_data = []
    latency_data = []
    accuracy_data = []
    mse_data = []
    training_time_data = []
    task_time_data = []
    n_tasks = 2
    n_trash = 0
    n_bikes = 0
    nodes_by_layer = {}
    tasks_t0: dict[str, datetime] = {}
    init_finished_at = None
    is_centralized = "baseline" in log.name

    with log.open() as logs:
        while line := logs.readline():
            if m := START_PAT.search(line):
                node_id, _, layer, app_type = m.groups()
                node_id = int(node_id)
                layer = layer.lower()
                if layer == "edge":
                    if app_type == "bikes":
                        n_bikes += 1
                    elif app_type == "trash":
                        n_trash += 1
                if layer == "user":
                    layer = "edge"
                nodes_by_layer[node_id] = (layer, app_type)
            elif m := BYTES_PAT.search(line):
                node_id, dt, ip, direction, nbytes = m.groups()
                node_id = int(node_id)
                (from_layer, _) = nodes_by_layer.get(node_id)
                bytes_data.append((dt, from_layer, ip, direction, int(nbytes)))
            elif m := INIT_FINISH_PAT.search(line):
                _node_id, dt = m.groups()
                if init_finished_at is None:
                    init_finished_at = safe_strptime(dt[:26])
            elif m := LATENCY_PAT.search(line):
                node_id, dt, ip, latency = m.groups()
                node_id = int(node_id)
                (from_layer, _) = nodes_by_layer.get(node_id)
                latency_data.append((dt, from_layer, ip, float(latency)))
            elif m := TRAINING_T_PAT.search(line):
                node_id, dt, ttime = m.groups()
                node_id = int(node_id)
                (_, app_type) = nodes_by_layer.get(node_id)
                training_time_data.append((app_type, dt, float(ttime)))
            elif m := ACCURACY_PAT.search(line):
                dt, i, acc = m.groups()
                accuracy_data.append((dt, int(i), float(acc)))
            elif m := MSE_PAT.search(line):
                dt, i, mse = m.groups()
                mse_data.append((dt, int(i), float(mse)))
            elif m := TASK_TRIGGER_PAT.search(line):
                node_id, dt = m.groups()
                node_id = int(node_id)
                (_, app_type) = nodes_by_layer.get(node_id)
                tasks_t0[app_type] = datetime.strptime(dt[:26], "%Y-%m-%d %H:%M:%S.%f")  # noqa: DTZ007
            elif m := TASK_FINISH_PAT.search(line):
                node_id, dt = m.groups()
                node_id = int(node_id)
                (_, app_type) = nodes_by_layer.get(node_id)
                t0 = tasks_t0[app_type]
                tf = datetime.strptime(dt[:26], "%Y-%m-%d %H:%M:%S.%f")  # noqa: DTZ007
                task_time_data.append((app_type, (tf - t0).total_seconds()))
                n_tasks -= 1
                if n_tasks == 0:
                    break

    bytes_df = prepare_df(
        pl.DataFrame(
            bytes_data,
            orient="row",
            schema=["t", "from_layer", "ip", "direction", "bytes"],
        ),
        n_trash,
        n_bikes,
        is_centralized,
    )
    bytes_df = assign_layer(bytes_df)
    bytes_df = add_layers_label(bytes_df)
    bytes_df = mark_post_init(bytes_df, init_finished_at)

    latency_df = prepare_df(
        pl.DataFrame(
            latency_data, orient="row", schema=["t", "from_layer", "ip", "latency"]
        ),
        n_trash,
        n_bikes,
        is_centralized,
    )
    latency_df = assign_layer(latency_df)
    latency_df = add_layers_label(latency_df)
    latency_df = mark_post_init(latency_df, init_finished_at)

    training_t_df = prepare_df(
        pl.DataFrame(training_time_data, orient="row", schema=["app", "t", "time"]),
        n_trash,
        n_bikes,
    )

    task_t_df = prepare_df(
        pl.DataFrame(task_time_data, orient="row", schema=["app", "time"]),
        n_trash,
        n_bikes,
    )

    accuracy_df = prepare_df(
        pl.DataFrame(accuracy_data, orient="row", schema=["t", "round", "accuracy"]),
        n_trash,
        n_bikes,
    )

    mse_df = prepare_df(
        pl.DataFrame(mse_data, orient="row", schema=["t", "round", "mse"]),
        n_trash,
        n_bikes,
    )

    return MetricData(
        n_edge_trash_nodes=n_trash,
        n_edge_bikes_nodes=n_bikes,
        bytes_df=bytes_df,
        latency_df=latency_df,
        training_t_df=training_t_df,
        accuracy_df=accuracy_df,
        mse_df=mse_df,
        task_t_df=task_t_df,
    )


def read_trash_outputs(p: Path) -> pl.DataFrame:
    n = int(re.search(r"t(\d+)", p.name).group(1))
    df = pl.read_csv(p) if p.suffix == ".csv" else pl.read_parquet(p)
    df = df.with_columns(
        prob=pl.col("prob").cast(pl.Float64),
        actual=pl.col("actual").str.replace(r"\d+$", ""),
    )
    return df.with_columns(n=pl.lit(n))


def read_bikes_outputs(p: Path) -> pl.DataFrame:
    n = int(re.search(r"b(\d+)", p.name).group(1))
    df = pl.read_parquet(p)
    df = df.select(
        pl.coalesce(
            pl.col("bss")
            .str.split("#")
            .list.get(1, null_on_oob=True)
            .str.strip_suffix(">"),
            pl.col("bss").str.strip_prefix("bikes:"),
        ).alias("bss"),
        "hour_slot",
        (pl.col("demand") if "demand" in df.columns else pl.col("predicted"))
        .cast(pl.Float32)
        .alias("predicted"),
        pl.lit(n).alias("n"),
    )
    return df


def assign_layer(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        to_layer=pl.when(pl.col("ip").str.starts_with("10.42"))
        .then(pl.lit("cloud"))
        .when(pl.col("ip").str.starts_with("10.23"))
        .then(pl.lit("fog"))
        .when(pl.col("ip").str.starts_with("192"))
        .then(pl.lit("edge"))
        .otherwise(pl.lit("other"))
    )


def prepare_df(
    df: pl.DataFrame, n_edge_trash: int, n_edge_bikes: int, is_centralized: bool = False
) -> pl.DataFrame:
    if "t" in df.columns and df.schema.get("t").is_(pl.String):
        df = df.with_columns(dt=pl.col("t").str.strptime(pl.Datetime)).with_columns(
            t=(pl.col("dt") - START_TIME).dt.total_microseconds() * 1e-6
        )
    return df.with_columns(
        n_edge_trash=pl.lit(n_edge_trash),
        n_edge_bikes=pl.lit(n_edge_bikes),
        is_centralized=pl.lit(is_centralized),
    ).with_columns(
        case=pl.format(
            "$N_{{ET}} = {}$\n$N_{{EB}} = {}$",
            pl.col("n_edge_trash"),
            pl.col("n_edge_bikes"),
        )
    )


def add_layers_label(df: pl.DataFrame) -> pl.DataFrame:
    def rename_layer(c: str) -> pl.Expr:
        return pl.col(c).str.slice(0, 1).str.to_uppercase()

    return df.with_columns(
        layers=pl.concat_list(rename_layer("from_layer"), rename_layer("to_layer"))
        .list.sort()
        .list.join("-")
    )


def mark_post_init(df: pl.DataFrame, at: datetime) -> pl.DataFrame:
    return df.with_columns(post_init=pl.col("dt") >= at)


def out_suffix(post_init: bool, centralized: bool) -> str:
    s = ""
    if not post_init:
        s += "-pre-treino"
    if centralized:
        s += "-centralizado"
    return s


def plot_bytes(out: Path, df: pl.DataFrame) -> None:
    df_filtered = df.filter(pl.col("post_init") & ~pl.col("is_centralized"))
    data = (
        df_filtered.group_by("case", "layers")
        .agg(pl.sum("bytes"), pl.len().alias("n_requests"))
        .with_columns(
            pl.col("case").alias("Case"),
            pl.col("layers").alias("Layers"),
            pl.col("bytes").alias("bytes"),
            pl.col("n_requests"),
        )
    )
    layers = sorted(c for (c,) in data.select("Layers").unique().iter_rows())
    cases = sorted(c for (c,) in data.select("Case").unique().iter_rows())

    with Plot(out / "bytes-trafegados-agrupado.png", figsize=(6, 4)) as (
        _,
        ax,
    ):
        sns.barplot(
            data=data.with_columns(pl.col("bytes") * 1e-6).to_pandas(),
            x="Case",
            y="bytes",
            hue="Layers",
            ax=ax,
            order=cases,
            hue_order=layers,
        )
        ax.set_ylabel("Transmitted data (MB)")

    with Plot(out / "bytes-trafegados-medio.png", figsize=(6, 4)) as (
        _,
        ax,
    ):
        sns.barplot(
            data=data.with_columns(
                ((pl.col("bytes") * 1e-3) / pl.col("n_requests")).alias("avg_bytes")
            ).to_pandas(),
            x="Case",
            y="avg_bytes",
            hue="Layers",
            ax=ax,
            order=cases,
            hue_order=layers,
        )
        ax.set_ylabel("Average transmitted data (KB/request)")


def plot_latency(out: Path, df: pl.DataFrame) -> None:
    df_filtered = df.filter(pl.col("post_init") & ~pl.col("is_centralized"))

    data = (
        df_filtered.group_by("case", "layers")
        .agg(pl.sum("latency"), pl.len().alias("n_requests"))
        .select(
            pl.col("case").alias("Case"),
            pl.col("layers").alias("Layers"),
            pl.col("latency"),
            pl.col("n_requests"),
        )
    )
    ungrouped_data = df_filtered.with_columns(
        pl.col("case").alias("Case"),
        pl.col("layers").alias("Layers"),
        pl.col("latency"),
    )
    cases: list[str] = sorted(data["Case"].unique())
    layers: list[str] = sorted(data["Layers"].unique())

    with Plot(out / "latencia-rtt-agrupado-simples.png", figsize=(6, 4)) as (_, ax):
        sns.barplot(
            data.to_pandas(),
            x="Case",
            y="latency",
            hue="Layers",
            order=cases,
            hue_order=layers,
            ax=ax,
        )
        for container in ax.containers:
            ax.bar_label(container, fmt='%.0f', rotation=0, padding=2, fontsize=6)
        plt.ylabel("RTT latency (s)")

    with Plot(out / "latencia-rtt-dist.png", figsize=(6, 4)) as (
        _,
        ax,
    ):
        sns.barplot(
            ungrouped_data.to_pandas(),
            x="Case",
            y="latency",
            errorbar=("pi", 50),
            capsize=0.1,
            hue="Layers",
            order=cases,
            hue_order=layers,
            ax=ax,
        )
        plt.ylabel("RTT latency (s)")

    palette = sns.color_palette()

    with Plot(
        out / "latencia-rtt-ao-tempo.png",
        nrows=len(cases),
        figsize=(6, 3 * len(cases)),
    ) as (_, axs_):
        axs: list[Axes] = axs_
        for i, (case, ax) in enumerate(zip(cases, axs)):
            sns.lineplot(
                data=df.filter(pl.col("case") == case).to_pandas(),
                x="t",
                y="latency",
                color=palette[i],
                ax=ax,
            )
            ax.set_xlabel("Simulation time (s)" if i + 1 == len(cases) else None)
            ax.set_ylabel(f"RTT latency (s)\n{case.replace('\n', ', ')}")


def tabulate_latency(out: Path, df: pl.DataFrame) -> None:
    df_filtered = df.filter(pl.col("post_init") & ~pl.col("is_centralized"))
    df_selected = df_filtered.select(
        pl.col("n_edge_trash").alias("$N_{ET}$"),
        pl.col("n_edge_bikes").alias("$N_{EB}$"),
        pl.col("layers").alias("Layers"),
        pl.col("latency"),
    )
    per_layer_df = df_selected.group_by("$N_{ET}$", "$N_{EB}$", "Layers").agg(
        pl.col("latency").len().alias("No. Req."),
        pl.col("latency").sum().alias("Sum"),
        pl.col("latency").mean().alias("Avg."),
        pl.col("latency").std().alias("Std. Dev."),
    )
    total_df = (
        df_selected.group_by("$N_{ET}$", "$N_{EB}$")
        .agg(
            pl.col("latency").len().alias("No. Req."),
            pl.col("latency").sum().alias("Sum"),
            pl.col("latency").mean().alias("Avg."),
            pl.col("latency").std().alias("Std. Dev."),
        )
        .with_columns(pl.lit("Total").alias("Layers"))
    )
    df_to_latex_table(
        pl.concat([per_layer_df, total_df], how="diagonal").sort(
            "$N_{ET}$", "$N_{EB}$", "Layers"
        ),
        caption="RTT latency during federated training.",
        label="tab:fed-latency",
        grouped_by=2,
    )
    print()


def tabulate_fed_cent_latency(out: Path, df: pl.DataFrame) -> None:
    df_filtered = df.filter(~pl.col("post_init") & (pl.col("layers") == pl.lit("C-E")))
    df_selected = df_filtered.select(
        pl.when(pl.col("is_centralized"))
        .then(
            pl.format(
                "Centralized, {}",
                (pl.col("case").str.replace("\n", ", ", literal=True)),
            )
        )
        .otherwise(
            pl.format(
                "Federated, {}", (pl.col("case").str.replace("\n", ", ", literal=True))
            )
        )
        .alias("Case"),
        pl.col("latency"),
    )
    total_df = df_selected.group_by("Case").agg(
        pl.col("latency").len().alias("No. Req."),
        pl.col("latency").sum().alias("Sum"),
        pl.col("latency").mean().alias("Avg."),
        pl.col("latency").std().alias("Std. Dev."),
    )
    df_to_latex_table(
        total_df.sort("Case"),
        caption="RTT latency for publishing context before training.",
        label="tab:fed-cent-latency",
    )
    print()


def plot_fed_cent_latency(out: Path, df: pl.DataFrame) -> None:
    df_filtered = df.filter(~pl.col("post_init") & (pl.col("layers") == pl.lit("C-E")))
    df_selected = df_filtered.select(
        pl.when(pl.col("is_centralized"))
        .then(pl.format("Centralized\n{}", (pl.col("case"))))
        .otherwise(pl.format("Federated\n{}", (pl.col("case"))))
        .alias("Case"),
        pl.col("latency"),
    )
    cases = sorted(df_selected["Case"], key=lambda c: ("Cent" in c, c))

    with Plot(out / "latencia-rtt-dist-fed-cent-comp.png", figsize=(6, 4)) as (_, ax):
        sns.barplot(
            df_selected.to_pandas(),
            x="Case",
            y="latency",
            errorbar=("pi", 50),
            capsize=0.1,
            order=cases,
            ax=ax,
        )
        plt.ylabel("RTT latency (s)")


def plot_training_time(out: Path, df: pl.DataFrame) -> None:
    ttdata = df.group_by("case", "app").agg(pl.sum("time").alias("training_time"))
    cases: list[str] = sorted(ttdata["case"].unique())
    with Plot(out / "tempo-treinamento.png", figsize=(6, 4)):
        sns.barplot(
            ttdata.select(
                pl.col("case").alias("Case"),
                pl.when(pl.col("app") == pl.lit("bikes"))
                .then(pl.lit("Bikes"))
                .when(pl.col("app") == pl.lit("trash"))
                .then(pl.lit("Trash"))
                .alias("Application"),
                pl.col("training_time"),
            ).to_pandas(),
            x="Case",
            y="training_time",
            hue="Application",
            order=cases,
        )
        plt.ylabel("Total training time (s)")


def plot_latency_with_subgroups(
    out: Path, ldf: pl.DataFrame, ttdf: pl.DataFrame
) -> None:
    data = ldf.group_by("case", "layers").agg(
        pl.sum("latency"), pl.len().alias("n_requests")
    )
    part_data = data.with_columns(is_ef=pl.col("layers") == "E-F").partition_by(
        "is_ef", as_dict=True
    )
    ef_data = part_data[True,].drop("is_ef")
    non_ef_data = part_data[False,].drop("is_ef")
    ttdata = ttdf.group_by("case", "app").agg(pl.sum("time").alias("training_time"))
    discounted = ef_data.join(
        ttdata.group_by("case").agg(pl.sum("training_time")), on="case", validate="1:1"
    ).select(
        "case",
        "layers",
        (pl.col("latency") - pl.col("training_time")).alias("latency"),
    )
    data = pl.concat(
        [
            non_ef_data.with_columns(tag=pl.lit("Others")).drop("n_requests"),
            discounted.with_columns(tag=pl.lit("Others")),
            ttdata.select(
                pl.col("case"),
                pl.lit("E-F").alias("layers"),
                pl.col("training_time").alias("latency"),
                pl.format("Training ({})", pl.col("app")).alias("tag"),
            ),
        ],
        how="vertical",
    ).with_columns(
        pl.col("case").alias("Case"),
        pl.col("layers").alias("Layers"),
        pl.col("tag").alias("Usage"),
        pl.col("latency"),
    )

    x_col = "Case"
    y_col = "latency"
    group_col = "Layers"
    subgroup_col = "Usage"
    stacked_group = "E-F"

    with Plot(out / "latencia-rtt-agrupado.png", figsize=(6, 4)) as (_, ax):
        grouped_stacked_barplot(
            data, ax, x_col, y_col, group_col, subgroup_col, stacked_group
        )
        ax.set_ylabel("RTT latency (s)")
        legend = ax.legend()
        legend.set_title("Layers")


def grouped_stacked_barplot(
    data: pl.DataFrame,
    ax: Axes,
    x_col: str,
    y_col: str,
    group_col: str,
    subgroup_col: str,
    stacked_group: str,
) -> None:
    cases = sorted(c for (c,) in data.select(x_col).unique().iter_rows())
    groups = sorted(c for (c,) in data.select(group_col).unique().iter_rows())
    n_groups = len(groups)
    n_cases = len(cases)
    width = 0.8 / n_groups
    xpos = np.arange(n_cases)
    for i, group in enumerate(groups):
        x = xpos + (i - (n_groups - 1) / 2) * width
        group_df = data.filter(pl.col(group_col) == group).to_pandas()

        if group == stacked_group:
            subgroups = sorted(sg for sg in group_df[subgroup_col].unique())

            bottom = np.zeros(n_cases)

            for sg in subgroups:
                vals = (
                    group_df[group_df[subgroup_col] == sg]
                    .set_index(x_col)[y_col]
                    .reindex(cases, fill_value=0)
                    .values
                )
                ax.bar(x, vals, width, bottom=bottom, label=f"{group}, {sg}")
                bottom += vals
        else:
            vals = group_df.set_index(x_col)[y_col].reindex(cases, fill_value=0).values
            ax.bar(x, vals, width, label=group)
    ax.set_xticks(xpos)
    ax.set_xticklabels(cases)
    ax.set_xlabel(x_col)


def plot_task_time(out: Path, df: pl.DataFrame) -> None:
    cases: list[str] = sorted(df["case"].unique())
    data = df.select(
        pl.when(pl.col("app") == pl.lit("bikes"))
        .then(pl.lit("Bikes"))
        .when(pl.col("app") == pl.lit("trash"))
        .then(pl.lit("Trash"))
        .alias("Application"),
        pl.col("time"),
        pl.col("case").alias("Case"),
    )
    with Plot(out / "tempo-tarefa.png", figsize=(3, 4)):
        sns.barplot(
            data.to_pandas(), x="Case", y="time", hue="Application", order=cases
        )
        plt.ylabel("Time until task conclusion (s)")


@app.command()
def bss(
    *,
    events_input: Annotated[list[Path], Option(help="Path to events input.")] = [],  # noqa: B006
    out: Annotated[Path, Option(help="Path to output dir")] = "data/img",
) -> None:
    events_input = events_input or [
        "data/archive/bikes-data-events-test-all.parquet",
        "data/archive/bikes-data-events-train-all.parquet",
    ]
    plot_bikes_stations(out / "bss.png", events_input)


@app.command()
def bikes_baseline(
    out: Annotated[Path, Option(help="Path to output dir")] = "data/img",
) -> None:
    plot_bikes_baseline(out / "baseline-bikes.png")


def plot_accuracy(out: Path, df: pl.DataFrame) -> None:
    data = df.with_columns(
        pl.format("{}", pl.col("n_edge_trash")).alias("$N_{ET}$"),
        (pl.col("round") + 1).alias("round"),
    )
    with Plot(out / "acuracia.png", figsize=(6, 4)) as (_, ax):
        sns.lineplot(
            data=data.to_pandas(),
            x="round",
            y="accuracy",
            hue="$N_{ET}$",
            ax=ax,
        )
        plt.yticks([0.1 * i for i in range(11)], [f"{i * 10}%" for i in range(11)])
        plt.ylim(0.0, 1.0)
        plt.xlabel("Algorithm iteration")
        plt.ylabel("Global training accuracy")


def plot_mse(out: Path, df: pl.DataFrame) -> None:
    data = df.with_columns(
        pl.format("{}", pl.col("n_edge_bikes")).alias("$N_{EB}$"),
        (pl.col("round") + 1).alias("round"),
        pl.col("mse").sqrt().alias("rmse"),
    )
    with Plot(out / "mse.png", figsize=(4, 4)) as (_, ax):
        sns.lineplot(
            data=data.to_pandas(),
            x="round",
            y="rmse",
            hue="$N_{EB}$",
            ax=ax,
        )
        plt.xlabel("Algorithm iteration")
        plt.ylabel("Global training RMSE")


def plot_test_accuracy(out: Path, df: pl.DataFrame) -> None:
    data = (
        df.select(
            pl.col("n"), (pl.col("actual") == pl.col("predicted")).cast(int).alias("ok")
        )
        .group_by("n")
        .agg(pl.sum("ok").alias("ok"), pl.len().alias("total"))
        .select(
            pl.col("n").cast(str).alias("$N_E$"),
            (pl.col("ok") / pl.col("total")).alias("accuracy"),
        )
    )
    with Plot(out / "acuracia-teste.png", figsize=(6, 4)) as (_, ax):
        ax = sns.barplot(
            data=data.to_pandas(),
            x="$N_E$",
            hue="$N_E$",
            y="accuracy",
            ax=ax,
        )
        for container in ax.containers:
            ax.bar_label(container, fontsize=10, fmt=lambda x: f"{x * 100.0:.2f}%")
        plt.yticks([0.1 * i for i in range(11)], [f"{i * 10}%" for i in range(11)])
        plt.ylim(0.0, 1.0)
        plt.xlabel("Edge node amount ($N_{ET}$)")
        plt.ylabel("Global test accuracy")


def plot_test_bikes_metrics(
    out: Path,
    df: pl.DataFrame,
    bikes_actual_p: Path,
    year: int = 2024,
    time_range: str = "2024/S2",
    folds: int = 8,
) -> None:
    baseline_paths = list(Path("data/archive").glob("baseline-bikes*/metrics.parquet"))
    labels = BikesLabel.from_paths(baseline_paths, year)
    baseline_df = get_bikes_baseline_df(baseline_paths, labels).filter(
        (pl.col("Time range") == time_range) & (pl.col("Folds") == folds)
    )
    df = df.select(
        pl.col("bss"),
        pl.col("hour_slot").dt.convert_time_zone("Etc/GMT+4"),
        pl.col("predicted"),
        pl.col("n").alias("case"),
    )
    actual_df = pl.read_parquet(bikes_actual_p).select(
        pl.col("located_at").str.replace_all(" +", "").alias("bss"),
        pl.col("hourslot").dt.cast_time_unit("ms").alias("hour_slot"),
        pl.col("demand").cast(pl.Float32).alias("actual"),
    )
    df = df.join(actual_df, on=["bss", "hour_slot"])
    metrics_df = (
        df.group_by("case")
        .agg(
            # n=pl.len(),
            ((pl.col("predicted") - pl.col("actual")) ** 2).mean().sqrt().alias("RMSE"),
            ((pl.col("predicted") - pl.col("actual")).abs()).mean().alias("MAE"),
            (
                (
                    (pl.col("predicted") - pl.col("actual")).abs()
                    / pl.col("actual").abs()
                )
                * 100
            )
            .mean()
            .alias("MAPE"),
        )
        .sort("case")
    )
    metrics = ["RMSE", "MAE", "MAPE"]
    rows = [" & ".join(["Caso", *metrics])]
    avgs = [
        baseline_df.select(pl.col("RMSE").mean()).item(),
        baseline_df.select(pl.col("MAE").mean()).item(),
        baseline_df.select(pl.col("MAPE").mean()).item(),
    ]
    for n, rmse, mae, mape in metrics_df.iter_rows():
        rows.append(f"{n} & {rmse:.3f} & {mae:.3f} & {mape:.2f}\\%")
    joined_rows = " \\\\\n".join(rows)
    metrics_table = rf"""
\begin{{table}}[tbp]
\centering
\begin{{tabular}}{{r|r|r|r}}
{joined_rows} \\
\end{{tabular}}
\caption{{Métricas de teste para previsão de demanda de bicicletas.}}
\label{{tab:bikes-metrics}}
\end{{table}}
"""
    print(metrics_table)

    with Plot(out / "bikes-test-metrics.png", ncols=3, figsize=(8, 4)) as (_, _axes):
        axes: list[Axes] = _axes
        for i, (metric, ax, avg) in enumerate(zip(metrics, axes, avgs)):
            suffix = "%" if metric == "MAPE" else ""
            ax.axhline(y=avg, color="r", linestyle="-")
            axhline_label = f"Avg. c. {metric} = {avg:.2f}{suffix}"
            offset = 0.1 if metric == "MAPE" else 0.05
            ax.text(
                0.98,
                avg * (1 - offset),
                axhline_label,
                transform=ax.get_yaxis_transform(),
                ha="right",
                va="center",
                bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "none"},
            )
            sns.barplot(
                data=metrics_df.with_columns(
                    pl.col("case").cast(pl.String)
                ).to_pandas(),
                x="case",
                hue="case",
                y=metric,
                ax=ax,
                width=0.3,
                legend=False,
            )
            for container in ax.containers:
                ax.bar_label(container, fontsize=10, fmt=lambda x: f"{x:.2f}{suffix}")  # noqa: B023
            if metric == "MAPE":
                ax.set_ylim(0.0, 100.0)
            ax.set_xlabel(
                "Edge node amount ($N_{EB}$)" if i == len(metrics) // 2 else ""
            )
            ax.set_ylabel(f"Global test {metric}")


def classification_metrics(
    df: pl.DataFrame,
    categories: list[str],
    case_col: str = "n",
    actual_col: str = "actual",
    predicted_col: str = "predicted",
) -> pl.DataFrame:
    metrics = []
    for (case,) in df.select(case_col).unique().iter_rows():
        case_df = df.filter(pl.col(case_col) == pl.lit(case))
        for cls in categories:
            tp = case_df.filter(
                (pl.col(actual_col) == cls) & (pl.col(predicted_col) == cls)
            ).height

            fp = case_df.filter(
                (pl.col(actual_col) != cls) & (pl.col(predicted_col) == cls)
            ).height

            fn = case_df.filter(
                (pl.col(actual_col) == cls) & (pl.col(predicted_col) != cls)
            ).height

            precision = tp / (tp + fp) if tp + fp > 0 else 0.0
            recall = tp / (tp + fn) if tp + fn > 0 else 0.0

            f1 = (
                2 * precision * recall / (precision + recall)
                if precision + recall > 0
                else 0.0
            )

            metrics.append(
                {
                    "case": case,
                    "category": cls,
                    "tp": tp,
                    "fp": fp,
                    "fn": fn,
                    "precision": precision,
                    "recall": recall,
                    "f1": f1,
                }
            )

    return pl.DataFrame(metrics)


def metrics_to_latex(df: pl.DataFrame) -> str:
    cases = sorted(df["case"].unique().to_list())
    categories = sorted(df["category"].unique().to_list())
    lookup = {
        (r["category"], r["case"]): (
            r["precision"],
            r["recall"],
            r["f1"],
        )
        for r in df.iter_rows(named=True)
    }

    n_cases = len(cases)

    lines = [
        r"\begin{table}[tbp]",
        r"\centering",
        rf"\begin{{tabular}}{{l|{'ccc|' * (n_cases - 1)}ccc}}",
        r"\hline",
    ]

    header = [""]

    for i, case in enumerate(cases):
        sep = "|" if i < n_cases - 1 else ""
        header.append(rf"\multicolumn{{3}}{{c{sep}}}{{\textbf{{{case}}}}}")

    lines.append(" & ".join(header) + r" \\")

    header = [r"\textbf{Categoria}"]
    header.extend([r"\textbf{P}", r"\textbf{R}", r"\textbf{F1}"] * len(cases))
    lines.append(" & ".join(header) + r" \\")
    lines.append(r"\hline")

    for category in categories:
        row = [TRASH_CL_MAP[str(category)]]
        for case in cases:
            p, r, f1 = lookup[(category, case)]
            row.extend(
                [
                    f"{p:.3f}",
                    f"{r:.3f}",
                    f"{f1:.3f}",
                ]
            )
        lines.append(" & ".join(row) + r" \\")

    lines.extend(
        [
            r"\hline",
            r"\end{tabular}",
            r"\caption{Métricas de teste para classificação de lixo.}",
            r"\label{tab:trash-metrics}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


class BikesLabel(BaseModel):
    folds: int
    time_range: str

    @classmethod
    def from_path(cls, p: Path, year: int) -> Self:
        label = BikesLabel(folds=1, time_range=str(year))
        name = p.parent.name.removeprefix("baseline-bikes").removeprefix("-")
        for part in name.split("-"):
            if part.startswith(("q", "s")):
                label.time_range = f"{year}/{part.upper()}"
            elif part.endswith("folds"):
                label.folds = int(part.removesuffix("folds"))
        return label

    @classmethod
    def from_paths(cls, paths: list[Path], year: int) -> Self:
        return [BikesLabel.from_path(p, year) for p in paths]


def get_bikes_baseline_df(paths: list[Path], labels: list[BikesLabel]) -> pl.DataFrame:
    return (
        pl.concat(
            pl.scan_parquet(p).with_columns(
                time_range=pl.lit(label.time_range), n_folds=label.folds
            )
            for label, p in zip(labels, paths)
        )
        .select(
            pl.col("time_range").alias("Time range"),
            pl.col("n_folds").alias("Folds"),
            pl.col("rmse").alias("RMSE"),
            pl.col("mae").alias("MAE"),
            pl.col("mape").alias("MAPE"),
        )
        .collect()
    )


def plot_bikes_baseline(out: Path, year: int = 2024) -> None:
    baseline_paths = list(Path("data/archive").glob("baseline-bikes*/metrics.parquet"))
    labels = BikesLabel.from_paths(baseline_paths, year)
    df = get_bikes_baseline_df(baseline_paths, labels).to_pandas()
    order = [
        str(year),
        *(f"{year}/S{i}" for i in range(1, 3)),
        *(f"{year}/Q{i}" for i in range(1, 5)),
    ]
    present_ranges = {lb.time_range for lb in labels}
    order = [o for o in order if o in present_ranges]
    with Plot(out, figsize=(15, 4), ncols=3) as (_, (ax1, ax2, ax3)):
        kwargs = {
            "data": df,
            "x": "Time range",
            "hue": "Folds",
            "order": order,
            "palette": "muted",
        }
        sns.boxplot(**kwargs, y="RMSE", ax=ax1)
        sns.boxplot(**kwargs, y="MAE", ax=ax2)
        sns.boxplot(**kwargs, y="MAPE", ax=ax3)


def plot_bikes_stations(out: Path, events_paths: list[Path]) -> None:
    stations_df = (
        pl.concat([pl.scan_parquet(e) for e in events_paths])
        .group_by("start_station_id")
        .agg(
            pl.len().alias("demand"),
            pl.first("start_lat").alias("lat"),
            pl.first("start_lng").alias("lng"),
        )
        .collect()
    )
    stations_df_p = stations_df.to_pandas()
    gdf = gpd.GeoDataFrame(
        stations_df_p,
        geometry=gpd.points_from_xy(stations_df_p["lng"], stations_df_p["lat"]),
        crs="EPSG:4326",
    ).to_crs(epsg=3857)
    weather_st = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy([-73.9692], [40.7789]), crs="EPSG:4326"
    ).to_crs(epsg=3857)
    with Plot(out, figsize=(5, 5)) as (_, ax):
        gdf.plot(
            ax=ax,
            markersize=10,
            alpha=0.5,
            column="demand",
            legend=True,
            legend_kwds={"label": "Total demand", "orientation": "vertical"},
            cmap="viridis",
        )
        ax.scatter(
            weather_st.geometry.x,
            weather_st.geometry.y,
            marker="D",
            s=30,
            c="red",
            edgecolors="black",
            label="Meteorological station",
            zorder=10,
        )
        ax.legend(loc="upper right")
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)
        plt.xlabel("")
        plt.ylabel("")
        ax.set_xticks([])
        ax.set_yticks([])


def df_to_latex_table(
    df: pl.DataFrame,
    *,
    caption: str,
    label: str,
    decimals: int = 2,
    grouped_by: int = 0,
) -> None:
    float_pat = f"{{:.{decimals}f}}"
    alignments = ["r" if dtype.is_numeric() else "l" for dtype in df.dtypes]
    col_spec = "|".join(alignments)
    header = " & ".join([f"\\textbf{{{col}}}" for col in df.columns])

    lines = []
    lines.append("\\begin{table}[htbp]")
    lines.append("\\centering")
    lines.append("\\begin{tabular}{" + col_spec + "}")
    lines.append(header + " \\\\")
    lines.append("\\hline")

    prev_group = None

    def fmt_cell(i: int, val: Any, show: bool) -> str:
        if (
            val is None
            or (isinstance(val, float) and isnan(val))
            or (not show and i < grouped_by)
        ):
            return ""
        if isinstance(val, float):
            return float_pat.format(val)
        return str(val)

    for row in df.iter_rows():
        current_group = tuple(row[:grouped_by]) if grouped_by else None
        if prev_group != current_group or grouped_by == 0:
            prev_group = current_group
            show = True
        else:
            show = False
        row_strs = (fmt_cell(i, val, show) for i, val in enumerate(row))
        lines.append(" & ".join(row_strs) + " \\\\")

    lines.append("\\end{tabular}")
    lines.append(f"\\caption{{{caption}}}")
    lines.append(f"\\label{{{label}}}")
    lines.append("\\end{table}")
    print("\n".join(lines))


if __name__ == "__main__":
    app()

#!/usr/bin/env python
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Annotated

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
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
            nrows=self.nrows, ncols=self.ncols, figsize=self.figsize, dpi=self.dpi
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
    n_edge_nodes: int
    bytes_df: pl.DataFrame
    latency_df: pl.DataFrame
    accuracy_df: pl.DataFrame


sns.set_theme("paper")
app = Typer()

BYTES_PAT = re.compile(
    r"\[([-:.\d ]*?) UTC\] \[metrics/Connection\]\[peer=([\d.]+):\d+\] (sent|received) bytes: (\d+)",
    flags=re.IGNORECASE,
)
LATENCY_PAT = re.compile(
    r"\[([-:.\d ]*?) UTC\] \[metrics/Connection\]\[peer=([\d.]+):\d+\] latency \(s\): ([\d.]+)",
    flags=re.IGNORECASE,
)
ACCURACY_PAT = re.compile(
    r"\[([-:.\d ]*?) UTC\] \[FlAlgorithm\] round #(\d+) global accuracy: ([\d.]+)",
    flags=re.IGNORECASE,
)
START_TIME = datetime(2026, 1, 1, 0, 0, 0)


def timestamp_to_delta(dt_str: str) -> float:
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return (dt - START_TIME).total_seconds()


@app.command()
def trash(
    logs_or_csvs: Annotated[list[Path], Argument(help="Paths to log or csv files.")],
    out: Annotated[Path, Option(help="Path to output dir")] = "data/img",
) -> None:
    out.mkdir(parents=True, exist_ok=True)
    logs = (f for f in logs_or_csvs if f.suffix == ".log")
    csvs = (f for f in logs_or_csvs if f.suffix == ".csv")
    data = [read_logs(log) for log in logs]
    test_acc_df = pl.concat(map(read_csv, csvs))
    bytes_df = pl.concat(d.bytes_df for d in data)
    latency_df = pl.concat(d.latency_df for d in data)
    accuracy_df = pl.concat(d.accuracy_df for d in data)
    plot_bytes(out, bytes_df)
    plot_latency(out, latency_df)
    plot_accuracy(out, accuracy_df)
    plot_test_accuracy(out, test_acc_df)


def read_logs(log: Path) -> MetricData:
    bytes_data = []
    latency_data = []
    accuracy_data = []
    with log.open() as logs:
        n = 0
        while line := logs.readline():
            if "[EdgeApp] start" in line:
                n += 1
            elif m := BYTES_PAT.search(line):
                dt, ip, direction, nbytes = m.groups()
                bytes_data.append((dt, ip, direction, int(nbytes)))
            elif m := LATENCY_PAT.search(line):
                dt, ip, latency = m.groups()
                latency_data.append((dt, ip, float(latency)))
            elif m := ACCURACY_PAT.search(line):
                dt, i, acc = m.groups()
                accuracy_data.append((dt, int(i), float(acc)))

    bytes_df = assign_layer(
        prepare_df(
            pl.DataFrame(
                bytes_data, orient="row", schema=["t", "ip", "direction", "bytes"]
            ),
            n,
        )
    )

    latency_df = assign_layer(
        prepare_df(
            pl.DataFrame(latency_data, orient="row", schema=["t", "ip", "latency"]), n
        )
    )

    accuracy_df = prepare_df(
        pl.DataFrame(accuracy_data, orient="row", schema=["t", "round", "accuracy"]), n
    )

    return MetricData(
        n_edge_nodes=n,
        bytes_df=bytes_df,
        latency_df=latency_df,
        accuracy_df=accuracy_df,
    )


def read_csv(csv: Path) -> pl.DataFrame:
    n = int(re.search(r"(\d+)", csv.name).group(1))
    return pl.read_csv(csv).with_columns(n=pl.lit(n))


def assign_layer(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        layer=pl.when(pl.col("ip").str.starts_with("10.42"))
        .then(pl.lit("cloud"))
        .when(pl.col("ip").str.starts_with("10.23"))
        .then(pl.lit("fog"))
        .when(pl.col("ip").str.starts_with("7."))
        .then(pl.lit("edge"))
        .otherwise(pl.lit("other"))
    )


def prepare_df(df: pl.DataFrame, n_edge: int) -> pl.DataFrame:
    return df.with_columns(
        t=(pl.col("t").str.strptime(pl.Datetime) - START_TIME).dt.total_microseconds()
        * 1e-6,
        n_edge=pl.lit(n_edge),
    )


def plot_bytes(out: Path, df: pl.DataFrame) -> None:
    ns: list[int] = sorted(list(df["n_edge"].unique()))
    palette = sns.color_palette()
    data = df.with_columns(
        pl.format("{}", pl.col("n_edge")).alias("$N_E$"),
        (pl.col("bytes") * 1e-3).alias("bytes"),
        ((pl.col("t") / 5.0).floor() * 5).alias("t"),
    )

    with Plot(out / "bytes-trafegados-agrupado.png", figsize=(6, 4)) as (_, ax):
        sns.lineplot(
            data=data.to_pandas(),
            x="t",
            y="bytes",
            hue="$N_E$",
            ax=ax,
        )
        ax.set_xlabel("Tempo de simulação (s)")
        ax.set_ylabel("Dados trafegados (KB)")

    with Plot(
        out / "bytes-trafegados.png",
        nrows=len(ns),
        figsize=(6, 3 * len(ns)),
    ) as (_, axs_):
        axs: list[Axes] = axs_
        for i, (n, ax) in enumerate(zip(ns, axs)):
            sns.lineplot(
                data=data.filter(pl.col("$N_E$") == str(n)).to_pandas(),
                x="t",
                y="bytes",
                color=palette[i],
                ax=ax,
            )
            ax.set_xlabel("Tempo de simulação (s)" if i + 1 == len(ns) else None)
            ax.set_ylabel(f"Dados trafegados (KB), $N_E = {n}$")


def plot_latency(out: Path, df: pl.DataFrame) -> None:
    ns: list[int] = sorted(list(df["n_edge"].unique()))
    palette = sns.color_palette()
    data = df.with_columns(
        pl.format("{}", pl.col("n_edge")).alias("$N_E$"),
    )
    with Plot(out / "latencia-rtt-agrupado.png", figsize=(6, 4)) as (_, ax):
        sns.lineplot(
            data=data.to_pandas(),
            x="t",
            y="latency",
            hue="$N_E$",
            ax=ax,
        )
        plt.xlabel("Tempo de simulação (s)")
        plt.ylabel("Latência de RTT (s)")

    with Plot(
        out / "latencia-rtt.png",
        nrows=len(ns),
        figsize=(6, 3 * len(ns)),
    ) as (_, axs_):
        axs: list[Axes] = axs_
        for i, (n, ax) in enumerate(zip(ns, axs)):
            sns.lineplot(
                data=data.filter(pl.col("$N_E$") == str(n)).to_pandas(),
                x="t",
                y="latency",
                color=palette[i],
                ax=ax,
            )
            ax.set_xlabel("Tempo de simulação (s)" if i + 1 == len(ns) else None)
            ax.set_ylabel(f"Latência de RTT (s), $N_E = {n}$")


def plot_accuracy(out: Path, df: pl.DataFrame) -> None:
    data = df.with_columns(
        pl.format("{}", pl.col("n_edge")).alias("$N_E$"),
        (pl.col("round") + 1).alias("round"),
    )
    with Plot(out / "acuracia.png", figsize=(6, 4)) as (_, ax):
        sns.lineplot(
            data=data.to_pandas(),
            x="round",
            y="accuracy",
            hue="$N_E$",
            ax=ax,
        )
        plt.yticks([0.1 * i for i in range(11)], [f"{i * 10}%" for i in range(11)])
        plt.ylim(0.0, 1.0)
        plt.xlabel("Iteração do algoritmo")
        plt.ylabel("Acurácia de treinamento global")


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
        plt.xlabel("Quantidade de nós da borda ($N_E$)")
        plt.ylabel("Acurácia de teste global")


if __name__ == "__main__":
    app()

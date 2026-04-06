#!/usr/bin/env python
from pathlib import Path
from typing import Annotated, TypeVar

import polars as pl
import requests
from typer import Argument, Option, Typer

app = Typer()


@app.command()
def main(
    label: Annotated[str, Argument(help="Label to name the output file")],
    out: Annotated[Path, Option(help="Path to output dir")] = "data/archive",
) -> None:
    endpoint = "http://localhost:3030/dataset/query"
    query = """
SELECT ?item ?category ?prob
WHERE {
  [] a mcl:CategorizedImage;
    mcl:represents ?item;
    mcl:predictProbability ?prob;
    mcl:category ?category;
    .
}
""".strip()
    df = run_query(endpoint, query)
    df = df.with_columns(
        item=pl.col("item").str.extract(r"\w+:([a-z]+)"),
        category=pl.col("category").str.to_lowercase().str.strip_prefix("trash:"),
    )
    df.select(
        pl.col("item").alias("actual"),
        pl.col("category").alias("predicted"),
        pl.col("prob"),
    ).write_csv(out / f"{label}.csv")


PREFIXES = {
    "owl": "http://www.w3.org/2002/07/owl#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "tl": "http://purl.org/NET/c4dm/timeline.owl#",
    "mcl": "http://nesped1.caf.ufv.br/micelio/ontology#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "qu": "http://purl.oclc.org/NET/ssnx/qu/qu#",
    "unit": "http://purl.oclc.org/NET/ssnx/qu/unit#",
    "tlc": "http://gessi.lsi.upc.edu/threelevelcontextmodelling/ThreeLContextOnt/UpperLevelOntology#",
    "trash": "http://nesped1.caf.ufv.br/micelio/simulation/trash#",
    "dim": "http://purl.oclc.org/NET/ssnx/qu/dim#",
}

QUERY_HEADER = "\n".join(f"PREFIX {k}: <{v}>" for k, v in PREFIXES.items())


def run_query(endpoint: str, query: str) -> pl.DataFrame:
    query = "\n".join((QUERY_HEADER, query))
    response = requests.get(endpoint, params={"query": query})
    if response.status_code != 200:
        msg = f"\n{query}\n\n{response.content.decode()}"
        raise ValueError(msg)
    body = response.json()
    df = pl.DataFrame(
        (
            {k: fmt_prefix(v["value"]) for k, v in bind.items()}
            for bind in body["results"]["bindings"]
        ),
        schema=body["head"]["vars"],
    )
    return df


T = TypeVar("T")


def fmt_prefix(iri: T) -> T:
    if not isinstance(iri, str):
        return iri
    for prefix, prefix_iri in PREFIXES.items():
        if iri.startswith(prefix_iri):
            return f"{prefix}:{iri.removeprefix(prefix_iri)}"
    return iri


if __name__ == "__main__":
    app()

#!/usr/bin/env python

import random
import re
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field
from typer import Exit, Option, Typer

app = Typer()


class TrashnetClass(BaseModel):
    label: str
    folder: Path
    entries: list[Path] = Field(default_factory=list)


@app.command()
def trash(
    n_nodes: Annotated[
        int, Option("-n", help="Number of edge nodes in the test case.")
    ],
    input_folder: Annotated[
        Path,
        Option(
            "-i",
            help="Path to folder containing trashnet data.",
        ),
    ] = Path("data/archive/trashnet"),
    model_file: Annotated[
        Path, Option("-m", "--model", help="Path to the model .ttl file.")
    ] = Path("data/simulation/trash-model.ttl"),
    output_pat: Annotated[
        str, Option("-o", help="Pattern to the output .ttl file.")
    ] = "data/archive/trash-data-{}.ttl",
    seed: Annotated[int, Option(help="Random seed for reproducibility.")] = 42,
    limit: Annotated[
        int | None, Option(help="Limits the number of samples for small scale tests.")
    ] = None,
) -> None:
    """Prepare trashnet data into .ttl format for use in simulations.

    The input folder is expected to contain one subfolder per target class,
    and each target class subfolder should contain only image files.
    """
    random.seed(seed)
    classes = read_trash_classes(input_folder)
    if limit is not None:
        limit_entries(classes, limit)
    edit_trash_model(model_file, classes)
    dump_trash_data(Path(output_pat.format(n_nodes)), n_nodes, classes)


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


def edit_trash_model(model_file: Path, classes: list[TrashnetClass]) -> None:
    text = model_file.read_text()
    data = "\n".join(
        f"""trash:{cls.label.capitalize()} a trash:TrashCategory.""" for cls in classes
    )
    new_text = re.sub(
        r"#\s*region\s+edit(.*?)#\s*endregion\s+edit",
        f"# region edit\n{data}\n# endregion edit",
        text,
        flags=re.DOTALL,
    )
    model_file.write_text(new_text)


def dump_trash_data(
    output_file: Path, n_nodes: int, classes: list[TrashnetClass]
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w") as f:
        f.write("""BASE <http://nesped1.caf.ufv.br/micelio/simulation/trash>
PREFIX : <http://nesped1.caf.ufv.br/micelio/simulation/trash#>
PREFIX sim: <http://nesped1.caf.ufv.br/micelio/simulation#>
PREFIX mcl: <http://nesped1.caf.ufv.br/micelio/ontology#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX qu: <http://purl.oclc.org/NET/ssnx/qu/qu#>
PREFIX unit: <http://purl.oclc.org/NET/ssnx/qu/unit#>
PREFIX tlc: <http://gessi.lsi.upc.edu/threelevelcontextmodelling/ThreeLContextOnt/UpperLevelOntology#>

""")

        for cls in classes:
            for entry in cls.entries:
                f.write(
                    f"""
[] a :CategorizedTrashImage, sim:CollectedContext;
    sim:byNode {random.randint(0, n_nodes - 1)};
    :image {str(entry)!r};
    :category :{cls.label.capitalize()};
    .
""".strip()
                )
                f.write("\n")


@app.command()
def bikes() -> None:
    raise NotImplementedError("coming soon!")


if __name__ == "__main__":
    app()

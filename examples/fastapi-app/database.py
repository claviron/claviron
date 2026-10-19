"""Read the shared, pre-migrated demo file using this process's own connection."""

from contextlib import closing
import os
from pathlib import Path
import sqlite3


def database_summary() -> dict:
    path = Path(os.environ.get("EXAMPLES_DATABASE", "../database/demo.sqlite"))
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    # No automatic creation or migrations while serving requests.
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as database:
        database.row_factory = sqlite3.Row
        row = database.execute(
            "select sqlite_version() as version, current_timestamp as now, "
            "(select count(*) from users) as users, "
            "(select count(*) from todos) as todos, "
            "(select count(*) from demo_records) as records"
        ).fetchone()
        return {"database": "demo.sqlite", **dict(row)}

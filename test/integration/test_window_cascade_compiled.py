#!/usr/bin/env python3
"""Forced view-delta cascade from DuckLake window partition refreshes.

openivm_compile_with_facts with force_view_delta_cascade asks a WINDOW_PARTITION refresh to
write its own signed row changes into openivm_delta_<view>. This runs the compiled programs
of the three DuckLake window paths (single-source change feed, joined lineage change feed,
result diff under a nested LIMIT) after a batch of conflicting DML. It then checks that:

- each view matches its query (EXCEPT ALL in both directions), and
- the emitted delta, as a Z-set, equals the new rows minus the old rows.

The trailing consumer cleanup is not run: no downstream view is registered, so it would
clear the delta that is being checked.

Needs the DuckLake extension in the build repository; it is skipped without it, unless
--require is passed.

Usage: test_window_cascade_compiled.py <duckdb CLI with OpenIVM linked> [--require]
"""

import csv
import io
import subprocess
import sys
import tempfile
from pathlib import Path

VIEWS = {
    "cwd_single": "SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val, id) AS rn " "FROM dl.cwd_src",
    "cwd_join": "SELECT s.id, s.grp, s.val, d.w, ROW_NUMBER() OVER (PARTITION BY s.grp ORDER BY s.val, s.id) AS rn "
    "FROM dl.cwd_src s JOIN dl.cwd_dim d ON s.grp = d.grp",
    "cwd_limit": "SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val, id) AS rn "
    "FROM (SELECT * FROM dl.cwd_src ORDER BY id LIMIT 4) t",
}
# A marker of the path each view must compile to.
PATH_MARKERS = {
    "cwd_single": "ducklake_table_insertions('dl', 'main', 'cwd_src'",
    "cwd_join": "ducklake_table_insertions('dl', 'main', 'cwd_dim'",
    "cwd_limit": "openivm_window_current_",
}
FACTS = '{"target_dialect":"duckdb","compile_only":true,"force_view_delta_cascade":true}'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_duckdb(binary: Path, database: Path, sql: str) -> str:
    result = subprocess.run(
        [str(binary), "-unsigned", str(database), "-csv", "-noheader"],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 or "Error" in result.stderr:
        raise RuntimeError(f"DuckDB failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: test_window_cascade_compiled.py <duckdb binary> [--require]")
    binary = Path(sys.argv[1]).resolve()
    repository = binary.parent / "repository"
    if not list(repository.glob("*/*/ducklake.duckdb_extension")):
        if "--require" in sys.argv[2:]:
            raise SystemExit(f"DuckLake extension not found under {repository}")
        print(f"SKIP: no DuckLake extension under {repository}; window cascade integration test not run")
        return

    with tempfile.TemporaryDirectory(prefix="openivm_window_cascade_") as temp_dir:
        root = Path(temp_dir)
        database = root / "test.db"
        preamble = (
            f"INSTALL ducklake FROM {sql_literal(str(repository))}; LOAD ducklake;\n"
            f"ATTACH {sql_literal('ducklake:' + str(root / 'lake.ducklake'))} AS dl "
            f"(DATA_PATH {sql_literal(str(root / 'data'))});\n"
        )
        setup = (
            preamble
            + """
CREATE TABLE dl.cwd_src (id INT, grp VARCHAR, val INT);
CREATE TABLE dl.cwd_dim (grp VARCHAR, w INT);
INSERT INTO dl.cwd_src VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',1);
INSERT INTO dl.cwd_dim VALUES ('a',1),('b',2),('c',3);
"""
        )
        for view, query in VIEWS.items():
            setup += f"CREATE MATERIALIZED VIEW dl.{view} AS {query};\n"
        setup += """
INSERT INTO dl.cwd_src VALUES (6,'a',5),(7,'c',9),(8,'d',2);
UPDATE dl.cwd_src SET val = 30 WHERE id = 1;
DELETE FROM dl.cwd_src WHERE id IN (3, 8);
UPDATE dl.cwd_src SET grp = 'c' WHERE id = 4;
UPDATE dl.cwd_dim SET w = 7 WHERE grp = 'a';
"""
        run_duckdb(binary, database, setup)

        for view, query in VIEWS.items():
            names = run_duckdb(
                binary,
                database,
                f"SELECT view_name FROM openivm_views WHERE view_sql_name = {sql_literal(view)};",
            )
            data_table = f"dl.main.openivm_data_{names}"
            delta_table = f"dl.main.openivm_delta_{names}"
            old_rows = root / f"{view}_old.parquet"
            program = run_duckdb(
                binary,
                database,
                preamble
                + f"COPY (FROM {data_table}) TO {sql_literal(str(old_rows))};\n"
                + "SELECT sql FROM openivm_compile_with_facts("
                + f"{sql_literal(view)}, {sql_literal(FACTS)}) WHERE stmt_kind = 'data' ORDER BY stmt_order;",
            )
            statements = [row[0] for row in csv.reader(io.StringIO(program)) if row]
            if not any(PATH_MARKERS[view] in statement for statement in statements):
                raise AssertionError(f"{view}: expected the path marked by {PATH_MARKERS[view]!r}")
            if not any(statement.startswith(f"INSERT INTO {delta_table}") for statement in statements):
                raise AssertionError(f"{view}: the forced cascade program writes no view delta")
            body = [
                statement
                for statement in statements
                if not (statement.startswith(f"DELETE FROM {delta_table}") and "NOT EXISTS" in statement)
            ]
            run_duckdb(binary, database, preamble + "\n".join(body) + "\n")

            result = run_duckdb(
                binary,
                database,
                preamble
                + f"""
CREATE TEMP VIEW expected AS {query};
SELECT
    (SELECT count(*) FROM ((FROM {data_table} EXCEPT ALL FROM expected)
                           UNION ALL (FROM expected EXCEPT ALL FROM {data_table}))),
    (SELECT count(*) FROM (
        SELECT * EXCLUDE (m), sum(m) FROM (
            SELECT * EXCLUDE (openivm_multiplicity, openivm_timestamp), openivm_multiplicity AS m FROM {delta_table}
            UNION ALL BY NAME SELECT *, -1 AS m FROM expected
            UNION ALL BY NAME SELECT *, 1 AS m FROM read_parquet({sql_literal(str(old_rows))})
        ) GROUP BY ALL HAVING sum(m) <> 0)),
    (SELECT count(*) FROM {delta_table});
""",
            )
            data_diff, delta_diff, delta_rows = result.split(",")
            if data_diff != "0":
                raise AssertionError(f"{view}: {data_diff} rows differ from the view query")
            if delta_diff != "0" or delta_rows == "0":
                raise AssertionError(
                    f"{view}: view delta ({delta_rows} rows) differs from new minus old by {delta_diff} rows"
                )
        print("PASS")


if __name__ == "__main__":
    main()

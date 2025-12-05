import os
from typing import List, Dict, Tuple
import duckdb
import pandas as pd
from contextlib import contextmanager

def qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def _safe_checkpoint(con: duckdb.DuckDBPyConnection) -> None:
    """
    Try the modern procedure-style checkpoint first, then fall back to PRAGMA.
    Never raise on failure (cleanup should be best-effort).
    """
    try:
        con.execute("CALL checkpoint()")
    except Exception:
        try:
            con.execute("PRAGMA checkpoint")
        except Exception:
            # Swallow: checkpoint is optional hygiene; don't kill the process
            pass

class DuckDBWriter:
    """
    Upserts cleaned DataFrames into DuckDB with PK and simple schema evolution.
    Opens a connection only when needed, checkpoints, and closes cleanly so the WAL clears.
    """

    def __init__(self, db_path: str = "/data/gold.duckdb"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_meta()

    # ----- connection helper -----
    @contextmanager
    def _conn(self, read_only: bool = False):
        con = duckdb.connect(self.db_path, read_only=read_only)
        try:
            yield con
        finally:
            # Best-effort fold WAL -> DB, then close, never raising out of __exit__
            _safe_checkpoint(con)
            try:
                con.close()
            except Exception:
                pass

    def _init_meta(self):
        with self._conn() as con:
            con.execute("""
                CREATE TABLE IF NOT E(
                    corr_id TEXT PRIMARY KEY,
                    table_name TEXT,
                    row_count BIGINT,
                    processed_at TIMESTAMP DEFAULT now()
                );
            """)

    # ---------- schema helpers ----------
    def _table_info(self, con, table: str) -> List[Tuple[int, str, str, bool, str, int]]:
        return con.execute(f"PRAGMA table_info({qi(table)})").fetchall()

    def _existing_cols(self, con, table: str) -> Dict[str, str]:
        return {row[1]: row[2] for row in self._table_info(con, table)}

    def _infer_df_types(self, con, df: pd.DataFrame) -> Dict[str, str]:
        con.register("clean_df_schema", df.head(0))
        con.execute("CREATE TEMP TABLE _incoming_schema AS SELECT * FROM clean_df_schema")
        rows = con.execute("PRAGMA table_info('_incoming_schema')").fetchall()
        inferred = {r[1]: r[2] for r in rows}
        con.execute("DROP TABLE _incoming_schema")
        con.unregister("clean_df_schema")
        return inferred

    def _ensure_table(self, con, table: str, df: pd.DataFrame, pk: str):
        con.register("clean_df_schema", df.head(0))
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {qi(table)} AS
            SELECT * FROM clean_df_schema;
        """)
        con.unregister("clean_df_schema")
        try:
            con.execute(f"ALTER TABLE {qi(table)} ADD CONSTRAINT {qi('pk_' + table)} PRIMARY KEY ({qi(pk)});")
        except duckdb.CatalogException:
            pass
        except duckdb.ConversionException:
            pass

    def _evolve_schema(self, con, table: str, df: pd.DataFrame):
        existing = self._existing_cols(con, table)
        needed = [c for c in df.columns if c not in existing]
        if not needed:
            return
        inferred = self._infer_df_types(con, df)
        for col in needed:
            col_type = inferred.get(col, "TEXT")
            con.execute(f"ALTER TABLE {qi(table)} ADD COLUMN {qi(col)} {col_type}")

    # ---------- upsert ----------
    def upsert(self, table: str, df: pd.DataFrame, pk: str = "crash_record_id") -> int:
        if df.empty:
            return 0
        if pk not in df.columns:
            raise ValueError(f"Primary key column {pk!r} not present in incoming DataFrame.")

        with self._conn() as con:
            self._ensure_table(con, table, df, pk)
            self._evolve_schema(con, table, df)

            con.register("clean_df", df)

            existing = set(self._existing_cols(con, table).keys())
            cols = [c for c in df.columns if c in existing]
            if pk not in cols:
                cols.append(pk)

            non_pk = [c for c in cols if c != pk]

            insert_cols   = ", ".join(qi(c) for c in cols)
            insert_select = ", ".join(f"clean_df.{qi(c)}" for c in cols)
            update_set    = ", ".join(f"{qi(c)} = excluded.{qi(c)}" for c in non_pk) if non_pk else ""

            sql = f"""
                INSERT INTO {qi(table)} ({insert_cols})
                SELECT {insert_select} FROM clean_df
                ON CONFLICT ({qi(pk)}) DO
                {"NOTHING" if not update_set else "UPDATE SET " + update_set};
            """
            con.execute(sql)

            affected = con.execute(f"""
                SELECT COUNT(*) 
                FROM {qi(table)} t
                JOIN clean_df s ON t.{qi(pk)} = s.{qi(pk)}
            """).fetchone()[0]

            con.unregister("clean_df")
            return int(affected)

    def record_run(self, corr_id: str, table: str, rows: int):
        with self._conn() as con:
            con.execute(f"""
                INSERT INTO {qi('_clean_runs')} (corr_id, table_name, row_count)
                VALUES (?, ?, ?)
                ON CONFLICT (corr_id) DO UPDATE SET
                    table_name   = excluded.table_name,
                    row_count    = excluded.row_count,
                    processed_at = now();
            """, [corr_id, table, rows])

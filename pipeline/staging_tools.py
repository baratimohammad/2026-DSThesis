import hashlib
import os
import re
import pandas as pd
from pathlib import Path
from io import StringIO
import csv


# ----------------------
# STAGING TOOLS
# ----------------------

EMPTY_MARKER = "Nessun dato disponibile nella tabella"


def sha256_file(path: str) -> str:
    """
    Compute the SHA-256 hash of a file.

    Args:
        path (str): Path to the file to hash. The file must exist and be readable.

    Returns:
        str: SHA-256 digest of the file contents as a hexadecimal string.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def create_run(cur, pipeline_name="csv_lane", triggered_by="manual") -> str:
    """
    Create a new pipeline run record and return its ID.

    Args:
        cur: Active database cursor used to execute SQL statements.
        pipeline_name (str): Name of the pipeline being executed.
        triggered_by (str): Source that triggered the run (e.g. manual, scheduler).

    Returns:
        str: The generated run_id of the newly created run.
    """
    cur.execute(
        """
        INSERT INTO etl.runs(pipeline_name, triggered_by, status)
        VALUES (%s, %s, 'RUNNING')
        RETURNING run_id
        """,
        (pipeline_name, triggered_by),
    )
    return str(cur.fetchone()[0])


def finish_run(cur, run_id: str, status: str, error_message: str | None = None):
    """
    Mark a pipeline run as finished and update its final status.

    Args:
        cur: Active database cursor used to execute SQL statements.
        run_id (str): Identifier of the run to update.
        status (str): Final run status (e.g. SUCCESS, FAILED).
        error_message (str | None): Optional error details if the run failed.

    Returns:
        None
    """
    cur.execute(
        """
        UPDATE etl.runs
        SET status = %s,
            finished_at = now(),
            error_message = %s
        WHERE run_id = %s
        """,
        (status, error_message, run_id),
    )


def upsert_manifest(
    cur,
    run_id: str,
    file_path: str,
    file_hash: str,
    file_size: int,
    status: str,
):
    """
    Insert a file record into the manifest for a pipeline run.

    Args:
        cur: Active database cursor used to execute SQL statements.
        run_id (str): Identifier of the pipeline run.
        file_path (str): Full path to the file on disk.
        file_hash (str): SHA-256 hash of the file contents.
        file_size (int): File size in bytes.
        status (str): Processing status of the file.

    Returns:
        None
    """
    file_name = os.path.basename(file_path)
    cur.execute(
        """
        INSERT INTO etl.file_manifest(
            run_id,
            file_path,
            file_name,
            file_hash_sha256,
            file_size_bytes,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (file_hash_sha256) DO NOTHING
        """,
        (run_id, file_path, file_name, file_hash, file_size, status),
    )


def mark_manifest(
    cur,
    file_hash: str,
    status: str,
    rows_loaded: int | None = None,
    error_message: str | None = None,
):
    """
    Update the processing status of a file in the manifest.

    Args:
        cur: Active database cursor used to execute SQL statements.
        file_hash (str): SHA-256 hash identifying the file.
        status (str): New processing status for the file.
        rows_loaded (int | None): Number of rows successfully loaded, if applicable.
        error_message (str | None): Optional error details if processing failed.

    Returns:
        None
    """
    cur.execute(
        """
        UPDATE etl.file_manifest
        SET status=%s,
            rows_loaded=%s,
            error_message=%s
        WHERE file_hash_sha256=%s
        """,
        (status, rows_loaded, error_message, file_hash),
    )


def extract_matricola_from_filename(path: str) -> str | None:
    """
    Extract the matricola (leading numeric identifier) from a filename.

    Args:
        path (str): File path or filename containing a leading numeric ID
            (e.g. "293106_attivita_formative_esterne.csv").

    Returns:
        str | None: The extracted matricola if found, otherwise None.
    """
    m = re.search(r"/(\d+)_", path.replace("\\", "/"))
    return m.group(1) if m else None


def extract_ciclo_from_path(path: str) -> str | None:
    """
    Extract the ciclo identifier from a filesystem path.

    Args:
        path (str): File path expected to contain a segment like "/cicli/<number>/".

    Returns:
        str | None: The extracted ciclo value if present, otherwise None.
    """
    m = re.search(r"/cicli/(\d+)/", path.replace("\\", "/"))
    return m.group(1) if m else None


def extract_cod_ins_and_anno(path: str) -> tuple[str | None, str | None]:
    """
    Extract course code and year from a CSV filename.

    Supported formats:
    - "<COD_INS>_<YYYY>.csv" (e.g. "01DLYRW_2022.csv")
    - "dettaglio_corso_<COD_INS>_<YYYY>.csv" (e.g. "dettaglio_corso_01DLYRW_2023.csv")

    Args:
        path (str): File path or filename to parse.

    Returns:
        tuple[str | None, str | None]:
            (cod_ins, anno) if a supported pattern matches,
            (None, None) otherwise.
    """
    base = os.path.basename(path)

    m = re.match(r"([A-Za-z0-9]+)_(\d{4})\.csv$", base)
    if m:
        return m.group(1), m.group(2)

    m = re.match(r"dettaglio_corso_([A-Za-z0-9]+)_(\d{4})\.csv$", base)
    if m:
        return m.group(1), m.group(2)

    return None, None


def read_csv_flexible(path: str) -> pd.DataFrame:
    """
    Read a CSV trying common separators and reject obvious bad parses.
    """
    for sep in [";", "\t", ","]:
        try:
            df = pd.read_csv(path, delimiter=sep, dtype=str) # , encoding="utf-8", engine="python"
            # reject obviously bad parses
            first = str(df.columns[0])
            if (";" in first and sep != ";") or ("," in first and sep != ","):
                continue
            return df
        except Exception:
            continue

    # rescue: header got swallowed into one string like "a;b;c",,,,,,
    df = pd.read_csv(path, delimiter=",", dtype=str)
    first_header = str(df.columns[0])

    if ";" in first_header:
        cols = [c.strip().strip('"') for c in first_header.split(";")]
        # rebuild df: take first column values, split by ';' into real columns
        data = df.iloc[:, 0].fillna("").astype(str).str.split(";", expand=True)
        data.columns = cols[: data.shape[1]]
        return data

    # give up
    return df


def is_empty_marker_file(path: str) -> bool:
    """
    Check whether a file contains the configured empty-marker string.

    Args:
        path (str): Path to the file to inspect.

    Returns:
        bool: True if the file content includes the empty marker,
        False if not present or if the file cannot be read.
    """
    try:
        txt = Path(path).read_text(encoding="utf-8", errors="ignore")
        return EMPTY_MARKER.lower() in txt.lower()
    except Exception:
        return False


def copy_df_to_staging(cur, df: pd.DataFrame, table: str):
    """
    Bulk-load a pandas DataFrame into a database table using COPY FROM STDIN.

    The DataFrame is serialized to CSV in memory and streamed directly
    to the database for efficient insertion.

    Args:
        cur: Active database cursor supporting COPY operations.
        df (pd.DataFrame): DataFrame to load into the database.
        table (str): Fully qualified target table name.

    Returns:
        None
    """

    buf = StringIO()
    # safer CSV output (quotes/escapes) than defaults
    df.to_csv(buf, index=False, quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)

    cols = ", ".join(f'"{c}"' for c in df.columns)  # quote identifiers
    sql = f'COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)'
    cur.copy_expert(sql, buf)


def load_one_file(cur, run_id: str, file_path: str, table: str, transform_fn):
    """
    Load a single input file into a staging table with manifest tracking.

    Workflow:
    - Compute file hash/size and insert into etl.file_manifest for this run.
    - Skip if the same file hash was already LOADED or SKIPPED in the past.
    - If the file contains an "empty marker", mark as SKIPPED.
    - Read CSV with flexible separator detection, transform it, and skip if no rows remain.
    - Bulk load to the target table via COPY, then mark manifest as LOADED.
    - On any error, mark manifest as FAILED and re-raise.

    Args:
        cur: Active database cursor used for SQL and COPY operations.
        run_id (str): Identifier of the current pipeline run.
        file_path (str): Path to the input file to process.
        table (str): Fully qualified staging table name to load into.
        transform_fn: Callable that takes (raw_df, file_path, run_id) and returns a transformed DataFrame.

    Returns:
        None
    """
    file_hash = sha256_file(file_path)
    file_size = os.path.getsize(file_path)

    upsert_manifest(cur, run_id, file_path, file_hash, file_size, status="NEW")

    cur.execute("SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s", (file_hash,))
    existing_status = cur.fetchone()
    if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
        return

    print(f"Loading file {file_path} into {table}...")
    try:
        if is_empty_marker_file(file_path):
            mark_manifest(
                cur,
                file_hash,
                status="SKIPPED",
                rows_loaded=0,
                error_message="Empty marker detected",
            )
            return

        raw = read_csv_flexible(file_path)
        df = transform_fn(raw, file_path, run_id)

        if df.empty:
            mark_manifest(
                cur,
                file_hash,
                status="SKIPPED",
                rows_loaded=0,
                error_message="No rows after transform",
            )
            return

        copy_df_to_staging(cur, df, table)
        mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None)

    except Exception as e:
        mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e))
        raise


def build_postgres_db_url(
    user: str,
    password: str,
    host: str,
    port: int | str,
    database: str,
) -> str:
    """
    Build a PostgreSQL database URL.

    Raises:
        ValueError: If any required input is missing or empty.
    """
    if not all([user, password, host, port, database]):
        raise ValueError("PostgreSQL connection parameters are missing or empty")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

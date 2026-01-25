from datetime import datetime, timezone
import os
import re
import pandas as pd
from pipeline.staging_tools import (extract_ciclo_from_path, extract_matricola_from_filename, extract_cod_ins_and_anno,
                           copy_df_to_staging, is_empty_marker_file, mark_manifest, sha256_file,
                           upsert_manifest)
from typing import Mapping
import psycopg2.extensions as conn
import glob

# ----------------------
# STAGING TRANSFORM FUNCTIONS
# ----------------------
EMPTY_MARKER = "Nessun dato disponibile nella tabella"


def stage_student_data(
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/cicli/*/0_students_info.csv",
    table: str = "staging.students",
) -> None:
    """Stage student CSV files into a Postgres staging table and track per-file status in the manifest.

    Args:
        cur: Open psycopg2 cursor bound to the target database connection.
        run_id: ETL run identifier attached to each staged row.
        file_path: Glob pattern for input CSV files.
        table: Fully-qualified destination staging table name.
    """
    colmap: Mapping[str, str] = {
        "Matricola dottorando/a:": "matricola_dottorando",
        "Matricola da dipendente/docente:": "matricola_dipendente_docente",
        "Email": "email",
        "Cognome": "cognome",
        "Nome": "nome",
        "Ciclo": "ciclo",
        "Tutore": "tutore",
        "Co-tutore": "co_tutore",
        "Status": "status",
        "Ore Soft Skills": "ore_soft_skills",
        "Ore Hard Skills": "ore_hard_skills",
        "Punti Soft Skills": "punti_soft_skills",
        "Punti Hard Skills": "punti_hard_skills",
        "Punti Attività fuorisede": "punti_attivita_fuorisede",
        "Punti totali": "punti_totali",
    }

    files = sorted(glob.glob(file_path))
    for f in files:
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        
        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="Empty marker detected",
                )
                continue

            raw = pd.read_csv(f, delimiter=";", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="No rows after transform",
                )
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())

            copy_df_to_staging(cur, df, table)
            mark_manifest(
                cur,
                file_hash,
                status="LOADED",
                rows_loaded=len(df),
                error_message=None,
            )
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(
                cur,
                file_hash,
                status="FAILED",
                rows_loaded=None,
                error_message=str(e),
            )
            raise


def stage_attivita_interne(
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/cicli/*/*_attivita_formative_interne.csv",
    table: str = "staging.attivita_formative_interne",
) -> None:
    """
    Stage internal training activities (attività formative interne) into the staging schema.

    The function scans CSV files matching the given path pattern, validates and renames
    columns according to the expected schema, enriches records with run metadata
    (run_id, source_file, loaded_at, matricola, ciclo), and bulk-loads the data
    into the target staging table. File-level ingestion is tracked via etl.file_manifest
    using a content-based hash to ensure idempotency.

    Files already marked as LOADED or SKIPPED are not reprocessed. Empty marker files
    and files producing no rows after transformation are safely skipped.

    Args:
        cur: Open database cursor used for manifest updates and COPY operations.
        run_id: Identifier of the current ETL run (used for traceability).
        file_path: Glob pattern pointing to input CSV files containing internal
            training activities, typically organized by ciclo and matricola.
        table: Fully qualified name of the staging table receiving the data.
    """


    colmap: Mapping[str, str] = {
    "Cod Ins.": "cod_ins",
    "Nome insegnamento": "nome_insegnamento",
    "Ore": "ore",
    "Ore riconosciute": "ore_riconosciute",
    "Voto": "voto",
    "Coeff. voto": "coeff_voto",
    "Data esame": "data_esame",
    "Tipo form.": "tipo_form",
    "Liv. Esame": "liv_esame",
    "Tipo attività": "tipo_attivita",
    "Punti": "punti",
}

    files = sorted(glob.glob(file_path))
    for f in files:
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        matricola = extract_matricola_from_filename(f)
        ciclo = extract_ciclo_from_path(f)

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="Empty marker detected",
                )
                continue

            raw = pd.read_csv(f, delimiter=",", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="No rows after transform",
                )
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "matricola", matricola)
            df.insert(4, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(
                cur,
                file_hash,
                status="LOADED",
                rows_loaded=len(df),
                error_message=None,
            )
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(
                cur,
                file_hash,
                status="FAILED",
                rows_loaded=None,
                error_message=str(e),
            )
            raise


def stage_attivita_esterne( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/cicli/*/*_attivita_formative_esterne.csv",
    table: str = "staging.attivita_formative_esterne",
) -> None:
    """
    Stage external training activities (attività formative esterne) into the staging schema.

    Reads TSV input files matching `file_path`, selects and renames columns via `colmap`,
    enriches rows with ETL metadata (run_id, source_file, loaded_at) plus identifiers
    extracted from the file path (matricola, ciclo), then bulk-loads into `table`.

    Ingestion is tracked in etl.file_manifest using a SHA-256 hash for idempotency:
    files already marked LOADED or SKIPPED are not reprocessed. Empty marker files and
    files yielding no rows after transformation are skipped with an explicit manifest status.

    Args:
        cur: Open DB cursor used for manifest operations and COPY into staging.
        run_id: Current ETL run identifier for traceability across staged records.
        file_path: Glob pattern for input TSV files (tab-delimited) containing external activities.
        table: Fully qualified staging table name that receives the transformed data.
    """

    colmap: Mapping[str, str] = {
    "Denominazione": "denominazione",
    "Ore dichiarate": "ore_dichiarate",
    "Ore riconosciute": "ore_riconosciute",
    "Ore calcolate": "ore_calcolate",
    "Coeff. voto": "coeff_voto",
    "Punti": "punti",
    "Tipo form.": "tipo_form",
    "Tipo Richiesta": "tipo_richiesta",
    "Liv. Esame": "liv_esame",
    "Data attività": "data_attivita",
    "Data convalida": "data_convalida",
}


    files = sorted(glob.glob(file_path))
    for f in files:
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        matricola = extract_matricola_from_filename(f)
        ciclo = extract_ciclo_from_path(f)

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="Empty marker detected",
                )
                continue

            raw = pd.read_csv(f, delimiter="\t", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="No rows after transform",
                )
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "matricola", matricola)
            df.insert(4, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(
                cur,
                file_hash,
                status="LOADED",
                rows_loaded=len(df),
                error_message=None,
            )
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(
                cur,
                file_hash,
                status="FAILED",
                rows_loaded=None,
                error_message=str(e),
            )
            raise


def stage_attivita_fuorisede( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/cicli/*/*_attivita_formative_fuorisede.csv",
    table: str = "staging.attivita_formative_fuorisede",
) -> None:
    """
    Stage off-site training activities (attività formative fuori sede) into the staging schema.

    Processes tab-delimited input files matching `file_path`, selects and renames columns
    according to the expected schema, enriches records with ETL metadata
    (run_id, source_file, loaded_at) and identifiers derived from the file path
    (matricola, ciclo), and bulk-loads the result into the target staging table.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to guarantee
    idempotency. Files already marked as LOADED or SKIPPED are ignored. Empty marker
    files and files producing no rows after transformation are explicitly skipped.

    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for tab-delimited CSV files containing off-site activities.
        table: Fully qualified staging table receiving the transformed data.
    """

    colmap: Mapping[str, str] = {
    "Denominazione": "denominazione",
    "Luogo": "luogo",
    "Ente": "ente",
    "Periodo": "periodo",
    "Data autorizzazione": "data_autorizzazione",
    "Data aut.pagamento": "data_aut_pagamento",
    "Data attestaz.": "data_attestaz",
    "Data convalida": "data_convalida",
}

    files = sorted(glob.glob(file_path))
    for f in files:
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        matricola = extract_matricola_from_filename(f)
        ciclo = extract_ciclo_from_path(f)

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="Empty marker detected",
                )
                continue

            raw = pd.read_csv(f, delimiter="\t", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(
                    cur,
                    file_hash,
                    status="SKIPPED",
                    rows_loaded=0,
                    error_message="No rows after transform",
                )
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "matricola", matricola)
            df.insert(4, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(
                cur,
                file_hash,
                status="LOADED",
                rows_loaded=len(df),
                error_message=None,
            )
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(
                cur,
                file_hash,
                status="FAILED",
                rows_loaded=None,
                error_message=str(e),
            )
            raise


def stage_pubblicazioni( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/cicli/*/*_pubblicazioni.csv",
    table: str = "staging.pubblicazioni",
) -> None:
    """
    Stage publication records into the staging schema.

    Processes tab-delimited publication files matching `file_path`, normalizes and renames
    columns according to the expected schema, and expands multi-author publications by
    splitting the 'Autori' field on ';' and exploding one row per author.

    Each record is enriched with ETL metadata (run_id, source_file, loaded_at) and
    identifiers derived from the file path (matricola, ciclo), then bulk-loaded into
    the target staging table. File ingestion is tracked via etl.file_manifest using a
    SHA-256 hash to ensure idempotent processing.

    Files already marked as LOADED or SKIPPED are ignored. Empty marker files and files
    yielding no rows after transformation are explicitly skipped.
    
    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for tab-delimited CSV files containing publication data.
        table: Fully qualified staging table receiving the transformed publication records.
    """

    colmap: Mapping[str, str] = {
    "Anno": "anno",
    "Tipo": "tipo",
    "Titolo": "titolo",
    "Rivista": "rivista",
    "Autori": "autori",
    "Convegno": "convegno",
    "Referee": "referee",
    "Grado proprietà dottorandi": "grado_proprieta_dottorandi",
    "Punteggio": "punteggio",
    "Grado proprietà": "grado_proprieta",
    "Indicatore R": "indicatore_r",
    "Errore Val.": "errore_val",
}


    files = sorted(glob.glob(file_path))
    for f in files:
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        matricola = extract_matricola_from_filename(f)
        ciclo = extract_ciclo_from_path(f)

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="Empty marker detected",)
                continue

            raw = pd.read_csv(f, delimiter="\t", dtype=str)
            
            # to handle multiple authors per publication, each author will be collapsed into its own row
            raw["Autori"] = raw["Autori"].str.split(";")
            raw = raw.explode("Autori")
            raw["Autori"] = raw["Autori"].str.strip()

            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="No rows after transform",)
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "matricola", matricola)
            df.insert(4, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None,)
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e),)
            raise


def stage_corsi( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/corsi/*_*.csv",
    table: str = "staging.corsi",
) -> None:
    """
    Stage course enrollment/status extracts into the staging schema.

    Scans tab-delimited course files matching `file_path`, excluding
    'dettaglio_corso_*' files (handled by a separate staging function).
    The function selects and renames columns via `colmap`, enriches each row with
    ETL metadata (run_id, source_file, loaded_at) and file-derived identifiers
    (cod_ins, anno), then bulk-loads the results into `table`.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to ensure
    idempotency: files already marked as LOADED or SKIPPED are not reprocessed.
    Empty marker files and files producing no rows after transformation are skipped.
     
    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for tab-delimited course files. Files named
            'dettaglio_corso_*' are intentionally skipped here.
        table: Fully qualified staging table receiving the transformed course records.
    """

    colmap: Mapping[str, str] = {
    "matricola": "matricola",
    "cognome": "cognome",
    "nome": "nome",
    "codInsegnamento": "cod_insegnamento_col",
    "codCorsoDottorato": "cod_corso_dottorato",
    "PeriodoDidattico": "periodo_didattico",
    "-": "dash_col",
    "stato": "stato",
}


    files = sorted(glob.glob(file_path))
    for f in files:
        if f.startswith('./data/input/corsi/dettaglio_corso_'):
            continue  # skip dettaglio_corso files in this function
        
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        
        codins, anno = extract_cod_ins_and_anno(f)

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="Empty marker detected",)
                continue

            raw = pd.read_csv(f, delimiter="\t", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="No rows after transform",)
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "cod_ins", codins)
            df.insert(4, "anno", anno)

            copy_df_to_staging(cur, df, table)
            mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None,)
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e),)
            raise


def stage_dettaglio_corsi( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/corsi/dettaglio_corso_*_*.csv",
    table: str = "staging.dettaglio_corso",
) -> None:
    """
    Stage detailed course teaching information into the staging schema.

    Processes tab-delimited course detail files (`dettaglio_corso_*`) containing
    teaching staff assignments and workload metrics. The function filters out
    non-informative rows (e.g. 'Nessuna collaborazione prevista'), renames columns
    according to the expected schema, enriches records with ETL metadata
    (run_id, source_file, loaded_at) and file-derived identifiers (cod_ins, anno),
    and bulk-loads the results into the target staging table.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to ensure
    idempotent processing. Files already marked as LOADED or SKIPPED are ignored.
    Empty marker files and files producing no rows after transformation are skipped.

    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for tab-delimited course detail files
            (dettaglio_corso_*), containing teaching assignments and hours.
        table: Fully qualified staging table receiving the transformed detail records.
    """

    colmap: Mapping[str, str] = {
    "Type": "type",
    "Teacher": "teacher",
    "Status": "status",
    "SSD": "ssd",
    "h.Les": "h_les",
    "h.Ex": "h_ex",
    "h.Lab": "h_lab",
    "h.Tut": "h_tut",
    "Years teaching": "years_teaching",
}


    files = sorted(glob.glob(file_path))
    for f in files:
        if not f.startswith('./data/input/corsi/dettaglio_corso_'):
            continue  # skip non-dettaglio_corso files in this function
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest
        
        codins, anno = extract_cod_ins_and_anno(f)

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="Empty marker detected",)
                continue

            raw = pd.read_csv(f, delimiter="\t", dtype=str)
            raw = raw[~(raw['Teacher']=='Nessuna collaborazione prevista')]
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="No rows after transform",)
                continue

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "cod_ins", codins)
            df.insert(4, "anno", anno)

            copy_df_to_staging(cur, df, table)
            mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None,)
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e),)
            raise


def stage_journal_details( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/journal_details/journal_details_*.csv",
    table: str = "staging.journal_details",
) -> None:
    """
    Stage journal publication details into the staging schema.

    Processes CSV files matching `file_path` that contain per-journal publication
    metadata (author identity, journal info, ISSN, year, quartile). Columns are
    selected and renamed according to the expected schema, then enriched with ETL
    metadata (run_id, source_file, loaded_at) and the cycle identifier extracted
    from the filename.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to ensure
    idempotent processing. Files already marked as LOADED or SKIPPED are ignored.
    Empty marker files and files producing no rows after transformation are skipped.

    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for comma-delimited CSV files named
            'journal_details_<ciclo>.csv', where <ciclo> is extracted from the filename.
        table: Fully qualified staging table receiving the transformed journal records.
    """

    colmap: Mapping[str, str] = {
    "matricola": "matricola",
    "cognome": "cognome",
    "nome": "nome",
    "titolo": "titolo",
    "rivista": "rivista",
    "issn": "issn",
    "anno": "anno",
    "quartile": "quartile",
}

    files = sorted(glob.glob(file_path))
    for f in files:
        
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="Empty marker detected",)
                continue

            raw = pd.read_csv(f, delimiter=",", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="No rows after transform",)
                continue
            
            base = os.path.basename(f)
            m = re.match(r"journal_details_(\d+)\.csv$", base)
            ciclo = m.group(1) if m else None

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None,)
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e),)
            raise


def stage_ore_formazione( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/ore_formazione/ore_formazione_ciclo_*.csv",
    table: str = "staging.ore_formazione",
) -> None:
    """
    Stage training-hours summaries (ore di formazione) into the staging schema.

    Processes comma-delimited CSV files matching `file_path` that report aggregated
    training hours per dottorando (soft skills, hard skills, total). Columns are
    selected and renamed according to the expected schema, then enriched with ETL
    metadata (run_id, source_file, loaded_at) and the cycle identifier extracted
    from the filename.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to ensure
    idempotent processing. Files already marked as LOADED or SKIPPED are ignored.
    Empty marker files and files producing no rows after transformation are skipped.

    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for comma-delimited CSV files named
            'ore_formazione_ciclo_<ciclo>.csv', where <ciclo> is extracted from the filename.
        table: Fully qualified staging table receiving the transformed training-hour records.
    """

    colmap: Mapping[str, str] = {
    "matricola": "matricola",
    "cognome": "cognome",
    "nome": "nome",
    "tutor": "tutor",
    "ore soft skill": "ore_soft_skill",
    "ore hard skill": "ore_hard_skill",
    "ore totali": "ore_totali",
}


    files = sorted(glob.glob(file_path))
    for f in files:
        
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="Empty marker detected",)
                continue

            raw = pd.read_csv(f, delimiter=",", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="No rows after transform",)
                continue
            
            base = os.path.basename(f)
            m = re.match(r"ore_formazione_ciclo_(\d+)\.csv$", base)
            ciclo = m.group(1) if m else None

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None,)
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e),)
            raise


def stage_stat_pubb( 
    cur: conn.cursor,
    run_id: int,
    file_path: str = "./data/input/stat_pubb/stat_pubb_*.csv",
    table: str = "staging.stat_pubb",
) -> None:
    """
    Stage publication statistics summaries into the staging schema.

    Processes comma-delimited CSV files matching `file_path` that contain aggregated
    publication counts per dottorando (journals, conferences, chapters, posters,
    abstracts, patents) and per-quartile metrics. Columns are selected and renamed
    according to the expected schema, then enriched with ETL metadata
    (run_id, source_file, loaded_at) and the cycle identifier extracted from
    the filename.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to ensure
    idempotent processing. Files already marked as LOADED or SKIPPED are ignored.
    Empty marker files and files producing no rows after transformation are skipped.
    
    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Glob pattern for comma-delimited CSV files named
            'stat_pubb_<ciclo>.csv', where <ciclo> is extracted from the filename.
        table: Fully qualified staging table receiving the transformed
            publication statistics records.
    """

    colmap: Mapping[str, str] = {
    "matricola": "matricola",
    "cognome": "cognome",
    "nome": "nome",
    "numero_journal": "numero_journal",
    "numero_conferenze": "numero_conferenze",
    "numero_capitoli": "numero_capitoli",
    "numero_poster": "numero_poster",
    "numero_abstract": "numero_abstract",
    "numero_brevetti": "numero_brevetti",
    "quartile_1": "quartile_1",
    "quartile_2": "quartile_2",
    "quartile_3": "quartile_3",
    "quartile_4": "quartile_4",
    "quartile_5": "quartile_5",
    "quartile_6": "quartile_6",
    "quartile_7": "quartile_7",
    "quartile_8": "quartile_8",
    "quartile_9": "quartile_9",
    "quartile_10": "quartile_10",
    "quartile_11": "quartile_11",
    "quartile_12": "quartile_12",
    "quartile_13": "quartile_13",
    "quartile_14": "quartile_14",
    "quartile_15": "quartile_15",
}



    files = sorted(glob.glob(file_path))
    for f in files:
        
        file_hash = sha256_file(f)                      # compute file hash for etl.file_manifest
        file_size = os.path.getsize(f)                  # compute file size for etl.file_manifest

        upsert_manifest(cur, run_id, f, file_hash, file_size, status="NEW")

        cur.execute(
            "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
            (file_hash,),
        )
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ("LOADED", "SKIPPED"):
            print(f"File {f} already loaded with status {existing_status[0]}, skipping.")
            continue

        print(f"Loading file {f} into {table}...")
        try:
            if is_empty_marker_file(f):
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="Empty marker detected",)
                continue

            raw = pd.read_csv(f, delimiter=",", dtype=str)
            keep = [c for c in colmap if c in raw.columns]
            df = raw[keep].rename(columns=colmap)

            if df.empty:
                mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0, error_message="No rows after transform",)
                continue
            
            base = os.path.basename(f)
            m = re.match(r"stat_pubb_(\d+)\.csv$", base)
            ciclo = m.group(1) if m else None

            df.insert(0, "run_id", run_id)
            df.insert(1, "source_file", f)
            df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())
            df.insert(3, "ciclo", ciclo)

            copy_df_to_staging(cur, df, table)
            mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None,)
            print(f"Loaded {len(df)} rows from {f} into {table}")

        except Exception as e:
            # COPY or any SQL error aborts the transaction → must rollback first
            cur.connection.rollback()

            mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e),)
            raise


def stage_collaborazioni_dettaglio(
    cur,
    run_id: int,
    file_path: str = "./data/input/collaborazioni_dettaglio.csv",
    table: str = "staging.collaborazioni_dettaglio",
) -> None:
    """
    Stage collaboration-detail records into the staging schema.

    Loads a single comma-delimited CSV at `file_path`, validates the expected input
    columns, renames them according to the target schema, enriches each row with ETL
    metadata (run_id, source_file, loaded_at), and bulk-loads into `table`.

    Ingestion is tracked in etl.file_manifest using a SHA-256 hash for idempotency:
    if the file is already marked LOADED or SKIPPED it is not reprocessed. Empty marker
    files are skipped, and structurally valid files that contain zero rows are skipped
    with an explicit manifest status.
    
    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Path to the comma-delimited CSV containing collaboration details.
        table: Fully qualified staging table receiving the transformed records.
    """

    colmap: Mapping[str, str] = {
        "Matricola Dott": "matricola_dott",
        "Cognome": "cognome",
        "Nome": "nome",
        "Ciclo": "ciclo",
        "Tutor": "tutor",
        "Ore": "ore",
        "Tipo Attività": "tipo_attivita",
        "Materia": "materia",
        "Docente": "docente",
        "Corso di Laurea": "corso_di_laurea",
    }

    file_hash = sha256_file(file_path)
    file_size = os.path.getsize(file_path)

    upsert_manifest(cur, run_id, file_path, file_hash, file_size, status="NEW")

    cur.execute(
        "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
        (file_hash,),
    )
    row = cur.fetchone()
    if row and row[0] in ("LOADED", "SKIPPED"):
        print(f"File {file_path} already loaded with status {row[0]}, skipping.")
        return

    print(f"Loading file {file_path} into {table}...")

    try:
        if is_empty_marker_file(file_path):
            mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0,
                          error_message="Empty marker detected")
            return

        raw = pd.read_csv(file_path, delimiter=",", dtype=str)

        missing = [c for c in colmap.keys() if c not in raw.columns]
        if missing:
            raise ValueError(f"Missing columns in {file_path}: {missing}")

        df = raw[list(colmap.keys())].rename(columns=colmap)

        if df.shape[0] == 0:
            mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0,
                          error_message="No rows after transform")
            return

        # If you truly need ciclo from filename, don't overwrite df["ciclo"].
        # Use a different column name:
        # df["ciclo_file"] = extracted_value

        df.insert(0, "run_id", run_id)
        df.insert(1, "source_file", file_path)
        df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())

        copy_df_to_staging(cur, df, table)
        mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None)
        print(f"Loaded {len(df)} rows from {file_path} into {table}")

    except Exception as e:
        cur.connection.rollback()
        mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e))
        raise


def stage_filtered_iu_stats(
    cur,
    run_id: int,
    file_path: str = "./data/input/filtered_IU_stats.csv",
    table: str = "staging.filtered_iu_stats",
) -> None:
    """
    Stage filtered IU course statistics into the staging schema.

    Loads a single comma-delimited CSV containing pre-filtered enrollment and
    completion statistics per course and academic year. The function validates
    the full expected input schema, renames columns to the target format, enriches
    records with ETL metadata (run_id, source_file, loaded_at), and bulk-loads the
    data into the target staging table.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to guarantee
    idempotent processing. If the file is already marked as LOADED or SKIPPED it is
    not reprocessed. Empty marker files and structurally valid files with zero rows
    are explicitly skipped.

    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Path to the comma-delimited CSV containing filtered IU statistics.
        table: Fully qualified staging table receiving the transformed statistics.
    """



    colmap: Mapping[str, str] = {
    "Cod.Ins": "cod_ins",
    "Nome Insegnamento": "nome_insegnamento",
    "Docente": "docente",
    "Iscritti_2025": "iscritti_2025",
    "Superati_2025": "superati_2025",
    "IIS_Iscritti_2025": "iis_iscritti_2025",
    "IIS_Superati_2025": "iis_superati_2025",
    "Iscritti_2024": "iscritti_2024",
    "Superati_2024": "superati_2024",
    "IIS_Iscritti_2024": "iis_iscritti_2024",
    "IIS_Superati_2024": "iis_superati_2024",
    "Iscritti_2023": "iscritti_2023",
    "Superati_2023": "superati_2023",
    "IIS_Iscritti_2023": "iis_iscritti_2023",
    "IIS_Superati_2023": "iis_superati_2023",
    "Iscritti_2022": "iscritti_2022",
    "Superati_2022": "superati_2022",
    "IIS_Iscritti_2022": "iis_iscritti_2022",
    "IIS_Superati_2022": "iis_superati_2022",
}


    file_hash = sha256_file(file_path)
    file_size = os.path.getsize(file_path)

    upsert_manifest(cur, run_id, file_path, file_hash, file_size, status="NEW")

    cur.execute(
        "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
        (file_hash,),
    )
    row = cur.fetchone()
    if row and row[0] in ("LOADED", "SKIPPED"):
        print(f"File {file_path} already loaded with status {row[0]}, skipping.")
        return

    print(f"Loading file {file_path} into {table}...")

    try:
        if is_empty_marker_file(file_path):
            mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0,
                          error_message="Empty marker detected")
            return

        raw = pd.read_csv(file_path, delimiter=",", dtype=str)

        missing = [c for c in colmap.keys() if c not in raw.columns]
        if missing:
            raise ValueError(f"Missing columns in {file_path}: {missing}")

        df = raw[list(colmap.keys())].rename(columns=colmap)

        if df.shape[0] == 0:
            mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0,
                          error_message="No rows after transform")
            return

        df.insert(0, "run_id", run_id)
        df.insert(1, "source_file", file_path)
        df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())

        copy_df_to_staging(cur, df, table)
        mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None)
        print(f"Loaded {len(df)} rows from {file_path} into {table}")

    except Exception as e:
        cur.connection.rollback()
        mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e))
        raise


def stage_mobilita_internazionale_con_studenti(
    cur,
    run_id: int,
    file_path: str = "./data/input/mobilita_internazionale_con_studenti.csv",
    table: str = "staging.mobilita_internazionale_con_studenti",
) -> None:
    """
    Stage international mobility records involving students into the staging schema.

    Loads a single comma-delimited CSV containing international mobility activities
    carried out with students. The function validates the expected input schema,
    renames columns to the target format, enriches each record with ETL metadata
    (run_id, source_file, loaded_at), and bulk-loads the data into the staging table.

    File ingestion is tracked in etl.file_manifest using a SHA-256 hash to ensure
    idempotent processing. If the file is already marked as LOADED or SKIPPED it is
    not reprocessed. Empty marker files and structurally valid files with zero rows
    are explicitly skipped.

    Args:
        cur: Open database cursor used for manifest bookkeeping and COPY operations.
        run_id: Identifier of the current ETL run for traceability.
        file_path: Path to the comma-delimited CSV containing international mobility
            activities with students.
        table: Fully qualified staging table receiving the transformed mobility records.
    """
    
    colmap: Mapping[str, str] = {
    "matricola": "matricola",
    "cognome": "cognome",
    "nome": "nome",
    "tutore": "tutore",
    "ciclo": "ciclo",
    "tipo": "tipo",
    "paese": "paese",
    "ente": "ente",
    "periodo": "periodo",
    "durata_giorni": "durata_giorni",
    "anno": "anno",
    "data_autorizzazione": "data_autorizzazione",
    "data_pagamento": "data_pagamento",
}



    file_hash = sha256_file(file_path)
    file_size = os.path.getsize(file_path)

    upsert_manifest(cur, run_id, file_path, file_hash, file_size, status="NEW")

    cur.execute(
        "SELECT status FROM etl.file_manifest WHERE file_hash_sha256=%s",
        (file_hash,),
    )
    row = cur.fetchone()
    if row and row[0] in ("LOADED", "SKIPPED"):
        print(f"File {file_path} already loaded with status {row[0]}, skipping.")
        return

    print(f"Loading file {file_path} into {table}...")

    try:
        if is_empty_marker_file(file_path):
            mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0,
                          error_message="Empty marker detected")
            return

        raw = pd.read_csv(file_path, delimiter=",", dtype=str)

        missing = [c for c in colmap.keys() if c not in raw.columns]
        if missing:
            raise ValueError(f"Missing columns in {file_path}: {missing}")

        df = raw[list(colmap.keys())].rename(columns=colmap)

        if df.shape[0] == 0:
            mark_manifest(cur, file_hash, status="SKIPPED", rows_loaded=0,
                          error_message="No rows after transform")
            return

        df.insert(0, "run_id", run_id)
        df.insert(1, "source_file", file_path)
        df.insert(2, "loaded_at", datetime.now(timezone.utc).isoformat())

        copy_df_to_staging(cur, df, table)
        mark_manifest(cur, file_hash, status="LOADED", rows_loaded=len(df), error_message=None)
        print(f"Loaded {len(df)} rows from {file_path} into {table}")

    except Exception as e:
        cur.connection.rollback()
        mark_manifest(cur, file_hash, status="FAILED", rows_loaded=None, error_message=str(e))
        raise



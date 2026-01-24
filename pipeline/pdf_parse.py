import os
import re
import sys
import glob
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Iterable, Any
from datetime import datetime, timezone

import psycopg2
import pdfplumber
from dotenv import load_dotenv
from pipeline.staging_tools import build_postgres_db_url
import logging
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# ----------------------------
# Your parsing logic (unchanged)
# ----------------------------
START_MARKER = "Experience"
END_MARKERS = {"Education", "Licenses & certifications", "Skills"}

date_line_re = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}\s*-\s*(Present|"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})\s*·\s*.+$"
)

def extract_lines(pdf_path: str) -> List[List[str]]:
    if not isinstance(pdf_path, str) or not pdf_path.strip():
        raise ValueError("pdf_path must be a non-empty string.")

    pages_lines: List[List[str]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            pages_lines.append(lines)
    return pages_lines


def get_experience_block(
    pages_lines: List[List[str]],
    start_marker: str = START_MARKER,
    end_markers: Iterable[str] = END_MARKERS,
) -> List[str]:
    if not pages_lines:
        return []

    in_exp = False
    block: List[str] = []

    for lines in pages_lines:
        for ln in lines:
            if not in_exp and ln == start_marker:
                in_exp = True
                continue
            if in_exp:
                if ln in end_markers:
                    return block
                block.append(ln)
    return block


def parse_experience(block_lines: List[str]) -> List[Dict[str, Optional[str]]]:
    roles: List[Dict[str, Optional[str]]] = []
    i = 0

    while i < len(block_lines):
        if block_lines[i] in {"Show all posts", "Show all"}:
            i += 1
            continue

        title = block_lines[i]
        company: Optional[str] = None
        emp_type: Optional[str] = None
        dates: Optional[str] = None
        location: Optional[str] = None

        if i + 1 < len(block_lines):
            company_line = block_lines[i + 1]
            if "·" in company_line:
                parts = [p.strip() for p in company_line.split("·", 1)]
                company = parts[0]
                emp_type = parts[1] if len(parts) > 1 else None
            else:
                company = company_line

        j = i + 2
        while j < len(block_lines) and not date_line_re.match(block_lines[j]):
            j += 1

        if j < len(block_lines) and date_line_re.match(block_lines[j]):
            dates = block_lines[j]
            if j + 1 < len(block_lines):
                location = block_lines[j + 1]
            i = j + 2
        else:
            i += 1
            continue

        roles.append({
            "title": title,
            "company": company,
            "employment_type": emp_type,
            "dates": dates,
            "location": location,
        })

    return roles


def parse_linkedin_experiences(pdf_path: str) -> List[Any]:
    if not isinstance(pdf_path, str) or not pdf_path.strip():
        raise ValueError("pdf_path must be a non-empty string.")
    pages_lines = extract_lines(pdf_path)
    exp_block = get_experience_block(pages_lines)
    roles = parse_experience(exp_block)
    return roles


# ----------------------------
# DB helpers
# ----------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_run(cur, pipeline_name="pdf_parse", triggered_by="manual") -> str:
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
    cur.execute(
        """
        UPDATE etl.runs
        SET status=%s, finished_at=now(), error_message=%s
        WHERE run_id=%s
        """,
        (status, error_message, run_id),
    )


def step_start(cur, run_id: str, step_name: str) -> int:
    cur.execute(
        """
        INSERT INTO etl.step_metrics(run_id, step_name, status)
        VALUES (%s, %s, 'RUNNING')
        RETURNING metric_id
        """,
        (run_id, step_name),
    )
    return int(cur.fetchone()[0])


def step_end(cur, metric_id: int, status: str, rows_in=None, rows_out=None, message=None):
    cur.execute(
        """
        UPDATE etl.step_metrics
        SET status=%s,
            finished_at=now(),
            duration_ms=EXTRACT(EPOCH FROM (now() - started_at))*1000,
            rows_in=%s,
            rows_out=%s,
            message=%s
        WHERE metric_id=%s
        """,
        (status, rows_in, rows_out, message, metric_id),
    )


def register_document(cur, run_id: str, pdf_path: str, file_hash: str) -> Optional[str]:
    """
    Inserts into etl.documents unless already present by hash.
    Returns document_id.
    """
    file_name = os.path.basename(pdf_path)
    # try insert; if conflict, fetch existing id
    cur.execute(
        """
        INSERT INTO etl.documents(run_id, source_path, file_name, file_hash_sha256, doc_type, status)
        VALUES (%s, %s, %s, %s, 'pdf', 'NEW')
        ON CONFLICT (file_hash_sha256) DO NOTHING
        """,
        (run_id, pdf_path, file_name, file_hash),
    )
    cur.execute("SELECT document_id FROM etl.documents WHERE file_hash_sha256=%s", (file_hash,))
    row = cur.fetchone()
    return str(row[0]) if row else None


def update_document_status(cur, document_id: str, status: str, error_message: str | None = None):
    cur.execute(
        """
        UPDATE etl.documents
        SET status=%s, error_message=%s
        WHERE document_id=%s
        """,
        (status, error_message, document_id),
    )


def upsert_pdf_pages(cur, run_id: str, document_id: str, source_file: str, pages_text: List[str]) -> int:
    """
    Upserts pages by (document_id, page_number). Returns rows_out inserted/updated.
    """
    rows = 0
    for i, txt in enumerate(pages_text, start=1):
        cur.execute(
            """
            INSERT INTO staging.pdf_pages(run_id, document_id, source_file, page_number, text, loaded_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (document_id, page_number)
            DO UPDATE SET
              run_id=EXCLUDED.run_id,
              source_file=EXCLUDED.source_file,
              text=EXCLUDED.text,
              loaded_at=now()
            """,
            (run_id, document_id, source_file, i, txt),
        )
        rows += 1
    return rows


def upsert_work_experiences(cur, run_id: str, document_id: str, source_file: str, roles: List[Dict[str, Optional[str]]]) -> int:
    rows = 0
    for idx, r in enumerate(roles, start=1):
        cur.execute(
            """
            INSERT INTO staging.work_experience_raw(
              run_id, document_id, source_file, role_index,
              title, company, employment_type, dates, location, extracted_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT (document_id, role_index)
            DO UPDATE SET
              run_id=EXCLUDED.run_id,
              source_file=EXCLUDED.source_file,
              title=EXCLUDED.title,
              company=EXCLUDED.company,
              employment_type=EXCLUDED.employment_type,
              dates=EXCLUDED.dates,
              location=EXCLUDED.location,
              extracted_at=now()
            """,
            (
                run_id, document_id, source_file, idx,
                r.get("title"), r.get("company"), r.get("employment_type"),
                r.get("dates"), r.get("location")
            ),
        )
        rows += 1
    return rows


def extract_page_texts(pdf_path: str) -> List[str]:
    """
    Page-level text for staging.pdf_pages. Keeps raw text per page.
    """
    pages_text: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            pages_text.append(page.extract_text() or "")
    return pages_text


# ----------------------------
# Main runner
# ----------------------------
def main() -> int:
    load_dotenv('.env')
    
    
    dsn = build_postgres_db_url(
        user = os.getenv('POSTGRES_USER'),
        password = os.getenv('POSTGRES_PASSWORD'),
        host= os.getenv('POSTGRES_HOST'),
        port= os.getenv('POSTGRES_PORT'),
        database= os.getenv('POSTGRES_DB')
    )
    if not dsn:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        return 1

    input_dir = "data/input/PhDStudentiLinkedIn"
    pattern = os.path.join(input_dir, "**", "*.pdf")
    pdf_files = sorted(glob.glob(pattern, recursive=True))

    if not pdf_files:
        print(f"ERROR: No PDFs found under {input_dir}", file=sys.stderr)
        return 1

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            run_id = create_run(cur)
            conn.commit()

            metric_discover = step_start(cur, run_id, "pdf_discover")
            step_end(cur, metric_discover, "SUCCESS", rows_in=len(pdf_files), rows_out=len(pdf_files), message=f"Found PDFs under {input_dir}")
            conn.commit()

            try:
                processed = 0
                for pdf_path in pdf_files:
                    file_hash = sha256_file(pdf_path)
                    document_id = register_document(cur, run_id, pdf_path, file_hash)
                    conn.commit()

                    if not document_id:
                        continue

                    source_file = os.path.basename(pdf_path)

                    # If the document was already parsed/enriched earlier, you might skip here.
                    # For now, re-parse deterministically (idempotent due to upserts).
                    update_document_status(cur, document_id, "NEW", None)
                    conn.commit()

                    m_pages = step_start(cur, run_id, f"pdf_extract_pages:{source_file}")
                    try:
                        pages_text = extract_page_texts(pdf_path)
                        rows_pages = upsert_pdf_pages(cur, run_id, document_id, source_file, pages_text)
                        step_end(cur, m_pages, "SUCCESS", rows_in=len(pages_text), rows_out=rows_pages, message=None)
                        conn.commit()
                    except Exception as e:
                        step_end(cur, m_pages, "FAILED", message=str(e))
                        update_document_status(cur, document_id, "FAILED", str(e))
                        conn.commit()
                        continue

                    m_exp = step_start(cur, run_id, f"pdf_parse_experience:{source_file}")
                    try:
                        roles = parse_linkedin_experiences(pdf_path)
                        rows_roles = upsert_work_experiences(cur, run_id, document_id, source_file, roles)
                        step_end(cur, m_exp, "SUCCESS", rows_in=len(roles), rows_out=rows_roles, message=None)

                        # If roles empty, that's not necessarily failure; mark NEEDS_REVIEW so you can inspect.
                        if len(roles) == 0:
                            update_document_status(cur, document_id, "NEEDS_REVIEW", "No roles parsed from Experience section")
                        else:
                            update_document_status(cur, document_id, "PARSED", None)

                        conn.commit()
                        processed += 1

                    except Exception as e:
                        step_end(cur, m_exp, "FAILED", message=str(e))
                        update_document_status(cur, document_id, "FAILED", str(e))
                        conn.commit()

                finish_run(cur, run_id, "SUCCESS", None)
                conn.commit()
                print(f"✅ PDF parse run success: {run_id} | processed={processed} | found={len(pdf_files)}")
                return 0

            except Exception as e:
                finish_run(cur, run_id, "FAILED", str(e))
                conn.commit()
                print(f"❌ PDF parse run failed: {run_id} | {e}", file=sys.stderr)
                return 1


if __name__ == "__main__":
    raise SystemExit(main())

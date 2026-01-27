import os
import sys
import json
import hashlib
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

import psycopg2
from psycopg2 import errors as pg_errors
import requests
from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv

from pipeline.staging_tools import build_postgres_db_url


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
MODEL = "llama3.1:8b-instruct-q4_0"
PROMPT_VERSION = "phd_status_v1"
OLLAMA_TIMEOUT_SEC = int(os.getenv("OLLAMA_TIMEOUT_SEC", "240"))
OLLAMA_MAX_RETRIES = int(os.getenv("OLLAMA_MAX_RETRIES", "2"))

# How many times to retry a doc whose LLM call failed OR produced invalid output
MAX_DOC_ATTEMPTS = int(os.getenv("MAX_DOC_ATTEMPTS", "3"))


class PhDStatus(BaseModel):
    # NOTE: keep Optional here so parsing can succeed, but we will enforce non-null before DB insert.
    is_current_phd: Optional[bool] = Field(default=None)
    current_employer: Optional[str] = None
    current_title: Optional[str] = None
    since_month: Optional[str] = None  # "YYYY-MM"
    evidence: str


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def create_run(cur) -> str:
    cur.execute(
        """
        INSERT INTO etl.runs(pipeline_name, triggered_by, status)
        VALUES ('llm_enrich_phd', 'manual', 'RUNNING')
        RETURNING run_id
        """
    )
    return str(cur.fetchone()[0])


def finish_run(cur, run_id: str, status: str, error: Optional[str] = None):
    cur.execute(
        """
        UPDATE etl.runs
        SET status=%s, finished_at=now(), error_message=%s
        WHERE run_id=%s
        """,
        (status, error, run_id),
    )


def get_prompt_template() -> str:
    path = os.path.join("prompts", f"{PROMPT_VERSION}.txt")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def render_roles(roles: List[Dict[str, Any]]) -> str:
    lines = []
    for r in roles:
        lines.append(
            f"- {r.get('title','')} | {r.get('company','')} | {r.get('dates','')} | {r.get('location','')}"
        )
    return "\n".join(lines)


def extract_json_candidate(text: str) -> str:
    fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if fenced:
        return fenced[0].strip()
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1].strip()
    return text.strip()


def ollama_chat(model: str, prompt: str) -> str:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"temperature": 0.0},
    }

    last_error: Optional[Exception] = None
    for attempt in range(1, OLLAMA_MAX_RETRIES + 1):
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json=payload,
                timeout=OLLAMA_TIMEOUT_SEC,
            )
            r.raise_for_status()
            data = r.json()
            return data["message"]["content"]
        except Exception as e:
            last_error = e
            if attempt < OLLAMA_MAX_RETRIES:
                print(f"Ollama call failed (attempt {attempt}/{OLLAMA_MAX_RETRIES}): {e}")
            else:
                print(f"Ollama call failed (attempt {attempt}/{OLLAMA_MAX_RETRIES}), giving up: {e}")

    raise last_error if last_error is not None else RuntimeError("Ollama call failed")


def count_attempts(cur, document_id: str) -> int:
    """
    Counts attempts for this doc for the same model+prompt_version, including:
      - status='FAILED'
      - status='SUCCESS' but validated=false (schema-noncompliant output)
    """
    cur.execute(
        """
        SELECT count(*)
        FROM etl.llm_calls c
        WHERE c.document_id = %s
          AND c.model = %s
          AND c.prompt_version = %s
          AND (c.status = 'FAILED' OR (c.status = 'SUCCESS' AND c.validated = false))
        """,
        (document_id, MODEL, PROMPT_VERSION),
    )
    return int(cur.fetchone()[0])


def select_documents(cur) -> List[str]:
    """
    Documents to process:
      - PARSED
      - NEEDS_REVIEW
      - FAILED but only if attempts < MAX_DOC_ATTEMPTS
    """
    cur.execute(
        """
        SELECT d.document_id
        FROM etl.documents d
        WHERE d.status IN ('PARSED', 'NEEDS_REVIEW')
           OR (
                d.status = 'FAILED'
                AND (
                    SELECT count(*)
                    FROM etl.llm_calls c
                    WHERE c.document_id = d.document_id
                      AND c.model = %s
                      AND c.prompt_version = %s
                      AND (c.status = 'FAILED' OR (c.status='SUCCESS' AND c.validated=false))
                ) < %s
           )
        ORDER BY d.ingested_at DESC
        """,
        (MODEL, PROMPT_VERSION, MAX_DOC_ATTEMPTS),
    )
    return [row[0] for row in cur.fetchall()]


def upsert_llm_call(
    cur,
    *,
    run_id: str,
    document_id: str,
    input_hash: str,
    started: datetime,
    ended: datetime,
    latency_ms: int,
    response_text: Optional[str],
    response_json: Optional[dict],
    validated: bool,
    validation_errors: Optional[str],
    status: str,
    error_message: Optional[str],
) -> str:
    cur.execute(
        """
        INSERT INTO etl.llm_calls(
          run_id, document_id, model, prompt_version, input_hash_sha256,
          parameters, status, started_at, ended_at, latency_ms,
          response_text, response_json, validated, validation_errors, error_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (document_id, model, prompt_version, input_hash_sha256)
        DO UPDATE SET
          run_id = EXCLUDED.run_id,
          parameters = EXCLUDED.parameters,
          status = EXCLUDED.status,
          started_at = EXCLUDED.started_at,
          ended_at = EXCLUDED.ended_at,
          latency_ms = EXCLUDED.latency_ms,
          response_text = EXCLUDED.response_text,
          response_json = EXCLUDED.response_json,
          validated = EXCLUDED.validated,
          validation_errors = EXCLUDED.validation_errors,
          error_message = EXCLUDED.error_message
        RETURNING llm_call_id
        """,
        (
            run_id,
            document_id,
            MODEL,
            PROMPT_VERSION,
            input_hash,
            json.dumps({"temperature": 0.0}),
            status,
            started,
            ended,
            latency_ms,
            response_text,
            json.dumps(response_json) if response_json is not None else None,
            validated,
            validation_errors,
            error_message,
        ),
    )
    return str(cur.fetchone()[0])


def main() -> int:
    load_dotenv(".env")

    dsn = build_postgres_db_url(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
    )
    if not dsn:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        return 1

    print("Starting LLM enrichment...")
    prompt_template = get_prompt_template()

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            run_id = create_run(cur)
            conn.commit()
            print(f"Run created: run_id={run_id}")

            try:
                doc_ids = select_documents(cur)
                print(f"Documents to process: {len(doc_ids)}")

                for document_id in doc_ids:
                    # Skip if attempts exhausted (extra guard)
                    attempts = count_attempts(cur, document_id)
                    if attempts >= MAX_DOC_ATTEMPTS:
                        print(f"Skipping document_id={document_id}: attempts exhausted ({attempts}/{MAX_DOC_ATTEMPTS})")
                        continue

                    print(f"Processing document_id={document_id}")

                    cur.execute(
                        """
                        SELECT title, company, employment_type, dates, location
                        FROM staging.work_experience_raw
                        WHERE document_id=%s
                        ORDER BY role_index
                        """,
                        (document_id,),
                    )
                    rows = cur.fetchall()
                    if not rows:
                        print(f"Skipping document_id={document_id}: no work_experience_raw rows")
                        continue

                    roles = [
                        {"title": t, "company": c, "employment_type": et, "dates": d, "location": l}
                        for (t, c, et, d, l) in rows
                    ]
                    roles_block = render_roles(roles)
                    prompt = prompt_template.replace("{{ROLES}}", roles_block)
                    input_hash = sha256_text(prompt)

                    # Idempotency: skip if already succeeded+validated
                    cur.execute(
                        """
                        SELECT 1
                        FROM etl.llm_calls
                        WHERE document_id=%s
                          AND model=%s
                          AND prompt_version=%s
                          AND input_hash_sha256=%s
                          AND status='SUCCESS'
                          AND validated=true
                        LIMIT 1
                        """,
                        (document_id, MODEL, PROMPT_VERSION, input_hash),
                    )
                    if cur.fetchone():
                        print(f"Skipping document_id={document_id}: already validated")
                        continue

                    started = datetime.now(timezone.utc)
                    try:
                        print(f"Calling Ollama for document_id={document_id}")
                        response_text = ollama_chat(MODEL, prompt)
                        ended = datetime.now(timezone.utc)
                        latency_ms = int((ended - started).total_seconds() * 1000)
                        print(f"Ollama response received: document_id={document_id} latency_ms={latency_ms}")

                        # Parse + validate
                        validated = False
                        validation_errors = None
                        response_json = None
                        parsed: Optional[PhDStatus] = None

                        try:
                            response_json = json.loads(extract_json_candidate(response_text))
                            parsed = PhDStatus(**response_json)
                            # Enforce NOT NULL expectation before DB write:
                            if parsed.is_current_phd is None:
                                raise ValueError("is_current_phd must be true/false (not null)")
                            validated = True
                        except (json.JSONDecodeError, ValidationError, ValueError) as ve:
                            validation_errors = str(ve)
                            validated = False
                            parsed = None
                            preview = (response_text or "")[:500].replace("\n", "\\n")
                            print(
                                f"Invalid / schema-noncompliant response preview: "
                                f"document_id={document_id} preview='{preview}'"
                            )

                        print(f"Validation result: document_id={document_id} validated={validated}")

                        # Store llm_call: invalid output counts as FAILED so retries work
                        call_status = "SUCCESS" if validated else "FAILED"
                        call_error_message = None if validated else f"Validation failed: {validation_errors}"

                        llm_call_id = upsert_llm_call(
                            cur,
                            run_id=run_id,
                            document_id=document_id,
                            input_hash=input_hash,
                            started=started,
                            ended=ended,
                            latency_ms=latency_ms,
                            response_text=response_text,
                            response_json=response_json,
                            validated=validated,
                            validation_errors=validation_errors,
                            status=call_status,
                            error_message=call_error_message,
                        )

                        if validated and parsed is not None:
                            # Upsert into core.phd_status (safe: is_current_phd is not None here)
                            cur.execute(
                                """
                                INSERT INTO core.phd_status(
                                  document_id, run_id, model, prompt_version,
                                  is_current_phd, current_employer, current_title, since_month,
                                  evidence, source_llm_call_id, updated_at
                                )
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                                ON CONFLICT (document_id) DO UPDATE SET
                                  run_id=EXCLUDED.run_id,
                                  model=EXCLUDED.model,
                                  prompt_version=EXCLUDED.prompt_version,
                                  is_current_phd=EXCLUDED.is_current_phd,
                                  current_employer=EXCLUDED.current_employer,
                                  current_title=EXCLUDED.current_title,
                                  since_month=EXCLUDED.since_month,
                                  evidence=EXCLUDED.evidence,
                                  source_llm_call_id=EXCLUDED.source_llm_call_id,
                                  updated_at=now()
                                """,
                                (
                                    document_id,
                                    run_id,
                                    MODEL,
                                    PROMPT_VERSION,
                                    parsed.is_current_phd,
                                    parsed.current_employer,
                                    parsed.current_title,
                                    parsed.since_month,
                                    parsed.evidence,
                                    llm_call_id,
                                ),
                            )
                            cur.execute(
                                "UPDATE etl.documents SET status='ENRICHED', error_message=NULL WHERE document_id=%s",
                                (document_id,),
                            )
                            print(f"Document enriched: document_id={document_id}")
                        else:
                            # Mark FAILED so it is eligible for retry (until MAX_DOC_ATTEMPTS)
                            cur.execute(
                                "UPDATE etl.documents SET status='FAILED', error_message=%s WHERE document_id=%s",
                                (f"LLM output invalid: {validation_errors}", document_id),
                            )
                            print(f"Document failed (invalid output): document_id={document_id} error={validation_errors}")

                        conn.commit()

                    except Exception as e:
                        # Make sure we can write failure records
                        conn.rollback()
                        ended = datetime.now(timezone.utc)
                        latency_ms = int((ended - started).total_seconds() * 1000)

                        # Record FAILED call (no ON CONFLICT here; if same hash already exists, we update it)
                        try:
                            _ = upsert_llm_call(
                                cur,
                                run_id=run_id,
                                document_id=document_id,
                                input_hash=input_hash,
                                started=started,
                                ended=ended,
                                latency_ms=latency_ms,
                                response_text=None,
                                response_json=None,
                                validated=False,
                                validation_errors=None,
                                status="FAILED",
                                error_message=str(e),
                            )
                        except Exception as inner:
                            # If even logging fails, don't kill the whole run; just surface it.
                            conn.rollback()
                            print(
                                f"ERROR: Could not log llm_call failure for document_id={document_id}: {inner}",
                                file=sys.stderr,
                            )

                        # Mark document failed
                        try:
                            cur.execute(
                                "UPDATE etl.documents SET status='FAILED', error_message=%s WHERE document_id=%s",
                                (str(e), document_id),
                            )
                            conn.commit()
                        except Exception as inner2:
                            conn.rollback()
                            print(
                                f"ERROR: Could not update etl.documents to FAILED for document_id={document_id}: {inner2}",
                                file=sys.stderr,
                            )

                        print(f"Document failed: document_id={document_id} error={e}", file=sys.stderr)
                        continue  # do not abort the whole run on a single doc

                # run success even if some docs failed; failures are tracked per-doc
                finish_run(cur, run_id, "SUCCESS", None)
                conn.commit()
                print(f"✅ LLM enrichment completed: run_id={run_id}")
                return 0

            except Exception as e:
                # The run-level failure handler MUST rollback first, otherwise you get InFailedSqlTransaction
                conn.rollback()
                try:
                    finish_run(cur, run_id, "FAILED", str(e))
                    conn.commit()
                except Exception:
                    conn.rollback()
                print(f"❌ LLM enrichment failed: run_id={run_id} error={e}", file=sys.stderr)
                return 1


if __name__ == "__main__":
    raise SystemExit(main())

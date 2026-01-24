import os
import sys
import json
import hashlib
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import psycopg2
from psycopg2 import errors as pg_errors
import requests
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from staging_tools import build_postgres_db_url


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL = "llama3.1:8b-instruct-q4_0"  # small/fast q4 model :contentReference[oaicite:3]{index=3}
PROMPT_VERSION = "phd_status_v1"
OLLAMA_TIMEOUT_SEC = int(os.getenv("OLLAMA_TIMEOUT_SEC", "240"))
OLLAMA_MAX_RETRIES = int(os.getenv("OLLAMA_MAX_RETRIES", "2"))


class PhDStatus(BaseModel):
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
        UPDATE etl.runs SET status=%s, finished_at=now(), error_message=%s
        WHERE run_id=%s
        """,
        (status, error, run_id),
    )


def get_prompt_template() -> str:
    path = os.path.join("prompts", "phd_status_v1.txt")
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
        return text[start:end + 1].strip()
    return text.strip()


def ollama_chat(model: str, prompt: str) -> str:
    # /api/chat is documented by Ollama library page :contentReference[oaicite:4]{index=4}
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "stream": False,
        "options": {
            "temperature": 0.0
        }
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

    print("Starting LLM enrichment...")
    prompt_template = get_prompt_template()

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            run_id = create_run(cur)
            conn.commit()
            print(f"Run created: run_id={run_id}")

            try:
                # Get documents that have parsed experiences
                cur.execute(
                    """
                    SELECT d.document_id
                    FROM etl.documents d
                    WHERE d.status IN ('PARSED','ENRICHED','NEEDS_REVIEW')
                    ORDER BY d.ingested_at DESC
                    """
                )
                doc_ids = [row[0] for row in cur.fetchall()]
                print(f"Documents to process: {len(doc_ids)}")

                for document_id in doc_ids:
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

                    # Idempotency: skip if already succeeded
                    cur.execute(
                        """
                        SELECT llm_call_id, validated
                        FROM etl.llm_calls
                        WHERE document_id=%s AND model=%s AND prompt_version=%s AND input_hash_sha256=%s
                          AND status='SUCCESS' AND validated=true
                        """,
                        (document_id, MODEL, PROMPT_VERSION, input_hash),
                    )
                    if cur.fetchone():
                        print(f"Skipping document_id={document_id}: already validated")
                        continue

                    # Call Ollama
                    started = datetime.now(timezone.utc)
                    try:
                        print(f"Calling Ollama for document_id={document_id}")
                        response_text = ollama_chat(MODEL, prompt)
                        ended = datetime.now(timezone.utc)
                        latency_ms = int((ended - started).total_seconds() * 1000)
                        print(f"Ollama response received: document_id={document_id} latency_ms={latency_ms}")

                        # Try parse JSON
                        validated = False
                        validation_errors = None
                        response_json = None

                        try:
                            response_json = json.loads(extract_json_candidate(response_text))
                            parsed = PhDStatus(**response_json)
                            validated = True
                        except Exception as ve:
                            validation_errors = str(ve)
                            parsed = None
                            preview = response_text[:500].replace("\n", "\\n")
                            print(f"Invalid JSON response preview: document_id={document_id} preview='{preview}'")
                        print(f"Validation result: document_id={document_id} validated={validated}")

                        # Write llm_calls
                        cur.execute(
                            """
                            INSERT INTO etl.llm_calls(
                              run_id, document_id, model, prompt_version, input_hash_sha256,
                              parameters, status, started_at, ended_at, latency_ms,
                              response_text, response_json, validated, validation_errors
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,'SUCCESS',%s,%s,%s,%s,%s,%s,%s)
                            RETURNING llm_call_id
                            """,
                            (
                                run_id, document_id, MODEL, PROMPT_VERSION, input_hash,
                                json.dumps({"temperature": 0.0}),
                                started, ended, latency_ms,
                                response_text,
                                json.dumps(response_json) if response_json is not None else None,
                                validated,
                                validation_errors,
                            ),
                        )
                        llm_call_id = cur.fetchone()[0]
                        inserted_llm_call = True

                        # If valid, upsert into core.phd_status
                        if validated and parsed is not None:
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
                                    document_id, run_id, MODEL, PROMPT_VERSION,
                                    parsed.is_current_phd,
                                    parsed.current_employer,
                                    parsed.current_title,
                                    parsed.since_month,
                                    parsed.evidence,
                                    llm_call_id,
                                ),
                            )

                            # mark doc enriched
                            cur.execute(
                                "UPDATE etl.documents SET status='ENRICHED', error_message=NULL WHERE document_id=%s",
                                (document_id,),
                            )
                            print(f"Document enriched: document_id={document_id}")
                        else:
                            # invalid output: needs review
                            cur.execute(
                                "UPDATE etl.documents SET status='NEEDS_REVIEW', error_message=%s WHERE document_id=%s",
                                (f"LLM JSON invalid: {validation_errors}", document_id),
                            )
                            print(f"Document needs review: document_id={document_id} error={validation_errors}")

                        conn.commit()

                    except pg_errors.UniqueViolation:
                        conn.rollback()
                        print(
                            f"Duplicate llm_calls entry; reusing existing record for document_id={document_id}"
                        )
                        cur.execute(
                            """
                            SELECT llm_call_id
                            FROM etl.llm_calls
                            WHERE document_id=%s AND model=%s AND prompt_version=%s AND input_hash_sha256=%s
                            ORDER BY ended_at DESC
                            LIMIT 1
                            """,
                            (document_id, MODEL, PROMPT_VERSION, input_hash),
                        )
                        row = cur.fetchone()
                        if not row:
                            raise
                        llm_call_id = row[0]
                        if validated and parsed is not None:
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
                                    document_id, run_id, MODEL, PROMPT_VERSION,
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
                            cur.execute(
                                "UPDATE etl.documents SET status='NEEDS_REVIEW', error_message=%s WHERE document_id=%s",
                                (f"LLM JSON invalid: {validation_errors}", document_id),
                            )
                            print(f"Document needs review: document_id={document_id} error={validation_errors}")
                        conn.commit()

                    except Exception as e:
                        conn.rollback()
                        ended = datetime.now(timezone.utc)
                        latency_ms = int((ended - started).total_seconds() * 1000)

                        cur.execute(
                            """
                            INSERT INTO etl.llm_calls(
                              run_id, document_id, model, prompt_version, input_hash_sha256,
                              parameters, status, error_message, started_at, ended_at, latency_ms,
                              response_text, validated
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,'FAILED',%s,%s,%s,%s,%s,false)
                            """,
                            (
                                run_id, document_id, MODEL, PROMPT_VERSION, input_hash,
                                json.dumps({"temperature": 0.0}),
                                str(e),
                                started, ended, latency_ms,
                                None,
                            ),
                        )
                        cur.execute(
                            "UPDATE etl.documents SET status='FAILED', error_message=%s WHERE document_id=%s",
                            (str(e), document_id),
                        )
                        conn.commit()
                        print(f"Document failed: document_id={document_id} error={e}", file=sys.stderr)

                finish_run(cur, run_id, "SUCCESS", None)
                conn.commit()
                print(f"✅ LLM enrichment completed: run_id={run_id}")
                return 0

            except Exception as e:
                finish_run(cur, run_id, "FAILED", str(e))
                conn.commit()
                print(f"❌ LLM enrichment failed: run_id={run_id} error={e}", file=sys.stderr)
                return 1


if __name__ == "__main__":
    raise SystemExit(main())

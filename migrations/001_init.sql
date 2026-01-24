-- Schemas (safe if you already created them in phase 0; for idempotency)
CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS quarantine;


CREATE EXTENSION IF NOT EXISTS pgcrypto; -- for generating UUIDs


-- 1) Runs: one row per pipeline execution
CREATE TABLE IF NOT EXISTS etl.runs (
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name TEXT NOT NULL DEFAULT 'main',
  triggered_by TEXT NOT NULL DEFAULT 'manual',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_started_at ON etl.runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status ON etl.runs (status);


-- 2) File manifest: tracks input CSVs (and their processing state)
CREATE TABLE IF NOT EXISTS etl.file_manifest (
  file_id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES etl.runs(run_id) ON DELETE CASCADE,
  source_type TEXT NOT NULL DEFAULT 'csv' CHECK (source_type IN ('csv')),
  file_path TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_hash_sha256 TEXT NOT NULL,
  file_size_bytes BIGINT,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL CHECK (status IN ('NEW','LOADED','SKIPPED','QUARANTINED','FAILED')),
  rows_loaded BIGINT,
  error_message TEXT,

  UNIQUE (file_hash_sha256)  -- file-level idempotency: same file won't be processed twice unless you override logic
);

CREATE INDEX IF NOT EXISTS idx_file_manifest_run ON etl.file_manifest (run_id);
CREATE INDEX IF NOT EXISTS idx_file_manifest_status ON etl.file_manifest (status);


-- 3) Step metrics: row counts + durations per step per run
CREATE TABLE IF NOT EXISTS etl.step_metrics (
  metric_id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES etl.runs(run_id) ON DELETE CASCADE,
  step_name TEXT NOT NULL, -- e.g. 'discover_files', 'copy_to_staging.students'
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT,
  rows_in BIGINT,
  rows_out BIGINT,
  status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_step_metrics_run ON etl.step_metrics (run_id);
CREATE INDEX IF NOT EXISTS idx_step_metrics_step ON etl.step_metrics (step_name);


-- 4) Validation results: DQ checks per run/table/check
CREATE TABLE IF NOT EXISTS etl.validation_results (
  validation_id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES etl.runs(run_id) ON DELETE CASCADE,
  object_name TEXT NOT NULL,        -- e.g. 'staging.students', 'core.publications'
  check_name TEXT NOT NULL,         -- e.g. 'not_null.matricola'
  status TEXT NOT NULL CHECK (status IN ('PASS','FAIL','WARN')),
  observed_value NUMERIC,
  expected_value NUMERIC,
  details TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_validation_run ON etl.validation_results (run_id);
CREATE INDEX IF NOT EXISTS idx_validation_object ON etl.validation_results (object_name);


-- 5) Documents registry: tracks PDFs (or other docs)
CREATE TABLE IF NOT EXISTS etl.documents (
  document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID REFERENCES etl.runs(run_id) ON DELETE SET NULL,
  source_path TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_hash_sha256 TEXT NOT NULL,
  doc_type TEXT NOT NULL DEFAULT 'pdf',
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL CHECK (status IN ('NEW','PARSED','ENRICHED','NEEDS_REVIEW','FAILED')),
  error_message TEXT,

  UNIQUE (file_hash_sha256)
);

CREATE INDEX IF NOT EXISTS idx_documents_status ON etl.documents (status);


-- 6) LLM calls: operational record of every call (for reproducibility + cost/latency + idempotency)
CREATE TABLE IF NOT EXISTS etl.llm_calls (
  llm_call_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID REFERENCES etl.runs(run_id) ON DELETE SET NULL,
  document_id UUID NOT NULL REFERENCES etl.documents(document_id) ON DELETE CASCADE,

  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  input_hash_sha256 TEXT NOT NULL,   -- hash of the exact text you sent
  parameters JSONB NOT NULL DEFAULT '{}'::jsonb,

  status TEXT NOT NULL CHECK (status IN ('SUCCESS','FAILED')),
  error_message TEXT,

  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  latency_ms BIGINT,

  -- raw + parsed outputs
  response_text TEXT,
  response_json JSONB,
  validated BOOLEAN NOT NULL DEFAULT FALSE,
  validation_errors TEXT,

  UNIQUE (document_id, model, prompt_version, input_hash_sha256) -- LLM idempotency key
);


CREATE INDEX IF NOT EXISTS idx_llm_calls_doc ON etl.llm_calls (document_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON etl.llm_calls (run_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_status ON etl.llm_calls (status);
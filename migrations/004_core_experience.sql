CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.phd_status (
  document_id UUID PRIMARY KEY,
  run_id UUID,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  is_current_phd BOOLEAN NOT NULL,
  current_employer TEXT,
  current_title TEXT,
  since_month TEXT, -- keep as YYYY-MM for now
  evidence TEXT,    -- short string referencing the role line(s)
  source_llm_call_id UUID,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_phd_status_run ON core.phd_status(run_id);

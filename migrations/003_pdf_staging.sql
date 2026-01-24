CREATE SCHEMA IF NOT EXISTS staging;

-- Store raw extracted text per page (debuggable & reproducible)
CREATE TABLE IF NOT EXISTS staging.pdf_pages (
  run_id UUID NOT NULL,
  document_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  page_number INT NOT NULL,
  text TEXT,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (document_id, page_number)
);

CREATE INDEX IF NOT EXISTS idx_pdf_pages_run ON staging.pdf_pages(run_id);
CREATE INDEX IF NOT EXISTS idx_pdf_pages_doc ON staging.pdf_pages(document_id);

-- Store the deterministic parsed experiences (pre-LLM)
-- dates/location are still text here; core typing comes later (phase 5/6)
CREATE TABLE IF NOT EXISTS staging.work_experience_raw (
  run_id UUID NOT NULL,
  document_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  role_index INT NOT NULL,
  title TEXT,
  company TEXT,
  employment_type TEXT,
  dates TEXT,
  location TEXT,
  extracted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (document_id, role_index)
);

CREATE INDEX IF NOT EXISTS idx_work_exp_run ON staging.work_experience_raw(run_id);
CREATE INDEX IF NOT EXISTS idx_work_exp_doc ON staging.work_experience_raw(document_id);

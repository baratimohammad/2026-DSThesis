{{ config(materialized='table') }}

WITH ranked AS (
  SELECT
    ps.is_current_phd,
    split_part(wer.source_file, ' _', 1) AS nome_cognome,
    ps.current_employer,
    ps.current_title,
    ps.since_month,
    ps.evidence,
    ROW_NUMBER() OVER (
      PARTITION BY wer.source_file
      ORDER BY ps.since_month DESC
    ) AS rn
  FROM core.phd_status ps
  LEFT JOIN staging.work_experience_raw wer
    ON ps.document_id = wer.document_id
),
final AS (
  SELECT
    nome_cognome,
    is_current_phd,
    COALESCE(NULLIF(TRIM(current_employer), ''), 'No experience section in linkedin page') AS current_employer,
    COALESCE(NULLIF(TRIM(current_title), ''), 'No experience section in linkedin page') AS current_title,
    COALESCE(NULLIF(TRIM(since_month), ''), 'No experience section in linkedin page') AS since_month,
    evidence
  FROM ranked
  WHERE rn = 1
)
SELECT
  s.matricola_dottorando as matricola,
  s.nome,
  s.cognome,
  s.ciclo,
  f.is_current_phd,
  CASE WHEN f.nome_cognome IS NULL THEN 'Unknown' ELSE f.current_employer END AS employer,
  CASE WHEN f.nome_cognome IS NULL THEN 'Unknown' ELSE f.current_title    END AS title,
  CASE WHEN f.nome_cognome IS NULL THEN 'Unknown' ELSE f.since_month      END AS since,
  f.evidence as "linkedin_exact_phrasing"
FROM core.students s
LEFT JOIN final f
  ON lower(regexp_replace(trim(s.nome || ' ' || s.cognome), '\s+', ' ', 'g'))
   = lower(regexp_replace(trim(f.nome_cognome), '\s+', ' ', 'g'))

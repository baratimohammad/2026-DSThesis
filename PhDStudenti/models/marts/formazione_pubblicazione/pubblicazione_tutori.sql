{{ config(materialized='table') }}

WITH students AS (
  SELECT
    trim(lower(nome))    AS nome,
    trim(lower(cognome)) AS cognome
  FROM core.students
),
tutors AS (
  SELECT p.*
  FROM core.publications p
  WHERE NOT EXISTS (
    SELECT 1
    FROM students s
    WHERE s.cognome = trim(lower(p.autore_cognome))
      AND left(regexp_replace(s.nome, '\.', '', 'g'), 1)
          = left(regexp_replace(trim(lower(p.autore_nome)), '\.', '', 'g'), 1)
  )
)
SELECT
  (autore_cognome || ' ' || left(regexp_replace(autore_nome, '\.', '', 'g'), 1) || '.') AS autore,
  anno::text AS anno,
  COUNT(*) AS n_pub,
  STRING_AGG(titolo, '; ' ORDER BY titolo) AS titoli
FROM tutors
GROUP BY
  (autore_cognome || ' ' || left(regexp_replace(autore_nome, '\.', '', 'g'), 1) || '.'),
  anno
ORDER BY n_pub DESC

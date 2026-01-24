-- migrations/002_staging_tables.sql
CREATE SCHEMA IF NOT EXISTS staging;

-- STUDENTS (cycle-specific file)
CREATE TABLE IF NOT EXISTS staging.students (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ciclo TEXT, -- derive from path cicli/<ciclo>/
  matricola_dottorando TEXT,
  matricola_dipendente_docente TEXT,
  email TEXT,
  cognome TEXT,
  nome TEXT,
  tutore TEXT,
  co_tutore TEXT,
  status TEXT,
  ore_soft_skills TEXT,
  ore_hard_skills TEXT,
  punti_soft_skills TEXT,
  punti_hard_skills TEXT,
  punti_attivita_fuorisede TEXT,
  punti_totali TEXT
);

-- Per-student activity tables (matricola from filename)
CREATE TABLE IF NOT EXISTS staging.attivita_formative_esterne (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  matricola TEXT,
  ciclo TEXT,
  denominazione TEXT,
  ore_dichiarate TEXT,
  ore_riconosciute TEXT,
  ore_calcolate TEXT,
  coeff_voto TEXT,
  punti TEXT,
  tipo_form TEXT,
  tipo_richiesta TEXT,
  liv_esame TEXT,
  data_attivita TEXT,
  data_convalida TEXT
);

CREATE TABLE IF NOT EXISTS staging.attivita_formative_fuorisede (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  matricola TEXT,
  ciclo TEXT,
  denominazione TEXT,
  luogo TEXT,
  ente TEXT,
  periodo TEXT,
  data_autorizzazione TEXT,
  data_aut_pagamento TEXT,
  data_attestaz TEXT,
  data_convalida TEXT
);

CREATE TABLE IF NOT EXISTS staging.attivita_formative_interne (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  matricola TEXT,
  ciclo TEXT,
  cod_ins TEXT,
  nome_insegnamento TEXT,
  ore TEXT,
  ore_riconosciute TEXT,
  voto TEXT,
  coeff_voto TEXT,
  data_esame TEXT,
  tipo_form TEXT,
  liv_esame TEXT,
  tipo_attivita TEXT,
  punti TEXT
);

CREATE TABLE IF NOT EXISTS staging.pubblicazioni (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  matricola TEXT,
  ciclo TEXT,
  anno TEXT,
  tipo TEXT,
  titolo TEXT,
  rivista TEXT,
  autori TEXT,
  convegno TEXT,
  referee TEXT,
  grado_proprieta_dottorandi TEXT,
  punteggio TEXT,
  grado_proprieta TEXT,
  indicatore_r TEXT,
  errore_val TEXT
);

-- CORSI (codInsegnamento + year from filename)
CREATE TABLE IF NOT EXISTS staging.corsi (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cod_ins TEXT,
  anno TEXT,
  matricola TEXT,
  cognome TEXT,
  nome TEXT,
  cod_insegnamento_col TEXT,
  cod_corso_dottorato TEXT,
  periodo_didattico TEXT,
  dash_col TEXT,
  stato TEXT
);

CREATE TABLE IF NOT EXISTS staging.dettaglio_corso (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cod_ins TEXT,
  anno TEXT,
  type TEXT,
  teacher TEXT,
  status TEXT,
  ssd TEXT,
  h_les TEXT,
  h_ex TEXT,
  h_lab TEXT,
  h_tut TEXT,
  years_teaching TEXT
);

-- journal_details per ciclo file
CREATE TABLE IF NOT EXISTS staging.journal_details (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ciclo TEXT,
  matricola TEXT,
  cognome TEXT,
  nome TEXT,
  titolo TEXT,
  rivista TEXT,
  issn TEXT,
  anno TEXT,
  quartile TEXT
);

CREATE TABLE IF NOT EXISTS staging.ore_formazione (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ciclo TEXT,
  matricola TEXT,
  cognome TEXT,
  nome TEXT,
  tutor TEXT,
  ore_soft_skill TEXT,
  ore_hard_skill TEXT,
  ore_totali TEXT
);

CREATE TABLE IF NOT EXISTS staging.stat_pubb (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ciclo TEXT,
  matricola TEXT,
  cognome TEXT,
  nome TEXT,
  numero_journal TEXT,
  numero_conferenze TEXT,
  numero_capitoli TEXT,
  numero_poster TEXT,
  numero_abstract TEXT,
  numero_brevetti TEXT,
  quartile_1 TEXT,
  quartile_2 TEXT,
  quartile_3 TEXT,
  quartile_4 TEXT,
  quartile_5 TEXT,
  quartile_6 TEXT,
  quartile_7 TEXT,
  quartile_8 TEXT,
  quartile_9 TEXT,
  quartile_10 TEXT,
  quartile_11 TEXT,
  quartile_12 TEXT,
  quartile_13 TEXT,
  quartile_14 TEXT,
  quartile_15 TEXT
);

CREATE TABLE IF NOT EXISTS staging.collaborazioni_dettaglio (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  matricola_dott TEXT,
  cognome TEXT,
  nome TEXT,
  ciclo TEXT,
  tutor TEXT,
  ore TEXT,
  tipo_attivita TEXT,
  materia TEXT,
  docente TEXT,
  corso_di_laurea TEXT
);

CREATE TABLE IF NOT EXISTS staging.filtered_iu_stats (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cod_ins TEXT,
  nome_insegnamento TEXT,
  docente TEXT,
  iscritti_2025 TEXT,
  superati_2025 TEXT,
  iis_iscritti_2025 TEXT,
  iis_superati_2025 TEXT,
  iscritti_2024 TEXT,
  superati_2024 TEXT,
  iis_iscritti_2024 TEXT,
  iis_superati_2024 TEXT,
  iscritti_2023 TEXT,
  superati_2023 TEXT,
  iis_iscritti_2023 TEXT,
  iis_superati_2023 TEXT,
  iscritti_2022 TEXT,
  superati_2022 TEXT,
  iis_iscritti_2022 TEXT,
  iis_superati_2022 TEXT
);

CREATE TABLE IF NOT EXISTS staging.mobilita_internazionale_con_studenti (
  run_id UUID NOT NULL,
  source_file TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  matricola TEXT, -- you said it's empty; keep anyway
  cognome TEXT,
  nome TEXT,
  tutore TEXT,
  ciclo TEXT,
  tipo TEXT,
  paese TEXT,
  ente TEXT,
  periodo TEXT,
  durata_giorni TEXT,
  anno TEXT,
  data_autorizzazione TEXT,
  data_pagamento TEXT
);

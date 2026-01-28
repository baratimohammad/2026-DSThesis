# Project Overview

This repository contains the final thesis project for the Master’s Degree in Digital Skills for Sustainable Societal Transitions at Politecnico di Torino (DAUIN Department).

**Thesis title:** Design of a Containerized Dashboard Platform and AI-Assisted Data Enrichment for Student Career Monitoring

A fully containerized stack that ingests, transforms, enriches, and visualizes academic data to support data-driven monitoring of PhD student careers.

## Table of Contents
- [Project Overview](#project-overview)
- [Objective](#objective)
- [Data Sources](#data-sources)
- [Architecture](#architecture)
  - [Architectural Layers](#architectural-layers)
- [Dashboards](#dashboards)
- [Project Setup](#project-setup)
  - [Input Data Requirements](#input-data-requirements)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Start Services](#start-services)
  - [Initialize Database](#initialize-database)
  - [Data Ingestion](#data-ingestion)
  - [LLM Enrichment](#llm-enrichment)
  - [Run dbt commands (from repo root)](#run-dbt-commands-from-repo-root)
  - [Grafana](#grafana)
  - [Stop Services](#stop-services)

## Objective

Enable structured, repeatable, and transparent analysis of PhD students’ academic careers to support departmental decision-making. The system answers questions about:

- Academic and professional activities over time
- Research output and publication trends per PhD cycle
- Participation in international mobility programs and their durations
- Collaboration patterns across activities and academic years
- Supervisors’ publication activity
- Core information about departmental courses

## Data Sources

- CSV files (structured data)
- PDFs (unstructured data enriched via AI-assisted extraction)

All data is curated and normalized before analysis.

# Architecture

![High-level architecture](jpg/Architecture.jpg)

*Figure 1 — Containerized, layered data architecture spanning ingestion through presentation.*

The system uses a layered design to separate ingestion, transformation, and presentation. All components run in Docker on Ubuntu.

## Architectural Layers
### Extraction Layer
- Python ingestion of CSV and PDF sources
- Early parsing and validation

### Staging Layer
- Python services populate the staging schema
- Ollama enriches unstructured data (AI-assisted extraction)
- Cleaning, normalization, and schema alignment

### Data Warehouse Layer
- dbt builds the core schema
- Business logic produces analytics-ready tables

### Data Mart Layer
- dbt publishes marts optimized for dashboards
- Organized by analytical domain (careers, publications, mobility, etc.)

### Data Serving Layer
- Grafana dashboards with filters for cycle, year, course, and mobility duration

### Storage Layer
- PostgreSQL underpins staging, warehouse, and mart schemas

### Containerization & Execution Model
- All services run in Docker (Ubuntu-based images)
- Version-controlled for reproducible builds
- Designed for annual runs with curated source files following the documented formats


# Dashboards
Example dashboards produced by the platform:
- ![Distribuzione delle ore](jpg/sample_distribuzione_delle_ore.png)
- ![Iscritti vs superati](jpg/sample_iscritti_superati.png)
- ![Mobilità internazionale](jpg/sample_mobilita_internazionale.png)
- ![Ore di formazione](jpg/sample_ore_formazione.png)
- ![Pubblicazioni](jpg/sample_pubblicazioni.png)


# Project Setup

This repository runs a data pipeline backed by Postgres, Grafana, and Ollama via Docker Compose.

## Input Data Requirements

Place input files under `./data/input` with the following structure and naming conventions:

```
data/input/
  cicli/<ciclo>/
    0_students_info.csv
    <matricola>_attivita_formative_interne.csv
    <matricola>_attivita_formative_esterne.csv
    <matricola>_attivita_formative_fuorisede.csv
    <matricola>_pubblicazioni.csv
  corsi/
    <COD_INS>_<YYYY>.csv
    dettaglio_corso_<COD_INS>_<YYYY>.csv
  journal_details/
    journal_details_<ciclo>.csv
  ore_formazione/
    ore_formazione_ciclo_<ciclo>.csv
  stat_pubb/
    stat_pubb_<ciclo>.csv
  collaborazioni_dettaglio.csv
  filtered_IU_stats.csv
  mobilita_internazionale_con_studenti.csv
```

Each staging loader expects specific columns. Extra columns are ignored; all listed columns should be present.

| Input file pattern | Delimiter | Required columns |
| --- | --- | --- |
| `data/input/cicli/*/0_students_info.csv` | `;` | `Matricola dottorando/a:`, `Matricola da dipendente/docente:`, `Email`, `Cognome`, `Nome`, `Ciclo`, `Tutore`, `Co-tutore`, `Status`, `Ore Soft Skills`, `Ore Hard Skills`, `Punti Soft Skills`, `Punti Hard Skills`, `Punti Attività fuorisede`, `Punti totali` |
| `data/input/cicli/*/*_attivita_formative_interne.csv` | `,` | `Cod Ins.`, `Nome insegnamento`, `Ore`, `Ore riconosciute`, `Voto`, `Coeff. voto`, `Data esame`, `Tipo form.`, `Liv. Esame`, `Tipo attività`, `Punti` |
| `data/input/cicli/*/*_attivita_formative_esterne.csv` | `\t` | `Denominazione`, `Ore dichiarate`, `Ore riconosciute`, `Ore calcolate`, `Coeff. voto`, `Punti`, `Tipo form.`, `Tipo Richiesta`, `Liv. Esame`, `Data attività`, `Data convalida` |
| `data/input/cicli/*/*_attivita_formative_fuorisede.csv` | `\t` | `Denominazione`, `Luogo`, `Ente`, `Periodo`, `Data autorizzazione`, `Data aut.pagamento`, `Data attestaz.`, `Data convalida` |
| `data/input/cicli/*/*_pubblicazioni.csv` | `\t` | `Anno`, `Tipo`, `Titolo`, `Rivista`, `Autori`, `Convegno`, `Referee`, `Grado proprietà dottorandi`, `Punteggio`, `Grado proprietà`, `Indicatore R`, `Errore Val.` |
| `data/input/corsi/*_*.csv` | `\t` | `matricola`, `cognome`, `nome`, `codInsegnamento`, `codCorsoDottorato`, `PeriodoDidattico`, `-`, `stato` |
| `data/input/corsi/dettaglio_corso_*_*.csv` | `\t` | `Type`, `Teacher`, `Status`, `SSD`, `h.Les`, `h.Ex`, `h.Lab`, `h.Tut`, `Years teaching` |
| `data/input/journal_details/journal_details_*.csv` | `,` | `matricola`, `cognome`, `nome`, `titolo`, `rivista`, `issn`, `anno`, `quartile` |
| `data/input/ore_formazione/ore_formazione_ciclo_*.csv` | `,` | `matricola`, `cognome`, `nome`, `tutor`, `ore soft skill`, `ore hard skill`, `ore totali` |
| `data/input/stat_pubb/stat_pubb_*.csv` | `,` | `matricola`, `cognome`, `nome`, `numero_journal`, `numero_conferenze`, `numero_capitoli`, `numero_poster`, `numero_abstract`, `numero_brevetti`, `quartile_1`, `quartile_2`, `quartile_3`, `quartile_4`, `quartile_5`, `quartile_6`, `quartile_7`, `quartile_8`, `quartile_9`, `quartile_10`, `quartile_11`, `quartile_12`, `quartile_13`, `quartile_14`, `quartile_15` |
| `data/input/collaborazioni_dettaglio.csv` | `,` | `Matricola Dott`, `Cognome`, `Nome`, `Ciclo`, `Tutor`, `Ore`, `Tipo Attività`, `Materia`, `Docente`, `Corso di Laurea` |
| `data/input/filtered_IU_stats.csv` | `,` | `Cod.Ins`, `Nome Insegnamento`, `Docente`, `Iscritti_2025`, `Superati_2025`, `IIS_Iscritti_2025`, `IIS_Superati_2025`, `Iscritti_2024`, `Superati_2024`, `IIS_Iscritti_2024`, `IIS_Superati_2024`, `Iscritti_2023`, `Superati_2023`, `IIS_Iscritti_2023`, `IIS_Superati_2023`, `Iscritti_2022`, `Superati_2022`, `IIS_Iscritti_2022`, `IIS_Superati_2022` |
| `data/input/mobilita_internazionale_con_studenti.csv` | `,` | `matricola`, `cognome`, `nome`, `tutore`, `ciclo`, `tipo`, `paese`, `ente`, `periodo`, `durata_giorni`, `anno`, `data_autorizzazione`, `data_pagamento` |

## Prerequisites

- Docker and Docker Compose (install them if not already installed)

### Install Docker + Docker Compose

Linux (Ubuntu/Debian):

```
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
```

## Configuration

Create or update `.env` in the project root (example below). Adjust credentials as needed, especially for shared environments.

```
POSTGRES_USER=changeadmin
POSTGRES_PASSWORD=changesecret
POSTGRES_DB=changedb
POSTGRES_PORT=5432
POSTGRES_HOST=postgres

DATABASE_URL=postgresql://changeadmin:changesecret@postgres:5432/changedb

OLLAMA_URL=http://ollama:11434
OLLAMA_NUM_CTX=2048
OLLAMA_NUM_BATCH=128
OLLAMA_NUM_PREDICT=700

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
GRAFANA_PORT=3000
```

Create dbt profile (`./PhDStudenti/.dbt/profiles.yml`):
```
mkdir -p ./PhDStudenti/.dbt

cat <<'EOF' > ./PhDStudenti/.dbt/profiles.yml
PhDStudenti:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT') | int }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: core
      threads: 1
      sslmode: disable
EOF
```




Notes:
- `POSTGRES_HOST` should be `postgres` to reach the container from other services.
- `DATABASE_URL` must match the Postgres settings above.
- Change default credentials for any shared environment.

## Start Services

```
docker compose up -d --build
```

Verify containers are healthy:

```
docker compose ps
```

## Initialize Database

Run schema bootstrap and migrations from the pipeline container:

```
docker compose run --rm pipeline python scripts/bootstrap_schemas.py
docker compose run --rm pipeline python scripts/apply_migrations.py
```

## Data Ingestion

Run ingestion jobs:

CSV ingestion:

```
docker compose run --rm pipeline python -m pipeline.load_csvs
```

PDF parsing:

```
docker compose run --rm pipeline python -m pipeline.pdf_parse
```

## LLM Enrichment

Pull the model once (if not already present):

```
docker exec -it ollama ollama pull llama3.1:8b-instruct-q4_0
```

Run enrichment:

```
docker compose run --rm pipeline python -m pipeline.llm_enrich
```

## Run dbt commands (from repo root)
- Install packages: `docker compose run --rm dbt deps`
- Validate connection/config: `docker compose run --rm dbt debug`
- Build models:
  - `docker compose run --rm dbt run -s path:models/core/*`
  - `docker compose run --rm dbt run -s path:models/marts/*`
- Tests: `docker compose run --rm dbt test`

## Grafana

Load the prebuilt dashboards and point them to your Postgres data source:

1) Download the JSON files from [`dashboards_json`](dashboards_json/) to your machine.
2) Open Grafana at:

```
http://localhost:3000
```
3) Add the Postgres data source:
   - Data sources → Add new → PostgreSQL
   - Host: `postgres:5432`
   - Database / User / Password: values from `.env`
   - TLS: disable unless you set up SSL
   - Save & Test.
4) Import dashboards:
   - Dashboards → New → Import → Upload JSON
   - Upload each file you downloaded, select the Postgres data source, and click Import.

Notes:
- Default credentials are `admin/admin` unless changed in `.env`. Datasources and dashboards are provisioned from `./Docker/grafana`.

## Stop Services

```
docker compose down
```

To remove persisted volumes:

```
docker compose down -v
```

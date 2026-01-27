# Project Setup

This repo runs a data pipeline backed by Postgres, Grafana, and Ollama using Docker Compose.

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

Each staging loader expects specific columns. Extra columns are ignored, but any column listed below should be present.

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

macOS (Homebrew):

```
brew install --cask docker
```

Windows (winget):

```
winget install -e --id Docker.DockerDesktop
```

After installation, start Docker Desktop (macOS/Windows) or log out/in on Linux for the `docker` group change to take effect.

## Configuration

Create or update `.env` in the project root. Example:

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

Create dbt folder and files
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
## Run dbt commands
From repo root:

### Install dbt packages
```
docker compose run --rm dbt deps
```
### Validate connection + config
```
docker compose run --rm dbt debug
```
### Build models
```
docker compose run --rm dbt run -s path:models/core/*
docker compose run --rm dbt dbt run -s models.marts
```
### Run tests
```
docker compose run --rm dbt dbt test
```

Data marts modeling

## Grafana

Open Grafana in the browser:

```
http://localhost:3000
```

Default credentials are `admin/admin` unless changed in `.env`. Datasources and dashboards are provisioned from `./Docker/grafana`.

## Stop Services

```
docker compose down
```

To remove persisted volumes:

```
docker compose down -v
```

import os
import sys
import psycopg2 as psycopg
from dotenv import load_dotenv

from pipeline.staging_tools import build_postgres_db_url, create_run, finish_run
from pipeline.staging_transforms import (stage_student_data, stage_attivita_interne, stage_attivita_esterne, stage_attivita_fuorisede,
                                stage_pubblicazioni, stage_corsi, stage_dettaglio_corsi, stage_journal_details,
                                stage_ore_formazione, stage_stat_pubb, stage_collaborazioni_dettaglio,
                                stage_filtered_iu_stats, stage_mobilita_internazionale_con_studenti)


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
    

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            run_id = create_run(cur)
            conn.commit()

            stage_student_data(cur, run_id)
            stage_attivita_interne(cur, run_id)
            stage_attivita_esterne(cur, run_id)
            stage_attivita_fuorisede(cur, run_id)
            stage_pubblicazioni(cur, run_id)
            stage_corsi(cur, run_id)
            stage_dettaglio_corsi(cur, run_id)
            stage_journal_details(cur, run_id)
            stage_ore_formazione(cur, run_id)
            stage_stat_pubb(cur, run_id)
            stage_collaborazioni_dettaglio(cur, run_id)
            stage_filtered_iu_stats(cur, run_id)
            stage_mobilita_internazionale_con_studenti(cur, run_id)            

            finish_run(cur, run_id, "SUCCESS", None)


if __name__ == "__main__":
    raise SystemExit(main())
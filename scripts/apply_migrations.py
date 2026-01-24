import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

MIGRATIONS_DIR = Path("migrations")


def main() -> int:
    load_dotenv('.env')

    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    if not all([host, port, dbname, user, password]):
        print("ERROR: Set DATABASE_URL or POSTGRES_HOST/POSTGRES_PORT/POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD", file=sys.stderr)
        return 1
    DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    if not MIGRATIONS_DIR.exists():
        print("ERROR: migrations/ directory not found", file=sys.stderr)
        return 1

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        print("ERROR: no .sql migrations found in migrations/", file=sys.stderr)
        return 1


    with psycopg2.connect(DB_URL) as conn:
        conn.autocommit = True

        with conn.cursor() as cur:
            # simple migration log table (so you don't re-run files)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl.schema_migrations (
                  filename TEXT PRIMARY KEY,
                  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)

            for f in files:
                cur.execute("SELECT 1 FROM etl.schema_migrations WHERE filename = %s", (f.name,))
                if cur.fetchone():
                    print(f"↩️  Skipping already applied: {f.name}")
                    continue

                sql_text = f.read_text(encoding="utf-8")
                print(f"➡️  Applying: {f.name}")
                cur.execute(sql_text)
                cur.execute("INSERT INTO etl.schema_migrations(filename) VALUES (%s)", (f.name,))
                print(f"✅ Applied: {f.name}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
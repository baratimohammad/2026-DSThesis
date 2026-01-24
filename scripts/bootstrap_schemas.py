import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

SCHEMAS = ["etl", "staging", "core", "marts", "quarantine"]

def main():
    load_dotenv('.env')

    db_params = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    if not all(db_params.values()):
        print("Database connection parameters are not fully set in environment variables.")
        sys.exit(1)
    DB_URL = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    try:
        with psycopg2.connect(DB_URL) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for schema in SCHEMAS:
                    cur.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(schema)
                        )
                    )
                    print(f"Schema '{schema}' ensured.")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    raise SystemExit(main())
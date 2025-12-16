import argparse
import json
from pathlib import Path
import psycopg2

def main(ds: str, data_dir: Path):
    file_path = data_dir / f"users_{ds}.json"

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    users = json.loads(file_path.read_text(encoding="utf-8"))

    conn = psycopg2.connect(
        host="postgres_warehouse",
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_users (
            user_id INTEGER PRIMARY KEY,
            email TEXT,
            created_at TIMESTAMP,
            country TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    insert_sql = """
        INSERT INTO raw_users (user_id, email, created_at, country)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
    """

    rows = [
        (u["user_id"], u["email"], u["created_at"], u["country"])
        for u in users
    ]

    cur.executemany(insert_sql, rows)

    conn.commit()
    cur.close()
    conn.close()

    print(f"[load_users] ds={ds} inserted={len(rows)} rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    parser.add_argument("--data-dir", default="/opt/airflow/data/source/users")
    args = parser.parse_args()

    main(ds=args.ds, data_dir=Path(args.data_dir))

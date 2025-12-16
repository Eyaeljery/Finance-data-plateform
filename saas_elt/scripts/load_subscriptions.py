import argparse
import json
from pathlib import Path
import psycopg2

def main(ds: str, data_dir: Path):
    file_path = data_dir / f"subscriptions_{ds}.json"

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    subs = json.loads(file_path.read_text(encoding="utf-8"))

    conn = psycopg2.connect(
        host="postgres_warehouse",
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_subscriptions (
            subscription_id BIGINT PRIMARY KEY,
            user_id INTEGER REFERENCES raw_users(user_id),
            plan_id INTEGER REFERENCES raw_plans(plan_id),
            status TEXT,
            started_at TIMESTAMP,
            canceled_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.executemany("""
        INSERT INTO raw_subscriptions
        (subscription_id, user_id, plan_id, status, started_at, canceled_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (subscription_id) DO NOTHING
    """, [
        (
            s["subscription_id"],
            s["user_id"],
            s["plan_id"],
            s["status"],
            s["started_at"],
            s["canceled_at"]
        )
        for s in subs
    ])

    conn.commit()
    cur.close()
    conn.close()

    print(f"[load_subscriptions] ds={ds} inserted={len(subs)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    parser.add_argument("--data-dir", default="/opt/airflow/data/source/subscriptions")
    args = parser.parse_args()

    main(ds=args.ds, data_dir=Path(args.data_dir))

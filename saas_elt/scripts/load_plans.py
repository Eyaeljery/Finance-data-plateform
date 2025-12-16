import json
from pathlib import Path
import psycopg2

def main(data_dir: Path):
    file_path = data_dir / "plans.json"
    plans = json.loads(file_path.read_text(encoding="utf-8"))

    conn = psycopg2.connect(
        host="postgres_warehouse",
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_plans (
            plan_id INTEGER PRIMARY KEY,
            plan_name TEXT,
            price_monthly NUMERIC,
            currency TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.executemany("""
        INSERT INTO raw_plans (plan_id, plan_name, price_monthly, currency)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (plan_id) DO NOTHING
    """, [
        (p["plan_id"], p["plan_name"], p["price_monthly"], p["currency"])
        for p in plans
    ])

    conn.commit()
    cur.close()
    conn.close()

    print(f"[load_plans] inserted={len(plans)}")

if __name__ == "__main__":
    main(Path("/opt/airflow/data/source/plans"))

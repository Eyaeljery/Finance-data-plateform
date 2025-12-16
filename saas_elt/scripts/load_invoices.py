import argparse
import json
from pathlib import Path
import psycopg2

def main(ds: str, data_dir: Path):
    file_path = data_dir / f"invoices_{ds}.json"
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    invoices = json.loads(file_path.read_text(encoding="utf-8"))

    conn = psycopg2.connect(
        host="postgres_warehouse",
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_invoices (
            invoice_id BIGINT PRIMARY KEY,
            subscription_id BIGINT REFERENCES raw_subscriptions(subscription_id),
            user_id INTEGER REFERENCES raw_users(user_id),
            amount NUMERIC,
            currency TEXT,
            invoice_date TIMESTAMP,
            paid_at TIMESTAMP,
            status TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.executemany("""
        INSERT INTO raw_invoices
        (invoice_id, subscription_id, user_id, amount, currency, invoice_date, paid_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (invoice_id) DO NOTHING
    """, [
        (
            inv["invoice_id"],
            inv["subscription_id"],
            inv["user_id"],
            inv["amount"],
            inv["currency"],
            inv["invoice_date"],
            inv["paid_at"],
            inv["status"],
        )
        for inv in invoices
    ])

    conn.commit()
    cur.close()
    conn.close()

    print(f"[load_invoices] ds={ds} inserted={len(invoices)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    parser.add_argument("--data-dir", default="/opt/airflow/data/source/invoices")
    args = parser.parse_args()
    main(ds=args.ds, data_dir=Path(args.data_dir))

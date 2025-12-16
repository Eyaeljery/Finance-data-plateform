import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

def main(ds: str, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"invoices_{ds}.json"

    # Idempotence par date: si le fichier existe déjà, on ne régénère pas
    if out_path.exists():
        print(f"[extract_invoices] ds={ds} already exists -> skip")
        return

    run_date = datetime.fromisoformat(ds).date()

    # On ne facture que le 1er du mois (modèle SaaS simple)
    if run_date.day != 1:
        out_path.write_text("[]", encoding="utf-8")
        print(f"[extract_invoices] ds={ds} not billing day -> wrote empty file")
        return

    conn = psycopg2.connect(
        host="postgres_warehouse",
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
        port=5432
    )
    cur = conn.cursor()

    # Récupérer les subscriptions actives + leur plan
    cur.execute("""
        SELECT subscription_id, user_id, plan_id
        FROM raw_subscriptions
        WHERE status = 'active'
    """)
    subs = cur.fetchall()

    # Récupérer prix + devise des plans
    cur.execute("SELECT plan_id, price_monthly, currency FROM raw_plans")
    plan_map = {r[0]: (float(r[1]), r[2]) for r in cur.fetchall()}

    cur.close()
    conn.close()

    random.seed(ds)

    invoices = []
    base_id = int(ds.replace("-", "")) * 100000  # base déterministe par date
    i = 0

    for subscription_id, user_id, plan_id in subs:
        if plan_id not in plan_map:
            continue

        amount, currency = plan_map[plan_id]

        # Simuler quelques impayés (ex: 5%)
        status = "paid" if random.random() > 0.05 else "open"
        invoice_date = datetime.fromisoformat(ds).replace(tzinfo=timezone.utc).isoformat()
        paid_at = invoice_date if status == "paid" else None

        invoices.append({
            "invoice_id": base_id + i,
            "subscription_id": int(subscription_id),
            "user_id": int(user_id),
            "amount": amount,
            "currency": currency,
            "invoice_date": invoice_date,
            "paid_at": paid_at,
            "status": status,
        })
        i += 1

    out_path.write_text(json.dumps(invoices, indent=2), encoding="utf-8")
    print(f"[extract_invoices] ds={ds} invoices={len(invoices)} wrote={out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    parser.add_argument("--out-dir", default="/opt/airflow/data/source/invoices")
    args = parser.parse_args()
    main(ds=args.ds, out_dir=Path(args.out_dir))

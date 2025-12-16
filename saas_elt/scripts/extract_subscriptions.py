import argparse
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

STATUSES = ["active", "canceled"]

def main(ds: str, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"subscriptions_{ds}.json"

    # Idempotence par date
    if out_path.exists():
        print(f"[extract_subscriptions] ds={ds} already exists -> skip")
        return

    # Connexion warehouse pour lire users & plans
    conn = psycopg2.connect(
        host="postgres_warehouse",
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM raw_users")
    users = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT plan_id FROM raw_plans")
    plans = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    random.seed(ds)

    subscriptions = []
    sub_id = int(ds.replace("-", "")) * 1000  # deterministic base

    for user_id in users:
        # Tous les users n'ont pas une subscription
        if random.random() < 0.7:
            plan_id = random.choice(plans)
            status = random.choices(
                ["active", "canceled"], weights=[0.8, 0.2]
            )[0]

            started_at = datetime.fromisoformat(ds).replace(tzinfo=timezone.utc)
            canceled_at = None

            if status == "canceled":
                canceled_at = started_at.isoformat()

            subscriptions.append({
                "subscription_id": sub_id,
                "user_id": user_id,
                "plan_id": plan_id,
                "status": status,
                "started_at": started_at.isoformat(),
                "canceled_at": canceled_at
            })
            sub_id += 1

    out_path.write_text(json.dumps(subscriptions, indent=2), encoding="utf-8")
    print(f"[extract_subscriptions] ds={ds} subscriptions={len(subscriptions)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    parser.add_argument("--out-dir", default="/opt/airflow/data/source/subscriptions")
    args = parser.parse_args()

    main(ds=args.ds, out_dir=Path(args.out_dir))

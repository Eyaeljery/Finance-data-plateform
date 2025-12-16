import json
from pathlib import Path

PLANS = [
    {"plan_id": 1, "plan_name": "Basic", "price_monthly": 10, "currency": "EUR"},
    {"plan_id": 2, "plan_name": "Pro", "price_monthly": 25, "currency": "EUR"},
    {"plan_id": 3, "plan_name": "Enterprise", "price_monthly": 100, "currency": "EUR"},
]

def main(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "plans.json"

    # Idempotent par design (réécrire le même fichier est OK)
    out_path.write_text(json.dumps(PLANS, indent=2), encoding="utf-8")
    print(f"[extract_plans] wrote {out_path}")

if __name__ == "__main__":
    main(Path("/opt/airflow/data/source/plans"))

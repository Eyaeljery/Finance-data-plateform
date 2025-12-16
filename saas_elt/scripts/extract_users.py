import argparse
import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path

COUNTRIES = ["FR", "US", "DE", "ES", "GB", "NL", "BE", "IT", "CA"]

def load_state(state_path: Path) -> dict:
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {"last_user_id": 0}

def save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

def main(ds: str, out_dir: Path, state_path: Path, n: int | None) -> None:
    # Seed deterministe par date => reproductible
    random.seed(ds)

    state = load_state(state_path)
    last_id = int(state.get("last_user_id", 0))

    # Nombre de nouveaux users du jour
    if n is None:
        n = random.randint(5, 20)

    users = []
    created_base = datetime.fromisoformat(ds).replace(tzinfo=timezone.utc)

    for i in range(1, n + 1):
        user_id = last_id + i
        country = random.choice(COUNTRIES)

        users.append({
            "user_id": user_id,
            "email": f"user{user_id}@example.com",
            "created_at": (created_base).isoformat(),
            "country": country
        })

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"users_{ds}.json"
    out_path.write_text(json.dumps(users, indent=2), encoding="utf-8")

    # update state
    state["last_user_id"] = last_id + n
    save_state(state_path, state)

    print(f"[extract_users] ds={ds} wrote={out_path} new_users={n} last_user_id={state['last_user_id']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=False, help="YYYY-MM-DD (date du run)")
    parser.add_argument("--out-dir", default="/opt/airflow/data/source/users")
    parser.add_argument("--state-path", default="/opt/airflow/data/state/users_state.json")
    parser.add_argument("--n", type=int, default=None, help="Nombre de users à générer (sinon aléatoire)")
    args = parser.parse_args()

    ds = args.ds or datetime.now(timezone.utc).date().isoformat()
    main(
        ds=ds,
        out_dir=Path(args.out_dir),
        state_path=Path(args.state_path),
        n=args.n
    )

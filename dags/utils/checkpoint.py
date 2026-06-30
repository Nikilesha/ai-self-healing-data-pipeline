import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR")


def mark_done(step_name, data=None):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    path = os.path.join(CHECKPOINT_DIR, f"{step_name}.json")

    with open(path, "w") as f:
        json.dump({
            "status": "done",
            "step": step_name,
            "completed at":datetime.utcnow().isoformat(),
            "meta": data or {}
        }, f,indent=4)


def load_checkpoint(step_name):
    path = os.path.join(CHECKPOINT_DIR, f"{step_name}.json")

    if not os.path.exists(path):
        return None

    with open(path) as f:
        return json.load(f)

    try:
        with open(path, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.warning(f"Checkpoint '{step_name}' is corrupted. Removing it.")
        os.remove(path)
        return None


def clear_all():
    if os.path.exists(CHECKPOINT_DIR):
        for file in os.listdir(CHECKPOINT_DIR):
            os.remove(os.path.join(CHECKPOINT_DIR, file))
import os
import json

CHECKPOINT_DIR = "/opt/airflow/checkpoints"


def mark_done(step_name, data=None):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    path = os.path.join(CHECKPOINT_DIR, f"{step_name}.json")

    with open(path, "w") as f:
        json.dump({
            "status": "done",
            "step": step_name,
            "meta": data or {}
        }, f)


def is_done(step_name):
    path = os.path.join(CHECKPOINT_DIR, f"{step_name}.json")
    return os.path.exists(path)


def clear_all():
    if os.path.exists(CHECKPOINT_DIR):
        for file in os.listdir(CHECKPOINT_DIR):
            os.remove(os.path.join(CHECKPOINT_DIR, file))
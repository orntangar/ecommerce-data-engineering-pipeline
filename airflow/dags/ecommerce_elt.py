"""Demo DAG for Airflow 2.x; deploy with a compatible Airflow environment."""
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"


@dag(
    dag_id="ecommerce_elt",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "dbt", "elt"],
    params={
        "batch_id": Param("demo_001", type="string", minLength=1),
        "file_name": Param("orders_batch_001.jsonl", type="string", minLength=1),
    },
)
def ecommerce_elt():
    @task
    def ingest(**context):
        batch_id = context["params"]["batch_id"]
        file_name = context["params"]["file_name"]
        if Path(file_name).name != file_name or not file_name.endswith(".jsonl"):
            raise ValueError("file_name must be a JSONL filename in data/")
        file_path = DATA / file_name
        if not file_path.is_file():
            raise FileNotFoundError(file_path)
        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "ingest.py"),
             "--file", str(file_path), "--batch-id", batch_id],
            check=True,
        )
        return batch_id

    @task
    def run_dbt(batch_id: str):
        subprocess.run(
            ["dbt", "build", "--project-dir", str(ROOT / "dbt"),
             "--profiles-dir", str(ROOT / "dbt"),
             "--vars", json.dumps({"batch_id": batch_id})],
            check=True,
        )
        return batch_id

    @task
    def acknowledge(batch_id: str):
        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "mark_processed.py"),
             "--batch-id", batch_id],
            check=True,
        )

    acknowledge(run_dbt(ingest()))


ecommerce_elt()

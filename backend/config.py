from pathlib import Path
import os


PROJECT_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_DIR / "backend"
FRONTEND_DIR = PROJECT_DIR / "docs"

GENERATED_DIR = BACKEND_DIR / "data" / "generated"
RAW_DIR = GENERATED_DIR / "raw"
BRONZE_DIR = GENERATED_DIR / "bronze" / "cluster_events"
SILVER_DIR = GENERATED_DIR / "silver" / "job_features"
GOLD_CUSTOMER_DIR = GENERATED_DIR / "gold" / "customer_health"
GOLD_INCIDENT_DIR = GENERATED_DIR / "gold" / "incident_queue"
SYSTEM_DIR = GENERATED_DIR / "system"
RAW_DATASET_PATH = RAW_DIR / "customer_cluster_events.csv"
PIPELINE_STATE_PATH = SYSTEM_DIR / "pipeline_state.json"
DB_PATH = GENERATED_DIR / "control_plane.sqlite3"

HOST = os.getenv("LISTEN_HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", os.getenv("BACKEND_PORT", "8000")))

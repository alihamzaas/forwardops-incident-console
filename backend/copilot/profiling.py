from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from backend.copilot.models import DatasetProfile


UPLOAD_DIR = Path(__file__).resolve().parents[1] / "data" / "copilot_uploads"


def ensure_upload_dir() -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    return UPLOAD_DIR


def save_upload_bytes(filename: str, content: bytes) -> tuple[str, Path]:
    ensure_upload_dir()
    session_id = f"session-{uuid.uuid4().hex[:10]}"
    suffix = Path(filename).suffix.lower() or ".csv"
    path = UPLOAD_DIR / f"{session_id}{suffix}"
    path.write_bytes(content)
    return session_id, path


def read_dataset(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def profile_dataframe(session_id: str, filename: str, dataframe: pd.DataFrame, parquet_path: str | None = None) -> DatasetProfile:
    numeric_columns = dataframe.select_dtypes(include="number").columns.tolist()
    categorical_columns = dataframe.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
    return DatasetProfile(
        session_id=session_id,
        filename=filename,
        rows=int(dataframe.shape[0]),
        columns=int(dataframe.shape[1]),
        column_names=dataframe.columns.tolist(),
        dtypes={column: str(dtype) for column, dtype in dataframe.dtypes.items()},
        numeric_columns=numeric_columns,
        categorical_columns=categorical_columns,
        missing_values={column: int(value) for column, value in dataframe.isna().sum().to_dict().items()},
        sample_rows=json_safe(dataframe.head(5).to_dict(orient="records")),
        parquet_path=parquet_path,
    )


def profile_to_prompt(profile: dict[str, Any]) -> str:
    numeric = ", ".join(profile.get("numeric_columns", [])) or "none"
    categorical = ", ".join(profile.get("categorical_columns", [])) or "none"
    columns = ", ".join(profile.get("column_names", []))
    return (
        f"Dataset {profile.get('filename')} has {profile.get('rows')} rows and {profile.get('columns')} columns.\n"
        f"Columns: {columns}\n"
        f"Numeric columns: {numeric}\n"
        f"Categorical columns: {categorical}\n"
        f"Missing values: {profile.get('missing_values', {})}\n"
        f"Sample rows: {profile.get('sample_rows', [])}"
    )


def json_safe(value: Any) -> Any:
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value

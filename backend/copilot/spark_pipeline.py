from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


COPILOT_LAKE_DIR = Path(__file__).resolve().parents[1] / "data" / "copilot_lake"


def materialize_with_pyspark(input_path: Path, session_id: str) -> dict[str, Any]:
    """Materialize uploaded data with PySpark when available.

    The project keeps a pandas fallback so the upload flow remains functional on
    small local machines, while the real PySpark path is used in Docker or any
    environment with Spark installed.
    """

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder.appName("agentic-data-copilot")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        bronze_path = COPILOT_LAKE_DIR / "bronze" / session_id
        silver_path = COPILOT_LAKE_DIR / "silver" / session_id
        suffix = input_path.suffix.lower()
        if suffix == ".csv":
            frame = spark.read.option("header", True).option("inferSchema", True).csv(str(input_path))
        else:
            pandas_frame = pd.read_excel(input_path) if suffix in {".xlsx", ".xls"} else pd.read_csv(input_path)
            frame = spark.createDataFrame(pandas_frame)
        frame.withColumn("_ingested_at", F.current_timestamp()).write.mode("overwrite").parquet(str(bronze_path))

        row_count = frame.count()
        feature_rows = []
        for column, dtype in frame.dtypes:
            missing = frame.where(F.col(column).isNull()).count()
            feature_rows.append((column, dtype, missing, row_count))
        features = spark.createDataFrame(feature_rows, ["column_name", "spark_dtype", "missing_count", "row_count"])
        features.write.mode("overwrite").parquet(str(silver_path))
        spark.stop()
        return {
            "engine": "pyspark",
            "bronze_path": str(bronze_path),
            "silver_path": str(silver_path),
            "row_count": row_count,
            "feature_count": len(feature_rows),
        }
    except Exception as exc:
        return materialize_with_pandas(input_path, session_id, fallback_reason=str(exc))


def materialize_with_pandas(input_path: Path, session_id: str, fallback_reason: str | None = None) -> dict[str, Any]:
    COPILOT_LAKE_DIR.mkdir(parents=True, exist_ok=True)
    suffix = input_path.suffix.lower()
    dataframe = pd.read_excel(input_path) if suffix in {".xlsx", ".xls"} else pd.read_csv(input_path)
    bronze_path = COPILOT_LAKE_DIR / "bronze" / session_id
    silver_path = COPILOT_LAKE_DIR / "silver" / session_id
    bronze_path.mkdir(parents=True, exist_ok=True)
    silver_path.mkdir(parents=True, exist_ok=True)

    dataframe.to_parquet(bronze_path / "part-000.parquet", index=False)
    features = pd.DataFrame(
        [
            {
                "column_name": column,
                "pandas_dtype": str(dtype),
                "missing_count": int(dataframe[column].isna().sum()),
                "row_count": int(dataframe.shape[0]),
            }
            for column, dtype in dataframe.dtypes.items()
        ]
    )
    features.to_parquet(silver_path / "part-000.parquet", index=False)
    return {
        "engine": "pandas-fallback",
        "fallback_reason": fallback_reason,
        "bronze_path": str(bronze_path),
        "silver_path": str(silver_path),
        "row_count": int(dataframe.shape[0]),
        "feature_count": int(features.shape[0]),
    }

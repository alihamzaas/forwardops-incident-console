import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.copilot.graph import run_agent_graph
from backend.copilot.sandbox import run_guarded_code
from backend.copilot.spark_pipeline import materialize_with_pandas


class CopilotSandboxTests(unittest.TestCase):
    def test_guarded_code_executes_dataframe_analysis(self) -> None:
        frame = pd.DataFrame(
            [
                {"account": "Nova", "duration_minutes": 34.2},
                {"account": "Nova", "duration_minutes": 41.7},
                {"account": "Atlas", "duration_minutes": 29.1},
            ]
        )
        result = run_guarded_code(
            """
summary = df.groupby("account")["duration_minutes"].mean().sort_values(ascending=False)
print(summary.to_string())
fig = px.bar(summary.reset_index(), x="account", y="duration_minutes")
""",
            frame,
        )

        self.assertTrue(result.ok, result.error)
        self.assertIn("Nova", result.stdout)
        self.assertIsNotNone(result.chart_json)
        self.assertGreaterEqual(len(result.traces), 3)

    def test_guarded_code_blocks_unsafe_imports(self) -> None:
        result = run_guarded_code("import os\nprint(os.getcwd())", pd.DataFrame([{"x": 1}]))

        self.assertFalse(result.ok)
        self.assertIn("Imports are disabled", result.error)


class CopilotGraphTests(unittest.TestCase):
    def test_graph_streams_all_agent_stages_with_fallbacks(self) -> None:
        profile = {
            "filename": "sample.csv",
            "rows": 3,
            "columns": 2,
            "column_names": ["account", "duration_minutes"],
            "numeric_columns": ["duration_minutes"],
            "categorical_columns": ["account"],
            "missing_values": {"account": 0, "duration_minutes": 0},
            "sample_rows": [{"account": "Nova", "duration_minutes": 34.2}],
        }
        events = list(
            run_agent_graph(
                {
                    "session_id": "test-session",
                    "query": "Compare average duration by account",
                    "profile": profile,
                    "dataframe_records": [
                        {"account": "Nova", "duration_minutes": 34.2},
                        {"account": "Nova", "duration_minutes": 41.7},
                        {"account": "Atlas", "duration_minutes": 29.1},
                    ],
                    "events": [],
                }
            )
        )

        stages = [event["stage"] for event in events]
        self.assertIn("planner", stages)
        self.assertIn("analyst", stages)
        self.assertIn("coder", stages)
        self.assertIn("executor", stages)
        self.assertIn("summarizer", stages)
        self.assertTrue(events[-1]["data"]["summary"])


class CopilotLakeTests(unittest.TestCase):
    def test_pandas_materialization_writes_bronze_and_silver_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "metrics.csv"
            pd.DataFrame(
                [
                    {"account": "Nova", "duration_minutes": 34.2},
                    {"account": "Atlas", "duration_minutes": 29.1},
                ]
            ).to_csv(csv_path, index=False)

            result = materialize_with_pandas(csv_path, "unit-test-session", fallback_reason="unit-test")

        self.assertEqual(result["engine"], "pandas-fallback")
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["feature_count"], 2)


if __name__ == "__main__":
    unittest.main()

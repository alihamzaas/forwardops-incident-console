import unittest

from backend.config import GOLD_CUSTOMER_DIR, GOLD_INCIDENT_DIR
from backend.data.pipeline import read_parquet_dataset, run_feature_pipeline


class FeaturePipelineTests(unittest.TestCase):
    def test_pipeline_materializes_gold_tables(self) -> None:
        artifacts = run_feature_pipeline()
        customer_health = read_parquet_dataset(GOLD_CUSTOMER_DIR)
        incident_queue = read_parquet_dataset(GOLD_INCIDENT_DIR)

        self.assertEqual(artifacts.customer_count, 4)
        self.assertGreaterEqual(artifacts.incident_count, 1)
        self.assertIn("customer_health_score", customer_health.columns)
        self.assertIn("incident_id", incident_queue.columns)


if __name__ == "__main__":
    unittest.main()

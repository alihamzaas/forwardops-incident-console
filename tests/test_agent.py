import unittest

from backend.agents.graph import run_triage
from backend.repository import ControlPlaneRepository


class AgentTriageTests(unittest.TestCase):
    def test_agent_generates_customer_ready_plan(self) -> None:
        repository = ControlPlaneRepository()
        repository.bootstrap()

        incident_id = repository.dashboard_payload()["incidents"][0]["incident_id"]
        report = run_triage("triage-test", incident_id, repository, publish=None, pause_seconds=0)

        self.assertGreaterEqual(len(report.next_actions), 1)
        self.assertGreaterEqual(len(report.deployment_plan), 1)
        self.assertTrue(report.customer_update)
        self.assertIn(report.customer_name, report.customer_update)


if __name__ == "__main__":
    unittest.main()

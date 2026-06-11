from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLISH_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "publish-ghcr-image.yml"


class ReleaseWorkflowTests(unittest.TestCase):
    def test_publish_workflow_passes_sms_runtime_secrets_to_materializer(self) -> None:
        workflow = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

        required_mappings = {
            "EASYREGISTER_ENV_SMS_SERVICE_BASE_URL": "${{ secrets.EASYREGISTER_ENV_SMS_SERVICE_BASE_URL }}",
            "EASYREGISTER_ENV_SMS_SERVICE_API_KEY": "${{ secrets.EASYREGISTER_ENV_SMS_SERVICE_API_KEY }}",
            "EASYREGISTER_ENV_SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS": "${{ secrets.EASYREGISTER_ENV_SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS }}",
            "EASYREGISTER_ENV_SMS_SERVICE_SELECTION_PLAN_ATTEMPTS": "${{ secrets.EASYREGISTER_ENV_SMS_SERVICE_SELECTION_PLAN_ATTEMPTS }}",
            "EASYREGISTER_ENV_REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS": "${{ secrets.EASYREGISTER_ENV_REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS }}",
            "EASYREGISTER_ENV_REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS": "${{ secrets.EASYREGISTER_ENV_REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS }}",
            "EASYREGISTER_ENV_REGISTER_SMS_BUSINESS_KEY": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_BUSINESS_KEY }}",
            "EASYREGISTER_ENV_REGISTER_SMS_PROVIDER_BLACKLIST": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_PROVIDER_BLACKLIST }}",
            "EASYREGISTER_ENV_REGISTER_SMS_ALLOW_PAID": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_ALLOW_PAID }}",
            "EASYREGISTER_ENV_REGISTER_SMS_ALLOW_REUSE": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_ALLOW_REUSE }}",
            "EASYREGISTER_ENV_REGISTER_SMS_MAX_BINDINGS_PER_PHONE": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_MAX_BINDINGS_PER_PHONE }}",
            "EASYREGISTER_ENV_REGISTER_SMS_COUNTRY_CODES": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_COUNTRY_CODES }}",
            "EASYREGISTER_ENV_REGISTER_SMS_SELECTION_MODE": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_SELECTION_MODE }}",
            "EASYREGISTER_ENV_REGISTER_SMS_BUSINESS_POLICIES_JSON": "${{ secrets.EASYREGISTER_ENV_REGISTER_SMS_BUSINESS_POLICIES_JSON }}",
        }

        for env_name, secret_expression in required_mappings.items():
            with self.subTest(env_name=env_name):
                self.assertIn(f"{env_name}: {secret_expression}", workflow)


if __name__ == "__main__":
    unittest.main()

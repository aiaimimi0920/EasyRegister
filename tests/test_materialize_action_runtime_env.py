from __future__ import annotations

import importlib.util
import base64
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MATERIALIZER_PATH = REPO_ROOT / "scripts" / "materialize-action-runtime-env.py"


def _load_materializer_module():
    spec = importlib.util.spec_from_file_location("materialize_action_runtime_env", MATERIALIZER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {MATERIALIZER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MaterializeActionRuntimeEnvTests(unittest.TestCase):
    def test_empty_secret_env_does_not_blank_base_defaults(self) -> None:
        materializer = _load_materializer_module()

        with tempfile.TemporaryDirectory(prefix="easyregister-runtime-env-") as temp:
            base_env = Path(temp) / "base.env"
            base_env.write_text(
                "\n".join(
                    [
                        "SMS_SERVICE_BASE_URL=http://easy-sms:8080",
                        "SMS_SERVICE_API_KEY=",
                        "REGISTER_SMS_BUSINESS_KEY=openai",
                        "REGISTER_SMS_SELECTION_MODE=balanced",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            base_lines, base_values = materializer._parse_env_file(base_env)
            overrides = materializer._load_secret_env(
                {
                    "EASYREGISTER_ENV_SMS_SERVICE_API_KEY": "sms-test-key",
                    "EASYREGISTER_ENV_REGISTER_SMS_BUSINESS_KEY": "",
                    "EASYREGISTER_ENV_REGISTER_SMS_SELECTION_MODE": "",
                }
            )
            rendered = materializer._render_lines(base_lines, {**base_values, **overrides})

        self.assertIn("SMS_SERVICE_API_KEY=sms-test-key", rendered)
        self.assertIn("REGISTER_SMS_BUSINESS_KEY=openai", rendered)
        self.assertIn("REGISTER_SMS_SELECTION_MODE=balanced", rendered)

    def test_b64_runtime_env_allows_non_empty_secret_env_override(self) -> None:
        materializer = _load_materializer_module()

        encoded_runtime_env = base64.b64encode(
            "\n".join(
                [
                    "SMS_SERVICE_BASE_URL=http://easy-sms:8080",
                    "SMS_SERVICE_API_KEY=",
                    "REGISTER_SMS_BUSINESS_KEY=openai",
                ]
            ).encode("utf-8")
        ).decode("ascii")

        with tempfile.TemporaryDirectory(prefix="easyregister-runtime-env-") as temp:
            base_env = Path(temp) / "base.env"
            output_env = Path(temp) / "output.env"
            base_env.write_text("SMS_SERVICE_API_KEY=\n", encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "EASYREGISTER_RUNTIME_ENV_B64": encoded_runtime_env,
                    "EASYREGISTER_ENV_SMS_SERVICE_API_KEY": "sms-test-key",
                    "EASYREGISTER_ENV_REGISTER_SMS_BUSINESS_KEY": "",
                },
                clear=True,
            ), patch.object(
                sys,
                "argv",
                [
                    "materialize-action-runtime-env.py",
                    "--base-env",
                    str(base_env),
                    "--output",
                    str(output_env),
                ],
            ):
                self.assertEqual(0, materializer.main())

            rendered = output_env.read_text(encoding="utf-8")

        self.assertIn("SMS_SERVICE_API_KEY=sms-test-key", rendered)
        self.assertIn("REGISTER_SMS_BUSINESS_KEY=openai", rendered)

    def test_b64_runtime_env_fills_sms_selection_plan_defaults(self) -> None:
        materializer = _load_materializer_module()

        encoded_runtime_env = base64.b64encode(
            "\n".join(
                [
                    "SMS_SERVICE_BASE_URL=http://easy-sms:8080",
                    "SMS_SERVICE_API_KEY=sms-test-key",
                    "REGISTER_SMS_BUSINESS_KEY=openai",
                ]
            ).encode("utf-8")
        ).decode("ascii")

        with tempfile.TemporaryDirectory(prefix="easyregister-runtime-env-") as temp:
            base_env = Path(temp) / "base.env"
            output_env = Path(temp) / "output.env"
            base_env.write_text(
                "\n".join(
                    [
                        "SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS=90",
                        "SMS_SERVICE_SELECTION_PLAN_ATTEMPTS=1",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "EASYREGISTER_RUNTIME_ENV_B64": encoded_runtime_env,
                },
                clear=True,
            ), patch.object(
                sys,
                "argv",
                [
                    "materialize-action-runtime-env.py",
                    "--base-env",
                    str(base_env),
                    "--output",
                    str(output_env),
                ],
            ):
                self.assertEqual(0, materializer.main())

            rendered = output_env.read_text(encoding="utf-8")

        self.assertIn("SMS_SERVICE_SELECTION_PLAN_TIMEOUT_SECONDS=90", rendered)
        self.assertIn("SMS_SERVICE_SELECTION_PLAN_ATTEMPTS=1", rendered)


if __name__ == "__main__":
    unittest.main()

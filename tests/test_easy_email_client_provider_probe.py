from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


PYTHON_SHARED_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "python_shared" / "src"
if str(PYTHON_SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(PYTHON_SHARED_ROOT))

from shared_mailbox import easy_email_client  # noqa: E402


class EasyEmailClientProviderProbeTests(unittest.TestCase):
    def test_nas_cloudflare_reuses_configured_instance_for_recovery_credentials(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True), \
            mock.patch.object(easy_email_client, "_wait_mail_service_ready"):
            payload, provider, _ = easy_email_client._build_mailbox_request_payload(
                provider="cloudflare_temp_email", default_host_id="nas-dst-test",
            )
        self.assertEqual("cloudflare_temp_email", provider)
        self.assertEqual("reuse-only", payload["provisionMode"])
        self.assertEqual("shared-instance", payload["bindingMode"])

    def test_probe_mailbox_provider_recovers_cooling_instance(self) -> None:
        with mock.patch.object(
            easy_email_client,
            "_get_json",
            side_effect=(
                {
                    "instances": [
                        {
                            "id": "mail2925_shared_default",
                            "providerTypeKey": "mail2925",
                            "status": "cooling",
                        }
                    ]
                },
                {"probe": {"ok": True, "status": "active", "providerTypeKey": "mail2925"}},
            ),
        ) as get_json:
            result = easy_email_client.probe_mailbox_provider(provider_type_key="mail2925")

        self.assertTrue(result.get("ok"))
        self.assertEqual(
            [
                mock.call("/mail/query/provider-instances?providerTypeKey=mail2925"),
                mock.call("/mail/providers/mail2925_shared_default/probe"),
            ],
            get_json.call_args_list,
        )

    def test_probe_mailbox_provider_skips_offline_instance(self) -> None:
        with mock.patch.object(
            easy_email_client,
            "_get_json",
            return_value={
                "instances": [
                    {
                        "id": "mail2925_shared_default",
                        "providerTypeKey": "mail2925",
                        "status": "offline",
                    }
                ]
            },
        ) as get_json:
            result = easy_email_client.probe_mailbox_provider(provider_type_key="mail2925")

        self.assertEqual({}, result)
        get_json.assert_called_once_with(
            "/mail/query/provider-instances?providerTypeKey=mail2925"
        )

    def test_wait_mail_service_ready_requires_catalog_object(self) -> None:
        with mock.patch.object(easy_email_client, "_mail_service_ready_timeout_seconds", return_value=1), mock.patch.object(
            easy_email_client,
            "_mail_service_ready_probe_interval_seconds",
            return_value=1,
        ), mock.patch.object(
            easy_email_client,
            "_mail_service_request",
            return_value={"ok": True},
        ), mock.patch.object(
            easy_email_client.time,
            "sleep",
            return_value=None,
        ):
            with self.assertRaisesRegex(RuntimeError, "catalog object missing"):
                easy_email_client._wait_mail_service_ready()

    def test_mail_service_base_url_accepts_nas_sdk_env(self) -> None:
        with mock.patch.dict(
            __import__("os").environ,
            {"EASY_EMAIL_BASE_URL": "http://easy-email-service:8080"},
            clear=True,
        ):
            self.assertEqual(
                "http://easy-email-service:8080",
                easy_email_client._mail_service_base_url(),
            )

    def test_mail_service_headers_accept_nas_sdk_api_key(self) -> None:
        with mock.patch.dict(
            __import__("os").environ,
            {"EASY_EMAIL_API_KEY": "nas-token"},
            clear=True,
        ):
            headers = easy_email_client._mail_service_headers()
        self.assertEqual("Bearer nas-token", headers.get("Authorization"))


if __name__ == "__main__":
    unittest.main()

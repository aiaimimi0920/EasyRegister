from __future__ import annotations

import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from shared_mailbox import easy_email_client as client


class StoredOpenAiCodeQueryTests(unittest.TestCase):
    def test_targets_one_session_without_sync_or_whole_registry(self):
        session_id = "session /with?reserved&characters"
        with patch.object(client, "_get_json", return_value={"messages": []}) as get:
            self.assertEqual(client._snapshot_session_openai_code(session_id=session_id, min_mail_id=0), ("", 0))
        path = get.call_args.args[0]
        self.assertEqual(urlsplit(path).path, "/mail/query/observed-messages")
        self.assertEqual(parse_qs(urlsplit(path).query), {
            "sessionId": [session_id], "sync": ["false"], "limit": ["20"], "newestFirst": ["true"],
        })

    def test_other_session_and_older_messages_are_not_accepted(self):
        messages = [{"sessionId": "other", "observedAt": "100", "extractedCode": "123456"},
                    {"sessionId": "mine", "observedAt": "10", "extractedCode": "234567"}]
        with patch.object(client, "_get_json", return_value={"messages": messages}), \
                patch.object(client, "_parse_mail_timestamp", side_effect=lambda value: int(value or 0)), \
                patch.object(client, "_extract_openai_code_from_message", side_effect=lambda item: item["extractedCode"]):
            self.assertEqual(client._snapshot_session_openai_code(session_id="mine", min_mail_id=20), ("", 0))

    def test_existing_equal_floor_option_is_preserved(self):
        message = {"sessionId": "mine", "observedAt": "20", "extractedCode": "123456"}
        with patch.object(client, "_get_json", return_value={"messages": [message]}), \
                patch.object(client, "_parse_mail_timestamp", side_effect=lambda value: int(value or 0)), \
                patch.object(client, "_extract_openai_code_from_message", return_value="123456"):
            self.assertEqual(client._snapshot_session_openai_code(session_id="mine", min_mail_id=20), ("", 0))
            self.assertEqual(client._snapshot_session_openai_code(
                session_id="mine", min_mail_id=20, allow_min_mail_id_equal=True), ("123456", 20))

    def test_selects_newest_valid_code_for_exact_session(self):
        messages = [{"sessionId": "mine", "observedAt": "21", "extractedCode": "123456"},
                    {"sessionId": "mine", "observedAt": "23", "extractedCode": "234567"},
                    {"sessionId": "other", "observedAt": "99", "extractedCode": "345678"}]
        with patch.object(client, "_get_json", return_value={"messages": messages}), \
                patch.object(client, "_parse_mail_timestamp", side_effect=lambda value: int(value or 0)), \
                patch.object(client, "_extract_openai_code_from_message", side_effect=lambda item: item["extractedCode"]):
            self.assertEqual(client._snapshot_session_openai_code(session_id="mine", min_mail_id=20), ("234567", 23))

    def test_query_failure_propagates_without_bulk_snapshot_fallback(self):
        with patch.object(client, "_get_json", side_effect=TimeoutError("stored_query_timeout")) as get:
            with self.assertRaises(TimeoutError):
                client._snapshot_session_openai_code(session_id="mine", min_mail_id=0)
        self.assertEqual(get.call_count, 1)

    def test_missing_messages_remains_no_code(self):
        with patch.object(client, "_get_json", return_value={}):
            self.assertEqual(client._snapshot_session_openai_code(session_id="mine", min_mail_id=0), ("", 0))


if __name__ == "__main__":
    unittest.main()

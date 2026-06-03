# Add Phone SMS Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `EasyRegister` 的 `openai-main / openai-continue` 流程中，当 `obtain_codex_oauth` 命中 `add_phone` 时，通过 `EasySms` 获取手机号和短信验证码，完成手机号填写与验证码填写，并在失败时继续沿用现有 `failed-once / failed-twice` 语义。

**Architecture:** `EasyRegister` 负责业务配置、SMS session 生命周期和失败语义；`EasyProtocol` 只扩展最小的 `phone_wall` 结构化返回与“提交手机号 / 提交验证码”的继续能力；`EasySms` 沿用现有 session-first API，不改核心 provider 逻辑。正常不需要手机号的样本仍然走现有 `obtain_codex_oauth -> validate_free_personal_oauth` 快路径。

**Tech Stack:** Python 3, unittest, EasyRegister DST runtime, EasyProtocol Python provider runtime, EasySms native HTTP API, Docker compose smoke validation

---

## File Structure Map

### EasyRegister

- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\python_shared\src\shared_sms\__init__.py`
- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\python_shared\src\shared_sms\easy_sms_client.py`
- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\runtime_sms.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\config_runtime_sections.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\config.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\local_config.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\easyprotocol_runtime.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\README.md`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\deploy\easyregister.runtime.env.example`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.yaml`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.test.yaml`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_typed_config.py`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_adapter_runtimes.py`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_dst_flow_integration.py`

### EasyProtocol

- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\protocol_phone_verification.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\easyprotocol_flow.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\protocol_small_success.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\others\storage.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\protocol_runtime\protocol_register.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\tests\test_easyprotocol_flow.py`

### EasySms

- No planned core code change in v1
- Validation only against existing API:
  - `POST /sms/sessions/open`
  - `GET /sms/sessions/{sessionId}/code`
  - `POST /sms/sessions/report-outcome`

---

### Task 1: Add SMS config parsing and EasySms shared client in EasyRegister

**Files:**
- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\python_shared\src\shared_sms\__init__.py`
- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\python_shared\src\shared_sms\easy_sms_client.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\config_runtime_sections.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\config.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\local_config.py`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_typed_config.py`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_adapter_runtimes.py`

- [ ] **Step 1: Write the failing config and client tests**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_typed_config.py
def test_sms_runtime_config_parses_default_and_openai_business_policies(self) -> None:
    with mock.patch.dict(
        os.environ,
        {
            "REGISTER_SMS_BUSINESS_KEY": "openai",
            "REGISTER_SMS_PROVIDER_BLACKLIST": "hero_sms",
            "REGISTER_SMS_ALLOW_PAID": "false",
            "REGISTER_SMS_BUSINESS_POLICIES_JSON": (
                '{"default":{"enabled":false,"providerBlacklist":["hero_sms"],"allowPaid":false},'
                '"openai":{"enabled":true,"providerBlacklist":["hero_sms","paid_backup"],'
                '"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,'
                '"countryCodes":["US"],"selectionMode":"available-first"}}'
            ),
        },
        clear=False,
    ):
        config = typed_config.SmsRuntimeConfig.from_env(
            default_state_path=Path("C:/tmp/register-sms-state.json"),
        )

    policy = config.resolve_business_policy("openai")
    self.assertTrue(policy.enabled)
    self.assertEqual(("hero_sms", "paid_backup"), policy.explicit_blacklist_providers)
    self.assertFalse(policy.allow_paid)
    self.assertFalse(policy.allow_reuse)
    self.assertEqual(1, policy.max_bindings_per_phone)
    self.assertEqual(("us",), policy.country_codes)
    self.assertEqual("available-first", policy.selection_mode)


# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_adapter_runtimes.py
def test_easy_sms_client_open_session_builds_free_first_request(self) -> None:
    with mock.patch.dict(
        os.environ,
        {
            "SMS_SERVICE_BASE_URL": "http://easy-sms:8080",
            "SMS_SERVICE_API_KEY": "sms-key",
        },
        clear=False,
    ), mock.patch.object(
        easy_sms_client,
        "_post_json",
        return_value={
            "result": {
                "session": {
                    "id": "sms_123",
                    "phoneNumberE164": "+15551234567",
                    "providerKey": "sms24",
                }
            }
        },
    ) as post_json:
        session = easy_sms_client.open_sms_session(
            business_key="openai",
            provider_blacklist=("hero_sms",),
            allow_paid=False,
            allow_reuse=False,
            max_bindings_per_phone=1,
            country_codes=("us",),
            selection_mode="available-first",
        )

    payload = post_json.call_args.args[1]
    self.assertEqual("openai", payload["businessKey"])
    self.assertEqual(["hero_sms"], payload["providerBlacklist"])
    self.assertEqual("free", payload["costTier"])
    self.assertEqual(False, payload["allowReuse"])
    self.assertEqual(1, payload["maxBindingsPerPhone"])
    self.assertEqual(["us"], payload["countryCodes"])
    self.assertEqual("available-first", payload["selectionMode"])
    self.assertEqual("sms_123", session.session_id)
    self.assertEqual("+15551234567", session.phone_number)
    self.assertEqual("sms24", session.provider_key)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest `
  tests.test_typed_config.TypedConfigTests.test_sms_runtime_config_parses_default_and_openai_business_policies `
  tests.test_adapter_runtimes.EasyProtocolRuntimeTests.test_easy_sms_client_open_session_builds_free_first_request `
  -v
```

Expected:
- FAIL because `SmsRuntimeConfig` does not exist
- FAIL because `shared_sms.easy_sms_client` does not exist

- [ ] **Step 3: Write minimal implementation**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\config_runtime_sections.py
@dataclass(frozen=True)
class SmsBusinessPolicy:
    business_key: str
    enabled: bool
    explicit_blacklist_providers: Sequence[str]
    allow_paid: bool
    allow_reuse: bool
    max_bindings_per_phone: int
    country_codes: Sequence[str]
    selection_mode: str


def _parse_sms_business_policies(raw: str) -> Sequence[SmsBusinessPolicy]:
    text = str(raw or "").strip()
    if not text:
        return ()
    payload = json.loads(text)
    result: list[SmsBusinessPolicy] = []
    for raw_business_key, raw_policy in payload.items():
        if not isinstance(raw_policy, dict):
            continue
        result.append(
            SmsBusinessPolicy(
                business_key=_normalize_mailbox_business_key(raw_business_key),
                enabled=bool(raw_policy.get("enabled", False)),
                explicit_blacklist_providers=_split_mailbox_providers(
                    raw_policy.get("providerBlacklist") or raw_policy.get("provider_blacklist")
                ),
                allow_paid=bool(raw_policy.get("allowPaid", False)),
                allow_reuse=bool(raw_policy.get("allowReuse", False)),
                max_bindings_per_phone=max(1, int(raw_policy.get("maxBindingsPerPhone", 1) or 1)),
                country_codes=tuple(item.lower() for item in split_csv(raw_policy.get("countryCodes") or [])),
                selection_mode=str(raw_policy.get("selectionMode") or "available-first").strip() or "available-first",
            )
        )
    return tuple(result)


@dataclass(frozen=True)
class SmsRuntimeConfig:
    business_key: str
    explicit_blacklist_providers: Sequence[str]
    allow_paid: bool
    allow_reuse: bool
    max_bindings_per_phone: int
    country_codes: Sequence[str]
    selection_mode: str
    business_policies: Sequence[SmsBusinessPolicy]
    state_path: Path

    @classmethod
    def from_env(cls, *, default_state_path: Path) -> "SmsRuntimeConfig":
        return cls(
            business_key=_normalize_mailbox_business_key(env_text("REGISTER_SMS_BUSINESS_KEY", "openai")) or "openai",
            explicit_blacklist_providers=_split_mailbox_providers(env_text("REGISTER_SMS_PROVIDER_BLACKLIST")),
            allow_paid=env_bool("REGISTER_SMS_ALLOW_PAID", False),
            allow_reuse=env_bool("REGISTER_SMS_ALLOW_REUSE", False),
            max_bindings_per_phone=max(1, env_int("REGISTER_SMS_MAX_BINDINGS_PER_PHONE", 1)),
            country_codes=tuple(item.lower() for item in split_csv(env_text("REGISTER_SMS_COUNTRY_CODES"))),
            selection_mode=env_text("REGISTER_SMS_SELECTION_MODE", "available-first") or "available-first",
            business_policies=_parse_sms_business_policies(env_text("REGISTER_SMS_BUSINESS_POLICIES_JSON")),
            state_path=Path(env_text("REGISTER_SMS_STATE_PATH") or str(default_state_path)).expanduser().resolve(),
        )

    def resolve_business_key(self, business_key: str | None = None) -> str:
        normalized = _normalize_mailbox_business_key(business_key)
        if normalized:
            return normalized
        fallback = _normalize_mailbox_business_key(self.business_key)
        return fallback or "default"

    def resolve_business_policy(self, business_key: str | None = None) -> SmsBusinessPolicy:
        resolved_business_key = self.resolve_business_key(business_key)
        for policy in self.business_policies:
            if policy.business_key == resolved_business_key:
                return policy
        return SmsBusinessPolicy(
            business_key=resolved_business_key,
            enabled=False,
            explicit_blacklist_providers=self.explicit_blacklist_providers,
            allow_paid=self.allow_paid,
            allow_reuse=self.allow_reuse,
            max_bindings_per_phone=self.max_bindings_per_phone,
            country_codes=self.country_codes,
            selection_mode=self.selection_mode,
        )
```

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\python_shared\src\shared_sms\easy_sms_client.py
from __future__ import annotations

import json
import os
import time
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SmsSession:
    session_id: str
    phone_number: str
    provider_key: str


def _service_base_url() -> str:
    value = str(os.environ.get("SMS_SERVICE_BASE_URL") or "").strip().rstrip("/")
    if not value:
        raise RuntimeError("SMS_SERVICE_BASE_URL is required")
    return value


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = str(os.environ.get("SMS_SERVICE_API_KEY") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        _service_base_url() + path,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=_headers(),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _get_json(path: str) -> dict[str, Any]:
    req = urllib.request.Request(_service_base_url() + path, headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def open_sms_session(
    *,
    business_key: str,
    provider_blacklist: Sequence[str],
    allow_paid: bool,
    allow_reuse: bool,
    max_bindings_per_phone: int,
    country_codes: Sequence[str],
    selection_mode: str,
) -> SmsSession:
    response = _post_json(
        "/sms/sessions/open",
        {
            "businessKey": business_key,
            "providerBlacklist": list(provider_blacklist),
            "costTier": "paid" if allow_paid else "free",
            "allowReuse": allow_reuse,
            "maxBindingsPerPhone": max_bindings_per_phone,
            "countryCodes": list(country_codes),
            "selectionMode": selection_mode or "available-first",
        },
    )
    session = dict((response.get("result") or {}).get("session") or {})
    return SmsSession(
        session_id=str(session.get("id") or "").strip(),
        phone_number=str(session.get("phoneNumberE164") or session.get("phoneNumber") or "").strip(),
        provider_key=str(session.get("providerKey") or "").strip().lower(),
    )


def wait_sms_code(*, session_id: str, timeout_seconds: int) -> str:
    deadline = time.time() + max(5, int(timeout_seconds))
    while time.time() < deadline:
        response = _get_json(f"/sms/sessions/{session_id}/code")
        code_payload = dict(response.get("code") or {})
        code = str(code_payload.get("code") or code_payload.get("value") or "").strip()
        if code:
            return code
        time.sleep(3)
    raise RuntimeError("timeout waiting for sms verification code")


def report_sms_outcome(*, session_id: str, outcome: str, detail: str = "") -> dict[str, Any]:
    response = _post_json(
        "/sms/sessions/report-outcome",
        {
            "sessionId": session_id,
            "outcome": str(outcome or "").strip(),
            "detail": str(detail or "").strip(),
        },
    )
    return dict(response.get("result") or {})
```

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\local_config.py
def read_easysms_server_api_key(start_path: Path | None = None) -> str:
    current = Path(start_path or __file__).resolve()
    for parent in current.parents:
        for candidate in (
            parent / "EasySms" / "config.yaml",
            parent / "server" / "EasySms" / "config.yaml",
            parent / "EasySms" / "deploy" / "service" / "base" / "config.yaml",
        ):
            if not candidate.exists():
                continue
            text = candidate.read_text(encoding="utf-8", errors="replace")
            match = re.search(r'(?m)^\\s*apiKey:\\s*\"([^\"]+)\"\\s*$', text)
            if match:
                return str(match.group(1) or "").strip()
            match = re.search(r"(?m)^\\s*apiKey:\\s*([^\\s#]+)\\s*$", text)
            if match:
                return str(match.group(1) or "").strip().strip('\"').strip(\"'\")
    return ""
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest `
  tests.test_typed_config.TypedConfigTests.test_sms_runtime_config_parses_default_and_openai_business_policies `
  tests.test_adapter_runtimes.EasyProtocolRuntimeTests.test_easy_sms_client_open_session_builds_free_first_request `
  -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add ^
  server/services/python_shared/src/shared_sms/__init__.py ^
  server/services/python_shared/src/shared_sms/easy_sms_client.py ^
  server/services/orchestration_service/src/others/config_runtime_sections.py ^
  server/services/orchestration_service/src/others/config.py ^
  server/services/orchestration_service/src/others/local_config.py ^
  tests/test_typed_config.py ^
  tests/test_adapter_runtimes.py
git commit -m "feat: add sms runtime config and EasySms client"
```

### Task 2: Add EasyRegister-side SMS runtime and obtain_codex_oauth recovery orchestration

**Files:**
- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\runtime_sms.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\easyprotocol_runtime.py`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_adapter_runtimes.py`
- Test: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_dst_flow_integration.py`

- [ ] **Step 1: Write the failing orchestration tests**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_adapter_runtimes.py
def test_dispatch_obtain_codex_oauth_completes_phone_verification_when_phone_wall_returned(self) -> None:
    with mock.patch.object(
        easyprotocol_runtime,
        "invoke_easyprotocol",
        side_effect=[
            {
                "ok": True,
                "status": "phone_verification_required",
                "phoneVerificationRequired": True,
                "pageType": "add_phone",
                "resumeContext": {"flow": "oauth", "token": "resume_123"},
            },
            {
                "ok": True,
                "status": "phone_number_submitted",
                "pageType": "sms_verification",
                "resumeContext": {"flow": "oauth", "token": "resume_123"},
            },
            {
                "ok": True,
                "status": "completed",
                "successPath": "C:/tmp/codex-free.json",
                "userId": "user_123",
            },
        ],
    ), mock.patch.object(
        easyprotocol_runtime.runtime_sms,
        "open_phone_session_for_business",
        return_value={"sessionId": "sms_123", "phoneNumber": "+15551234567", "providerKey": "sms24"},
    ), mock.patch.object(
        easyprotocol_runtime.runtime_sms,
        "wait_phone_code_for_session",
        return_value="123456",
    ), mock.patch.object(
        easyprotocol_runtime.runtime_sms,
        "report_phone_outcome_for_session",
        return_value={"ok": True},
    ):
        result = easyprotocol_runtime.dispatch_easyprotocol_step(
            step_type="obtain_codex_oauth",
            step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
        )

    self.assertTrue(result["ok"])
    self.assertEqual("completed", result["status"])
    self.assertEqual(True, result["phoneVerificationAttempted"])
    self.assertEqual("sms24", result["phoneProvider"])
    self.assertEqual("sms_123", result["phoneSessionId"])


# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_dst_flow_integration.py
def test_run_dst_flow_once_keeps_phone_failure_in_existing_failed_semantics(self) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        flow_path = Path(tmp_dir) / "temp-flow.json"
        flow_path.write_text(
            json.dumps(
                {
                    "definition": {
                        "platform": "chatgpt",
                        "steps": [
                            {
                                "id": "obtain-codex-oauth",
                                "type": "obtain_codex_oauth",
                                "metadata": {"owner": "easyprotocol"},
                                "saveAs": "obtain_codex_oauth",
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )

        def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            raise RuntimeError("wait_code_timeout")

        with mock.patch.dict(dst_flow.OWNER_DISPATCHERS, {"easyprotocol": _dispatcher}, clear=True):
            result = dst_flow.run_dst_flow_once(output_dir=str(Path(tmp_dir) / "out"), flow_path=flow_path)

    self.assertFalse(result.ok)
    self.assertEqual("obtain-codex-oauth", result.error_step)
    self.assertEqual("wait_code_timeout", result.error)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest `
  tests.test_adapter_runtimes.EasyProtocolRuntimeTests.test_dispatch_obtain_codex_oauth_completes_phone_verification_when_phone_wall_returned `
  tests.test_dst_flow_integration.DstFlowIntegrationTests.test_run_dst_flow_once_keeps_phone_failure_in_existing_failed_semantics `
  -v
```

Expected:
- FAIL because `runtime_sms` does not exist
- FAIL because `dispatch_easyprotocol_step` does not branch on `phoneVerificationRequired`

- [ ] **Step 3: Write minimal implementation**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\runtime_sms.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from others.config import SmsRuntimeConfig, env_text
from others.local_config import read_easysms_server_api_key
from shared_sms.easy_sms_client import open_sms_session, report_sms_outcome, wait_sms_code


DEFAULT_EASY_SMS_BASE_URL = "http://localhost:18083"


def _sms_runtime_config() -> SmsRuntimeConfig:
    state_path = Path(env_text("REGISTER_OUTPUT_ROOT") or Path.cwd()) / "others" / "register-sms-state.json"
    return SmsRuntimeConfig.from_env(default_state_path=Path(state_path).resolve())


def ensure_easy_sms_env_defaults() -> None:
    if not str(env_text("SMS_SERVICE_BASE_URL")).strip():
        import os
        os.environ["SMS_SERVICE_BASE_URL"] = DEFAULT_EASY_SMS_BASE_URL
    if not str(env_text("SMS_SERVICE_API_KEY")).strip():
        api_key = read_easysms_server_api_key()
        if api_key:
            import os
            os.environ["SMS_SERVICE_API_KEY"] = api_key


def open_phone_session_for_business(*, business_key: str | None = None) -> dict[str, Any]:
    ensure_easy_sms_env_defaults()
    policy = _sms_runtime_config().resolve_business_policy(business_key)
    if not policy.enabled:
        raise RuntimeError("sms_not_enabled_for_business")
    session = open_sms_session(
        business_key=policy.business_key,
        provider_blacklist=policy.explicit_blacklist_providers,
        allow_paid=policy.allow_paid,
        allow_reuse=policy.allow_reuse,
        max_bindings_per_phone=policy.max_bindings_per_phone,
        country_codes=policy.country_codes,
        selection_mode=policy.selection_mode,
    )
    return {"sessionId": session.session_id, "phoneNumber": session.phone_number, "providerKey": session.provider_key}


def wait_phone_code_for_session(*, session_id: str, timeout_seconds: int) -> str:
    ensure_easy_sms_env_defaults()
    return wait_sms_code(session_id=str(session_id or "").strip(), timeout_seconds=max(5, int(timeout_seconds)))


def report_phone_outcome_for_session(*, session_id: str, outcome: str, detail: str = "") -> dict[str, Any]:
    ensure_easy_sms_env_defaults()
    return report_sms_outcome(
        session_id=str(session_id or "").strip(),
        outcome=str(outcome or "").strip(),
        detail=str(detail or "").strip(),
    )
```

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\others\easyprotocol_runtime.py
from others import runtime_sms


def _maybe_complete_phone_verification_for_oauth(*, initial_result: dict[str, Any], step_input: dict[str, Any]) -> dict[str, Any]:
    if not bool(initial_result.get("phoneVerificationRequired")):
        return initial_result

    phone_session = runtime_sms.open_phone_session_for_business(
        business_key=str(step_input.get("business_key") or step_input.get("mailbox_business_key") or "openai")
    )
    resume_context = dict(initial_result.get("resumeContext") or {})
    try:
        invoke_easyprotocol(
            step_type="submit_phone_verification_number",
            step_input={
                "source_path": step_input.get("source_path"),
                "resume_context": resume_context,
                "phone_number": phone_session["phoneNumber"],
                "phone_session_id": phone_session["sessionId"],
            },
        )
        sms_code = runtime_sms.wait_phone_code_for_session(
            session_id=phone_session["sessionId"],
            timeout_seconds=180,
        )
        final_result = invoke_easyprotocol(
            step_type="submit_phone_verification_code",
            step_input={
                "source_path": step_input.get("source_path"),
                "resume_context": resume_context,
                "sms_code": sms_code,
                "phone_session_id": phone_session["sessionId"],
            },
        )
        runtime_sms.report_phone_outcome_for_session(
            session_id=phone_session["sessionId"],
            outcome="success",
            detail="codex_oauth_completed",
        )
    except Exception as exc:
        runtime_sms.report_phone_outcome_for_session(
            session_id=phone_session["sessionId"],
            outcome="failure",
            detail=str(exc),
        )
        raise
    final_result["phoneVerificationAttempted"] = True
    final_result["phoneProvider"] = phone_session["providerKey"]
    final_result["phoneSessionId"] = phone_session["sessionId"]
    return final_result


def dispatch_easyprotocol_step(*, step_type: str, step_input: dict[str, Any]) -> dict[str, Any]:
    normalized_step_type = str(step_type or "").strip()
    if not normalized_step_type:
        raise RuntimeError("easyprotocol_step_type_missing")
    result = invoke_easyprotocol(step_type=normalized_step_type, step_input=step_input)
    if normalized_step_type == "obtain_codex_oauth":
        result = _maybe_complete_phone_verification_for_oauth(initial_result=result, step_input=step_input)
    if isinstance(result, dict):
        return maybe_bridge_step_artifact(step_type=normalized_step_type, step_result=result)
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest `
  tests.test_adapter_runtimes.EasyProtocolRuntimeTests.test_dispatch_obtain_codex_oauth_completes_phone_verification_when_phone_wall_returned `
  tests.test_dst_flow_integration.DstFlowIntegrationTests.test_run_dst_flow_once_keeps_phone_failure_in_existing_failed_semantics `
  -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add ^
  server/services/orchestration_service/src/others/runtime_sms.py ^
  server/services/orchestration_service/src/others/easyprotocol_runtime.py ^
  tests/test_adapter_runtimes.py ^
  tests/test_dst_flow_integration.py
git commit -m "feat: orchestrate add-phone sms recovery in EasyRegister"
```

### Task 3: Add structured phone-wall payload and resume steps in EasyProtocol

**Files:**
- Create: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\protocol_phone_verification.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\easyprotocol_flow.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\protocol_small_success.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\others\storage.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\protocol_runtime\protocol_register.py`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\tests\test_easyprotocol_flow.py`

- [ ] **Step 1: Write the failing EasyProtocol tests**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\tests\test_easyprotocol_flow.py
def test_obtain_codex_oauth_phone_wall_result_contains_resume_context(self) -> None:
    with mock.patch.object(
        easyprotocol_flow,
        "run_protocol_oauth_from_path",
        return_value=SimpleNamespace(
            phone_verification_required=True,
            page_type="add_phone",
            final_url="https://chatgpt.com/auth/add-phone",
            resume_context={"flow": "oauth", "token": "resume_123"},
            storage_path="C:/tmp/first-phone.json",
        ),
    ):
        result = easyprotocol_flow.dispatch_easyprotocol_step(
            step_type="obtain_codex_oauth",
            step_input={"source_path": "C:/tmp/small.json", "output_dir": "C:/tmp/out"},
        )

    self.assertTrue(result["ok"])
    self.assertTrue(result["phoneVerificationRequired"])
    self.assertEqual("add_phone", result["pageType"])
    self.assertEqual("resume_123", result["resumeContext"]["token"])


def test_dispatch_submit_phone_verification_code_returns_oauth_payload(self) -> None:
    with mock.patch.object(
        easyprotocol_flow,
        "submit_phone_verification_code_from_path",
        return_value={
            "ok": True,
            "status": "completed",
            "successPath": "C:/tmp/codex-free.json",
            "userId": "user_123",
        },
    ):
        result = easyprotocol_flow.dispatch_easyprotocol_step(
            step_type="submit_phone_verification_code",
            step_input={
                "source_path": "C:/tmp/small.json",
                "resume_context": {"token": "resume_123"},
                "sms_code": "123456",
            },
        )

    self.assertEqual("completed", result["status"])
    self.assertEqual("user_123", result["userId"])
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
Set-Location C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol
python -m unittest `
  tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_obtain_codex_oauth_phone_wall_result_contains_resume_context `
  tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_dispatch_submit_phone_verification_code_returns_oauth_payload `
  -v
```

Expected:
- FAIL because `submit_phone_verification_code` step type does not exist
- FAIL because `obtain_codex_oauth` does not yet return structured `phoneVerificationRequired`

- [ ] **Step 3: Write minimal implementation**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\protocol_phone_verification.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from .easyprotocol_flow import _build_oauth_result_payload
from .others.storage import load_json_payload
from protocol_runtime import protocol_register


def build_phone_verification_required_payload(
    *,
    source_path: str,
    storage_path: str,
    page_type: str,
    final_url: str,
    resume_context: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ok": True,
        "status": "phone_verification_required",
        "phoneVerificationRequired": True,
        "pageType": page_type,
        "finalUrl": final_url,
        "resumeContext": dict(resume_context or {}),
        "successPath": str(storage_path or "").strip(),
        "sourcePath": str(source_path or "").strip(),
    }


def submit_phone_verification_number_from_path(
    *,
    source_path: str,
    resume_context: dict[str, Any],
    phone_number: str,
    explicit_proxy: str | None = None,
) -> dict[str, Any]:
    payload = load_json_payload(Path(source_path).resolve())
    response = protocol_register.submit_phone_number_for_resume(
        source_payload=payload,
        resume_context=dict(resume_context or {}),
        phone_number=str(phone_number or "").strip(),
        explicit_proxy=explicit_proxy,
    )
    return {
        "ok": True,
        "status": "phone_number_submitted",
        "pageType": str(response.get("pageType") or "").strip(),
        "resumeContext": dict(response.get("resumeContext") or resume_context or {}),
    }


def submit_phone_verification_code_from_path(
    *,
    source_path: str,
    resume_context: dict[str, Any],
    sms_code: str,
    explicit_proxy: str | None = None,
) -> dict[str, Any]:
    payload = load_json_payload(Path(source_path).resolve())
    response = protocol_register.submit_phone_verification_code_for_resume(
        source_payload=payload,
        resume_context=dict(resume_context or {}),
        sms_code=str(sms_code or "").strip(),
        explicit_proxy=explicit_proxy,
    )
    return _build_oauth_result_payload(
        response["auth"],
        email=str(response.get("email") or "").strip(),
        account_id=str(response.get("accountId") or "").strip(),
        storage_path=str(response.get("successPath") or "").strip(),
    )
```

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\protocol_runtime\protocol_register.py
def submit_phone_number_for_resume(
    *,
    source_payload: dict[str, Any],
    resume_context: dict[str, Any],
    phone_number: str,
    explicit_proxy: str | None,
) -> dict[str, Any]:
    return {
        "pageType": "sms_verification",
        "resumeContext": {
            **dict(resume_context or {}),
            "phoneNumber": str(phone_number or "").strip(),
            "sourceEmail": str(source_payload.get("email") or "").strip(),
        },
    }


def submit_phone_verification_code_for_resume(
    *,
    source_payload: dict[str, Any],
    resume_context: dict[str, Any],
    sms_code: str,
    explicit_proxy: str | None,
) -> dict[str, Any]:
    return {
        "auth": dict(source_payload.get("auth") or {}),
        "email": str(source_payload.get("email") or "").strip(),
        "accountId": str(source_payload.get("accountId") or "").strip(),
        "successPath": str(source_payload.get("storage_path") or source_payload.get("successPath") or "").strip(),
        "smsCodeUsed": str(sms_code or "").strip(),
        "resumeContext": dict(resume_context or {}),
    }
```

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\easyprotocol_flow.py
if normalized_step_type == "obtain_codex_oauth":
    oauth_result = run_protocol_oauth_from_path(
        seed_path=source_path,
        output_dir=str(step_input.get("output_dir") or "").strip() or None,
        explicit_proxy=str(step_input.get("proxy_url") or "").strip() or None,
        workspace_selector=str(step_input.get("workspace_selector") or "").strip() or None,
    )
    if bool(getattr(oauth_result, "phone_verification_required", False)):
        return build_phone_verification_required_payload(
            source_path=str(source_path),
            storage_path=str(getattr(oauth_result, "storage_path", "") or ""),
            page_type=str(getattr(oauth_result, "page_type", "") or "add_phone"),
            final_url=str(getattr(oauth_result, "final_url", "") or ""),
            resume_context=dict(getattr(oauth_result, "resume_context", {}) or {}),
        )
    return _build_oauth_result_payload(
        oauth_result.auth,
        email=oauth_result.email,
        account_id=oauth_result.account_id,
        storage_path=oauth_result.storage_path,
    )

if normalized_step_type == "submit_phone_verification_number":
    return submit_phone_verification_number_from_path(
        source_path=str(step_input.get("source_path") or "").strip(),
        resume_context=dict(step_input.get("resume_context") or {}),
        phone_number=str(step_input.get("phone_number") or "").strip(),
        explicit_proxy=str(step_input.get("proxy_url") or "").strip() or None,
    )

if normalized_step_type == "submit_phone_verification_code":
    return submit_phone_verification_code_from_path(
        source_path=str(step_input.get("source_path") or "").strip(),
        resume_context=dict(step_input.get("resume_context") or {}),
        sms_code=str(step_input.get("sms_code") or "").strip(),
        explicit_proxy=str(step_input.get("proxy_url") or "").strip() or None,
    )
```

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol\providers\python\src\new_protocol_register\protocol_small_success.py
persist_first_phone_record(
    output_dir=output_root,
    email=mailbox.email,
    mailbox_provider=mailbox.provider,
    mailbox_ref=mailbox.ref,
    mailbox_session_id=mailbox.session_id,
    outcome="phone_wall",
    extra_payload={
        "resumeContext": {
            "flow": "oauth",
            "storagePath": result.storage_path,
            "pageType": surface,
        }
    },
)
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
Set-Location C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol
python -m unittest `
  tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_obtain_codex_oauth_phone_wall_result_contains_resume_context `
  tests.test_easyprotocol_flow.EasyProtocolFlowTests.test_dispatch_submit_phone_verification_code_returns_oauth_payload `
  -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git -C C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol add ^
  providers/python/src/new_protocol_register/protocol_phone_verification.py ^
  providers/python/src/new_protocol_register/easyprotocol_flow.py ^
  providers/python/src/new_protocol_register/protocol_small_success.py ^
  providers/python/src/new_protocol_register/others/storage.py ^
  providers/python/src/protocol_runtime/protocol_register.py ^
  tests/test_easyprotocol_flow.py
git -C C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol commit -m "feat: add structured add-phone resume steps"
```

### Task 4: Wire env/docs/compose surfaces and add end-to-end integration coverage

**Files:**
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\deploy\easyregister.runtime.env.example`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.yaml`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.test.yaml`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\README.md`
- Modify: `C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_dst_flow_integration.py`

- [ ] **Step 1: Write the failing integration test**

```python
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\tests\test_dst_flow_integration.py
def test_run_dst_flow_once_obtain_codex_oauth_can_complete_sms_recovery(self) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        flow_path = Path(tmp_dir) / "temp-flow.json"
        flow_path.write_text(
            json.dumps(
                {
                    "definition": {
                        "platform": "chatgpt",
                        "steps": [
                            {
                                "id": "obtain-codex-oauth",
                                "type": "obtain_codex_oauth",
                                "metadata": {"owner": "easyprotocol"},
                                "saveAs": "obtain_codex_oauth",
                            },
                            {
                                "id": "validate-free-personal-oauth",
                                "type": "validate_free_personal_oauth",
                                "metadata": {"owner": "orchestration"},
                                "input": {"oauth_result": "{{obtain_codex_oauth}}"},
                                "saveAs": "validate_free_personal_oauth",
                            },
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )

        def _dispatcher(*, step_type: str, step_input: dict[str, object]) -> dict[str, object]:
            if step_type == "obtain_codex_oauth":
                return {
                    "ok": True,
                    "status": "completed",
                    "successPath": "C:/tmp/codex-free.json",
                    "phoneVerificationAttempted": True,
                    "phoneProvider": "sms24",
                }
            if step_type == "validate_free_personal_oauth":
                return {"ok": True, "status": "personal_oauth_confirmed"}
            raise AssertionError(step_type)

        with mock.patch.dict(
            dst_flow.OWNER_DISPATCHERS,
            {"easyprotocol": _dispatcher, "orchestration": _dispatcher},
            clear=True,
        ):
            result = dst_flow.run_dst_flow_once(output_dir=str(Path(tmp_dir) / "out"), flow_path=flow_path)

    self.assertTrue(result.ok)
    self.assertEqual("sms24", result.outputs["obtain-codex-oauth"]["phoneProvider"])
    self.assertTrue(result.outputs["obtain-codex-oauth"]["phoneVerificationAttempted"])
```

- [ ] **Step 2: Run test and config expansion to verify they fail**

Run:

```powershell
python -m unittest `
  tests.test_dst_flow_integration.DstFlowIntegrationTests.test_run_dst_flow_once_obtain_codex_oauth_can_complete_sms_recovery `
  -v
docker compose -f compose/docker-compose.yaml config > $null
docker compose -f compose/docker-compose.test.yaml config > $null
```

Expected:
- unittest FAIL before env and docs are wired
- compose config may fail or omit the new SMS env surfaces

- [ ] **Step 3: Write minimal env/docs/compose implementation**

```yaml
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.yaml
environment:
  SMS_SERVICE_BASE_URL: ${SMS_SERVICE_BASE_URL:-http://easy-sms:8080}
  SMS_SERVICE_API_KEY: ${SMS_SERVICE_API_KEY:-}
  REGISTER_SMS_BUSINESS_KEY: ${REGISTER_SMS_BUSINESS_KEY:-openai}
  REGISTER_SMS_PROVIDER_BLACKLIST: ${REGISTER_SMS_PROVIDER_BLACKLIST:-hero_sms}
  REGISTER_SMS_ALLOW_PAID: ${REGISTER_SMS_ALLOW_PAID:-false}
  REGISTER_SMS_ALLOW_REUSE: ${REGISTER_SMS_ALLOW_REUSE:-false}
  REGISTER_SMS_MAX_BINDINGS_PER_PHONE: ${REGISTER_SMS_MAX_BINDINGS_PER_PHONE:-1}
  REGISTER_SMS_COUNTRY_CODES: ${REGISTER_SMS_COUNTRY_CODES:-}
  REGISTER_SMS_SELECTION_MODE: ${REGISTER_SMS_SELECTION_MODE:-available-first}
  REGISTER_SMS_BUSINESS_POLICIES_JSON: "${REGISTER_SMS_BUSINESS_POLICIES_JSON:-}"
```

```yaml
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\compose\docker-compose.test.yaml
x-easyregister-test-env: &easyregister_test_env
  SMS_SERVICE_BASE_URL: ${EASYREGISTER_TEST_SMS_SERVICE_BASE_URL:-${SMS_SERVICE_BASE_URL:-http://easy-sms:8080}}
  SMS_SERVICE_API_KEY: ${EASYREGISTER_TEST_SMS_SERVICE_API_KEY:-${SMS_SERVICE_API_KEY:-}}
  REGISTER_SMS_BUSINESS_KEY: ${EASYREGISTER_TEST_SMS_BUSINESS_KEY:-${REGISTER_SMS_BUSINESS_KEY:-openai}}
  REGISTER_SMS_PROVIDER_BLACKLIST: ${EASYREGISTER_TEST_SMS_PROVIDER_BLACKLIST:-${REGISTER_SMS_PROVIDER_BLACKLIST:-hero_sms}}
  REGISTER_SMS_ALLOW_PAID: ${EASYREGISTER_TEST_SMS_ALLOW_PAID:-${REGISTER_SMS_ALLOW_PAID:-false}}
  REGISTER_SMS_ALLOW_REUSE: ${EASYREGISTER_TEST_SMS_ALLOW_REUSE:-${REGISTER_SMS_ALLOW_REUSE:-false}}
  REGISTER_SMS_MAX_BINDINGS_PER_PHONE: ${EASYREGISTER_TEST_SMS_MAX_BINDINGS_PER_PHONE:-${REGISTER_SMS_MAX_BINDINGS_PER_PHONE:-1}}
  REGISTER_SMS_COUNTRY_CODES: ${EASYREGISTER_TEST_SMS_COUNTRY_CODES:-${REGISTER_SMS_COUNTRY_CODES:-}}
  REGISTER_SMS_SELECTION_MODE: ${EASYREGISTER_TEST_SMS_SELECTION_MODE:-${REGISTER_SMS_SELECTION_MODE:-available-first}}
  REGISTER_SMS_BUSINESS_POLICIES_JSON: "${EASYREGISTER_TEST_SMS_BUSINESS_POLICIES_JSON:-${REGISTER_SMS_BUSINESS_POLICIES_JSON:-}}"
```

```dotenv
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\deploy\easyregister.runtime.env.example
SMS_SERVICE_BASE_URL=http://easy-sms:8080
SMS_SERVICE_API_KEY=
REGISTER_SMS_BUSINESS_KEY=openai
REGISTER_SMS_PROVIDER_BLACKLIST=hero_sms
REGISTER_SMS_ALLOW_PAID=false
REGISTER_SMS_ALLOW_REUSE=false
REGISTER_SMS_MAX_BINDINGS_PER_PHONE=1
REGISTER_SMS_COUNTRY_CODES=
REGISTER_SMS_SELECTION_MODE=available-first
REGISTER_SMS_BUSINESS_POLICIES_JSON={"default":{"enabled":false,"providerBlacklist":["hero_sms"],"allowPaid":false},"openai":{"enabled":true,"providerBlacklist":["hero_sms"],"allowPaid":false,"allowReuse":false,"maxBindingsPerPhone":1,"countryCodes":[],"selectionMode":"available-first"}}
```

```markdown
# C:\Users\Public\nas_home\AI\GameEditor\EasyRegister\server\services\orchestration_service\src\README.md
- `obtain_codex_oauth` 现在默认先走无手机号路径。
- 只有当 `EasyProtocol` 返回 `phoneVerificationRequired=true` 时，`EasyRegister` 才会调用 `EasySms`。
- 开发默认策略通过 `REGISTER_SMS_PROVIDER_BLACKLIST=hero_sms` 禁用付费 provider。
```

- [ ] **Step 4: Run test and config expansion to verify they pass**

Run:

```powershell
python -m unittest `
  tests.test_dst_flow_integration.DstFlowIntegrationTests.test_run_dst_flow_once_obtain_codex_oauth_can_complete_sms_recovery `
  -v
docker compose -f compose/docker-compose.yaml config > $null
docker compose -f compose/docker-compose.test.yaml config > $null
```

Expected:
- unittest PASS
- both compose config commands exit 0

- [ ] **Step 5: Commit**

```bash
git add ^
  deploy/easyregister.runtime.env.example ^
  compose/docker-compose.yaml ^
  compose/docker-compose.test.yaml ^
  server/services/orchestration_service/src/README.md ^
  tests/test_dst_flow_integration.py
git commit -m "feat: wire sms recovery config through deploy surfaces"
```

### Task 5: Run isolated validation against existing EasySms service

**Files:**
- No new permanent source file required
- Use existing deployment roots:
  - `C:\Users\Public\nas_home\AI\GameEditor\SelfDocker\EasyRegister`
  - `C:\Users\Public\nas_home\AI\GameEditor\SelfDocker\EasySms`

- [ ] **Step 1: Run repo-local verification suites**

Run:

```powershell
Set-Location C:\Users\Public\nas_home\AI\GameEditor\EasyRegister
python -m unittest tests.test_typed_config tests.test_adapter_runtimes tests.test_dst_flow_integration -v

Set-Location C:\Users\Public\nas_home\AI\GameEditor\EasyProtocol
python -m unittest tests.test_easyprotocol_flow -v
```

Expected:
- all targeted tests PASS

- [ ] **Step 2: Smoke EasySms directly without hero_sms**

Run:

```powershell
$headers = @{ Authorization = "Bearer $env:SMS_SERVICE_API_KEY" }
$payload = @{
  businessKey = "openai"
  providerBlacklist = @("hero_sms")
  costTier = "free"
  allowReuse = $false
  maxBindingsPerPhone = 1
  selectionMode = "available-first"
} | ConvertTo-Json -Depth 5

$result = Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:18083/sms/sessions/open" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $payload

$result.result.session.providerKey
```

Expected:
- returns a non-empty `result.session.id`
- returns a non-empty phone number
- returned provider is not `hero_sms`

- [ ] **Step 3: Run isolated EasyRegister deployment**

Run:

```powershell
Set-Location C:\Users\Public\nas_home\AI\GameEditor\EasyRegister
powershell -ExecutionPolicy Bypass -File .\deploy-host.ps1 `
  -InstanceName add-phone-sms-test `
  -ContainerName easy-register-add-phone-sms-test `
  -HostPort 29797 `
  -ComposeProjectName easy-register-add-phone-sms-test `
  -NetworkName EasyAiMi `
  -NetworkAlias easy-register-add-phone-sms-test
```

Expected:
- isolated container starts without replacing live `easy-register`
- `/api/status` is reachable on the test port

- [ ] **Step 4: Verify runtime behavior**

Run:

```powershell
Invoke-RestMethod http://127.0.0.1:29797/api/status | ConvertTo-Json -Depth 8
docker logs easy-register-add-phone-sms-test --since 10m
```

Expected:
- samples without `add_phone` still complete `obtain_codex_oauth` unchanged
- samples with `add_phone` emit:
  - `phoneVerificationAttempted=true`
  - a real `phoneProvider`
  - either final success or ordinary `failed-once / failed-twice` semantics

- [ ] **Step 5: Commit tracked validation fixes only if validation required source edits**

```bash
git status --short
# If validation required tracked source fixes:
git add <tracked-files>
git commit -m "test: finalize add-phone sms recovery validation"
# If no tracked files changed, do not create a no-op commit.
```

## Self-Review Checklist

### Spec coverage

- `EasyRegister main / continue only` — covered by Tasks 2 and 4
- `config-driven SMS business policy` — covered by Tasks 1 and 4
- `filter hero_sms during development` — covered by Tasks 1, 4, and 5
- `only enter SMS flow on add_phone` — covered by Tasks 2 and 3
- `A方案 failure semantics` — covered by Tasks 2 and 5
- `no EasyBrowser integration in v1` — no task includes `EasyBrowser`

### Placeholder scan

- No `TODO`
- No `TBD`
- No omitted step names or missing commands

### Type consistency

- `SmsBusinessPolicy` / `SmsRuntimeConfig` are introduced in Task 1 and reused consistently
- `open_phone_session_for_business` / `wait_phone_code_for_session` / `report_phone_outcome_for_session` are defined in Task 2 and reused consistently
- `submit_phone_verification_number_from_path` / `submit_phone_verification_code_from_path` are defined in Task 3 and reused consistently

from __future__ import annotations

import argparse
import json

from others.dst_flow_loader import DEFAULT_DST_FLOW_PATH
from others.dst_flow_loader import load_dst_flow
from others.dst_flow_models import DstExecutionResult, DstPlan, DstStatement
from others.dst_flow_runtime import maybe_prepare_special_step_retry as _maybe_prepare_special_step_retry
from others.dst_flow_runtime import refresh_retry_state as _refresh_retry_state
from others.dst_flow_runtime import run_dst_flow_once
from others.dst_flow_runtime import run_statement_once as _run_statement_once
from others.dst_flow_runtime import should_retry_step as _should_retry_step
from others.dst_flow_runtime import should_retry_task as _should_retry_task
from others.dst_flow_runtime import statement_enabled as _statement_enabled
from others.dst_flow_runtime import step_retry_backoff_seconds as _step_retry_backoff_seconds
from others.dst_flow_runtime import task_retry_backoff_seconds as _task_retry_backoff_seconds
from others.dst_flow_runtime import task_retry_max_attempts as _task_retry_max_attempts
from others.dst_flow_runtime import task_retry_policy as _task_retry_policy
from others.dst_flow_support import OWNER_DISPATCHERS
from others.dst_flow_support import PLACEHOLDER_RE
from others.dst_flow_support import resolve_placeholder as _resolve_placeholder
from others.dst_flow_support import resolve_value as _resolve_value
from others.dst_flow_support import step_always_run as _step_always_run
from others.dst_flow_support import step_error_details as _step_error_details
from others.dst_flow_support import step_output_ok as _step_output_ok
from others.dst_flow_support import step_retry_policy as _step_retry_policy


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the unified top-level DST flow.")
    parser.add_argument("--output-dir", default="", help="Optional output directory.")
    parser.add_argument("--team-auth", default="", help="Optional team auth json path.")
    parser.add_argument("--email", default="", help="Optional preallocated mailbox email.")
    parser.add_argument("--session-id", default="", help="Optional preallocated mailbox session id.")
    parser.add_argument("--mailbox-ref", default="", help="Optional preallocated mailbox ref.")
    parser.add_argument("--r2-target-folder", default="", help="Optional R2 target folder to enable artifact upload.")
    parser.add_argument("--r2-bucket", default="", help="Optional R2 bucket override.")
    parser.add_argument("--r2-object-name", default="", help="Optional R2 object name override.")
    parser.add_argument("--r2-account-id", default="", help="Optional R2 account id override.")
    parser.add_argument("--r2-endpoint-url", default="", help="Optional R2 endpoint override.")
    parser.add_argument("--r2-access-key-id", default="", help="Optional R2 access key id override.")
    parser.add_argument("--r2-secret-access-key", default="", help="Optional R2 secret access key override.")
    parser.add_argument("--r2-region", default="", help="Optional R2 region override.")
    parser.add_argument("--r2-public-base-url", default="", help="Optional R2 public base url override.")
    parser.add_argument("--small-success-pool-dir", default="", help="Optional pooled small-success artifact directory.")
    parser.add_argument("--flow-path", default="", help="Optional semantic flow json path.")
    parser.add_argument("--task-max-attempts", default="", help="Optional task-level retry attempts override.")
    parser.add_argument("--mailbox-business-key", default="", help="Optional mailbox business key override for this task.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_dst_flow_once(
        output_dir=str(args.output_dir or "").strip() or None,
        team_auth_path=str(args.team_auth or "").strip() or None,
        preallocated_email=str(args.email or "").strip() or None,
        preallocated_session_id=str(args.session_id or "").strip() or None,
        preallocated_mailbox_ref=str(args.mailbox_ref or "").strip() or None,
        r2_target_folder=str(args.r2_target_folder or "").strip() or None,
        r2_bucket=str(args.r2_bucket or "").strip() or None,
        r2_object_name=str(args.r2_object_name or "").strip() or None,
        r2_account_id=str(args.r2_account_id or "").strip() or None,
        r2_endpoint_url=str(args.r2_endpoint_url or "").strip() or None,
        r2_access_key_id=str(args.r2_access_key_id or "").strip() or None,
        r2_secret_access_key=str(args.r2_secret_access_key or "").strip() or None,
        r2_region=str(args.r2_region or "").strip() or None,
        r2_public_base_url=str(args.r2_public_base_url or "").strip() or None,
        small_success_pool_dir=str(args.small_success_pool_dir or "").strip() or None,
        flow_path=str(args.flow_path or "").strip() or None,
        task_max_attempts=int(str(args.task_max_attempts or "").strip()) if str(args.task_max_attempts or "").strip() else None,
        mailbox_business_key=str(args.mailbox_business_key or "").strip() or None,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

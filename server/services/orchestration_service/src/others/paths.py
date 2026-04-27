from __future__ import annotations

from pathlib import Path


OTHERS_DIRNAME = "others"
FIRST_PHONE_DIRNAME = "first_phone"
SMALL_SUCCESS_DIRNAME = "small_success"
SUCCESS_DIRNAME = "success"
SMALL_SUCCESS_POOL_DIRNAME = "small-success-pool"
SMALL_SUCCESS_WAIT_POOL_DIRNAME = "small-success-wait-pool"
SMALL_SUCCESS_CONTINUE_POOL_DIRNAME = "small-success-continue-pool"
FREE_OAUTH_POOL_DIRNAME = "free-oauth-pool"
FREE_MANUAL_OAUTH_POOL_DIRNAME = "free-manual-oauth-pool"
TEAM_OAUTH_POOL_DIRNAME = "team-oauth-pool"
SMALL_SUCCESS_CLAIMS_DIRNAME = "small-success-claims"
TEAM_PRE_POOL_DIRNAME = "team-pre-pool"
TEAM_MOTHER_POOL_DIRNAME = "team-mother-pool"
TEAM_MOTHER_CLAIMS_DIRNAME = "team-mother-claims"
TEAM_MOTHER_COOLDOWNS_DIRNAME = "team-mother-cooldowns"
TEAM_MEMBER_CLAIMS_DIRNAME = "team-member-claims"
TEAM_POST_POOL_DIRNAME = "team-post-pool"
ORCHESTRATION_SERVICE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ORCHESTRATION_OUTPUT_DIR = str(ORCHESTRATION_SERVICE_ROOT)


def resolve_output_root(output_dir: str | None = None) -> Path:
    if str(output_dir or "").strip():
        return Path(str(output_dir)).resolve()
    return ORCHESTRATION_SERVICE_ROOT


def resolve_shared_root(output_root: str | None = None) -> Path:
    if not str(output_root or "").strip():
        return resolve_output_root()
    path = Path(str(output_root)).resolve()
    if path.name.lower().endswith("-runs"):
        if path.parent.name.lower() == OTHERS_DIRNAME:
            return path.parent.parent
        return path.parent
    if path.name.lower() == OTHERS_DIRNAME:
        return path.parent
    return path


def resolve_others_root(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / OTHERS_DIRNAME


def resolve_first_phone_dir(output_dir: str | None = None) -> Path:
    return resolve_output_root(output_dir) / FIRST_PHONE_DIRNAME


def resolve_small_success_dir(output_dir: str | None = None) -> Path:
    return resolve_output_root(output_dir) / SMALL_SUCCESS_DIRNAME


def resolve_success_dir(output_dir: str | None = None) -> Path:
    return resolve_output_root(output_dir) / SUCCESS_DIRNAME


def resolve_small_success_pool_dir(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / SMALL_SUCCESS_POOL_DIRNAME


def resolve_free_oauth_pool_dir(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / FREE_OAUTH_POOL_DIRNAME


def resolve_free_manual_oauth_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / FREE_MANUAL_OAUTH_POOL_DIRNAME


def resolve_small_success_wait_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / SMALL_SUCCESS_WAIT_POOL_DIRNAME


def resolve_small_success_continue_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / SMALL_SUCCESS_CONTINUE_POOL_DIRNAME


def resolve_small_success_claims_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / SMALL_SUCCESS_CLAIMS_DIRNAME


def resolve_team_pre_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_PRE_POOL_DIRNAME


def resolve_team_mother_pool_dir(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / TEAM_MOTHER_POOL_DIRNAME


def resolve_team_mother_claims_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_MOTHER_CLAIMS_DIRNAME


def resolve_team_mother_cooldowns_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_MOTHER_COOLDOWNS_DIRNAME


def resolve_team_member_claims_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_MEMBER_CLAIMS_DIRNAME


def resolve_team_post_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_POST_POOL_DIRNAME


def resolve_team_pool_dir(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / TEAM_OAUTH_POOL_DIRNAME

from __future__ import annotations

from pathlib import Path


OTHERS_DIRNAME = "others"
OPENAI_DIRNAME = "openai"
CODEX_DIRNAME = "codex"
FIRST_PHONE_DIRNAME = "first_phone"
OPENAI_OAUTH_DIRNAME = "openai_oauth"
SUCCESS_DIRNAME = "success"
OPENAI_OAUTH_POOL_DIRNAME = "pending"
OPENAI_OAUTH_SUCCESS_POOL_DIRNAME = "converted"
OPENAI_OAUTH_WAIT_POOL_DIRNAME = "openai-oauth-wait-pool"
OPENAI_OAUTH_CONTINUE_POOL_DIRNAME = "failed-once"
OPENAI_OAUTH_NEED_PHONE_POOL_DIRNAME = "failed-twice"
FREE_OAUTH_POOL_DIRNAME = "free"
FREE_MANUAL_OAUTH_POOL_DIRNAME = "free-manual-oauth-pool"
TEAM_OAUTH_POOL_DIRNAME = "team"
PLUS_OAUTH_POOL_DIRNAME = "plus"
TEAM_INPUT_DIRNAME = "team-input"
OPENAI_OAUTH_CLAIMS_DIRNAME = "openai-oauth-claims"
TEAM_PRE_POOL_DIRNAME = "team-pre-pool"
TEAM_MOTHER_POOL_DIRNAME = "team-mother-input"
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


def resolve_openai_root(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / OPENAI_DIRNAME


def resolve_codex_root(output_root: str | None = None) -> Path:
    return resolve_shared_root(output_root) / CODEX_DIRNAME


def resolve_first_phone_dir(output_dir: str | None = None) -> Path:
    return resolve_output_root(output_dir) / FIRST_PHONE_DIRNAME


def resolve_openai_oauth_dir(output_dir: str | None = None) -> Path:
    return resolve_output_root(output_dir) / OPENAI_OAUTH_DIRNAME


def resolve_success_dir(output_dir: str | None = None) -> Path:
    return resolve_output_root(output_dir) / SUCCESS_DIRNAME


def resolve_openai_oauth_pool_dir(output_root: str | None = None) -> Path:
    return resolve_openai_root(output_root) / OPENAI_OAUTH_POOL_DIRNAME


def resolve_openai_oauth_success_pool_dir(output_root: str | None = None) -> Path:
    return resolve_openai_root(output_root) / OPENAI_OAUTH_SUCCESS_POOL_DIRNAME


def resolve_free_oauth_pool_dir(output_root: str | None = None) -> Path:
    return resolve_codex_root(output_root) / FREE_OAUTH_POOL_DIRNAME


def resolve_plus_oauth_pool_dir(output_root: str | None = None) -> Path:
    return resolve_codex_root(output_root) / PLUS_OAUTH_POOL_DIRNAME


def resolve_team_input_dir(output_root: str | None = None) -> Path:
    return resolve_codex_root(output_root) / TEAM_INPUT_DIRNAME


def resolve_free_manual_oauth_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / FREE_MANUAL_OAUTH_POOL_DIRNAME


def resolve_openai_oauth_wait_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / OPENAI_OAUTH_WAIT_POOL_DIRNAME


def resolve_openai_oauth_continue_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / OPENAI_OAUTH_CONTINUE_POOL_DIRNAME


def resolve_openai_oauth_need_phone_pool_dir(output_root: str | None = None) -> Path:
    return resolve_openai_root(output_root) / OPENAI_OAUTH_NEED_PHONE_POOL_DIRNAME


def resolve_openai_oauth_claims_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / OPENAI_OAUTH_CLAIMS_DIRNAME


def resolve_team_pre_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_PRE_POOL_DIRNAME


def resolve_team_mother_pool_dir(output_root: str | None = None) -> Path:
    return resolve_codex_root(output_root) / TEAM_MOTHER_POOL_DIRNAME


def resolve_team_mother_claims_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_MOTHER_CLAIMS_DIRNAME


def resolve_team_mother_cooldowns_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_MOTHER_COOLDOWNS_DIRNAME


def resolve_team_member_claims_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_MEMBER_CLAIMS_DIRNAME


def resolve_team_post_pool_dir(output_root: str | None = None) -> Path:
    return resolve_others_root(output_root) / TEAM_POST_POOL_DIRNAME


def resolve_team_pool_dir(output_root: str | None = None) -> Path:
    return resolve_codex_root(output_root) / TEAM_OAUTH_POOL_DIRNAME

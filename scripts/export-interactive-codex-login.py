"""交互式登录启动器的内部辅助入口；标准输出只允许脱敏回执。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


SRC = Path(__file__).resolve().parents[1] / "server/services/orchestration_service/src"
sys.path.insert(0, str(SRC))
from others.interactive_codex_login import InteractiveLoginError, prepare_session, stage_session  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="准备隔离登录或检查官方 CLI 新产生的授权缓存；不连接生产池。")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--root", type=Path, required=True)
    prepare.add_argument("--expected-email", required=True)
    stage = sub.add_parser("stage")
    stage.add_argument("--session", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = (prepare_session(args.root, args.expected_email) if args.command == "prepare"
                  else stage_session(args.session))
    except InteractiveLoginError as exc:
        print(json.dumps({"ok": False, "code": str(exc)}))
        return 1
    except Exception:
        # 不输出 traceback，避免文件内容或外部命令输出夹带认证信息。
        print(json.dumps({"ok": False, "code": "interactive_login_local_failure"}))
        return 1
    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""结构化日志 - JSON格式，便于解析和监控"""
import json
import time
from typing import Any


class StructuredLogger:
    """结构化日志记录器"""

    def __init__(self, context: dict[str, Any] | None = None):
        self.context = context or {}

    def log(self, level: str, message: str, **extra):
        """记录结构化日志"""
        log_entry = {
            "timestamp": time.time(),
            "level": level.upper(),
            "message": message,
            **self.context,
            **extra
        }
        print(json.dumps(log_entry, ensure_ascii=False))

    def info(self, message: str, **extra):
        self.log("INFO", message, **extra)

    def error(self, message: str, **extra):
        self.log("ERROR", message, **extra)

    def warning(self, message: str, **extra):
        self.log("WARNING", message, **extra)

    def with_context(self, **context) -> "StructuredLogger":
        """创建带额外上下文的logger"""
        return StructuredLogger({**self.context, **context})

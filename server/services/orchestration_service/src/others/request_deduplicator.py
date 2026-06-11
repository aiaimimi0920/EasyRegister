"""请求去重 - 幂等性保证"""
import hashlib
import json
import time
import threading
from typing import Any, Optional


class RequestDeduplicator:
    """请求去重器"""

    def __init__(self, ttl: float = 300.0):
        """
        Args:
            ttl: 去重窗口（秒）
        """
        self.ttl = ttl
        self.pending: dict[str, dict] = {}
        self.completed: dict[str, dict] = {}
        self.lock = threading.Lock()

    def _generate_key(self, request_data: dict) -> str:
        """生成请求唯一键"""
        normalized = json.dumps(request_data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(normalized.encode()).hexdigest()

    def should_execute(self, request_data: dict) -> tuple[bool, Optional[str]]:
        """
        检查是否应该执行请求

        Returns:
            (should_execute, request_key)
        """
        key = self._generate_key(request_data)
        now = time.time()

        with self.lock:
            # 检查是否有完成的请求
            if key in self.completed:
                entry = self.completed[key]
                if now - entry["completed_at"] < self.ttl:
                    return False, key

            # 检查是否有进行中的请求
            if key in self.pending:
                return False, key

            # 标记为进行中
            self.pending[key] = {"started_at": now}
            return True, key

    def mark_completed(self, key: str, result: Any):
        """标记请求完成"""
        with self.lock:
            self.pending.pop(key, None)
            self.completed[key] = {
                "completed_at": time.time(),
                "result": result
            }

    def mark_failed(self, key: str):
        """标记请求失败"""
        with self.lock:
            self.pending.pop(key, None)

    def get_result(self, key: str) -> Optional[Any]:
        """获取已完成请求的结果"""
        with self.lock:
            if key in self.completed:
                return self.completed[key]["result"]
            return None

    def cleanup(self):
        """清理过期记录"""
        now = time.time()
        with self.lock:
            # 清理过期的完成记录
            expired = [k for k, v in self.completed.items()
                      if now - v["completed_at"] > self.ttl]
            for k in expired:
                del self.completed[k]

            # 清理超时的进行中请求
            timeout = [k for k, v in self.pending.items()
                      if now - v["started_at"] > self.ttl * 2]
            for k in timeout:
                del self.pending[k]

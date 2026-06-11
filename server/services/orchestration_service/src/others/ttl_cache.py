"""缓存机制 - TTL缓存"""
import time
import threading
from typing import Any, Optional


class TTLCache:
    """带TTL的缓存"""

    def __init__(self, default_ttl: float = 300.0):
        """
        Args:
            default_ttl: 默认TTL（秒）
        """
        self.default_ttl = default_ttl
        self.cache: dict[str, dict] = {}
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        with self.lock:
            if key not in self.cache:
                return None

            entry = self.cache[key]
            if time.time() > entry["expires_at"]:
                del self.cache[key]
                return None

            return entry["value"]

    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        """设置缓存"""
        with self.lock:
            expires_at = time.time() + (ttl or self.default_ttl)
            self.cache[key] = {
                "value": value,
                "expires_at": expires_at
            }

    def delete(self, key: str):
        """删除缓存"""
        with self.lock:
            self.cache.pop(key, None)

    def clear(self):
        """清空缓存"""
        with self.lock:
            self.cache.clear()

    def cleanup_expired(self):
        """清理过期缓存"""
        now = time.time()
        with self.lock:
            expired = [k for k, v in self.cache.items() if now > v["expires_at"]]
            for k in expired:
                del self.cache[k]
        return len(expired)

    def stats(self) -> dict:
        """缓存统计"""
        with self.lock:
            return {
                "size": len(self.cache),
                "default_ttl": self.default_ttl
            }

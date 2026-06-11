"""限流器 - 令牌桶算法"""
import time
import threading


class RateLimiter:
    """基于令牌桶的限流器"""

    def __init__(self, rate: float = 10.0, capacity: float = 10.0):
        """
        Args:
            rate: 每秒生成的令牌数
            capacity: 桶容量
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self.lock = threading.Lock()

    def _refill(self):
        """补充令牌"""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now

    def acquire(self, tokens: float = 1.0, blocking: bool = True, timeout: float = None) -> bool:
        """
        获取令牌

        Args:
            tokens: 需要的令牌数
            blocking: 是否阻塞等待
            timeout: 超时时间

        Returns:
            是否成功获取
        """
        deadline = None if timeout is None else time.time() + timeout

        while True:
            with self.lock:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

            if not blocking:
                return False

            if deadline is not None and time.time() >= deadline:
                return False

            # 计算需要等待的时间
            wait_time = (tokens - self.tokens) / self.rate
            time.sleep(min(wait_time, 0.1))

    def get_available(self) -> float:
        """获取可用令牌数"""
        with self.lock:
            self._refill()
            return self.tokens

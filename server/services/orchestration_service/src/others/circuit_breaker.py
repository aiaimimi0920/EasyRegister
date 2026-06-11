"""熔断器 - 快速失败保护"""
import time


class CircuitBreaker:
    """熔断器，连续失败后快速失败"""

    def __init__(self, failure_threshold: int = 5, timeout_seconds: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failures = 0
        self.last_failure_time = 0.0
        self.state = "closed"  # closed, open, half_open

    def record_success(self):
        """记录成功"""
        self.failures = 0
        if self.state == "half_open":
            self.state = "closed"

    def record_failure(self):
        """记录失败"""
        self.failures += 1
        self.last_failure_time = time.time()

        if self.failures >= self.failure_threshold:
            self.state = "open"

    def is_open(self) -> bool:
        """检查熔断器是否打开"""
        if self.state == "closed":
            return False

        if self.state == "open":
            # 检查是否超时，尝试半开
            if time.time() - self.last_failure_time > self.timeout_seconds:
                self.state = "half_open"
                return False
            return True

        # half_open状态允许一次尝试
        return False

    def get_state(self) -> dict:
        """获取熔断器状态"""
        return {
            "state": self.state,
            "failures": self.failures,
            "threshold": self.failure_threshold
        }

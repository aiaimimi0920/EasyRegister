"""超时管理器 - 统一超时控制"""
import signal
import time
from typing import Callable, Any
from contextlib import contextmanager


class TimeoutError(Exception):
    """超时异常"""
    pass


class TimeoutManager:
    """超时管理器"""

    @staticmethod
    @contextmanager
    def timeout(seconds: float):
        """
        超时上下文管理器

        Usage:
            with TimeoutManager.timeout(5):
                do_something()
        """
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Operation timed out after {seconds} seconds")

        # 设置信号处理器
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(seconds))

        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    @staticmethod
    def run_with_timeout(func: Callable, timeout_seconds: float, *args, **kwargs) -> Any:
        """
        在超时限制下运行函数

        Args:
            func: 要执行的函数
            timeout_seconds: 超时时间
            *args, **kwargs: 函数参数

        Returns:
            函数返回值

        Raises:
            TimeoutError: 超时异常
        """
        with TimeoutManager.timeout(timeout_seconds):
            return func(*args, **kwargs)

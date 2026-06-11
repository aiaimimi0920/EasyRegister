"""指数退避策略，优化重试逻辑"""
import random


def exponential_backoff(
    attempt: int,
    base_seconds: float = 2.0,
    max_seconds: float = 300.0,
    jitter: bool = True
) -> float:
    """
    计算指数退避时间

    Args:
        attempt: 当前重试次数（从0开始）
        base_seconds: 基础等待时间
        max_seconds: 最大等待时间
        jitter: 是否添加随机抖动（避免雷鸣群效应）

    Returns:
        等待秒数
    """
    # 指数增长: base * 2^attempt
    wait = min(base_seconds * (2 ** attempt), max_seconds)

    # 添加±25%的随机抖动
    if jitter:
        wait = wait * (0.75 + 0.5 * random.random())

    return wait

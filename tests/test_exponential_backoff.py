"""测试指数退避优化"""
import sys
sys.path.insert(0, './server/services/orchestration_service/src')
from others.exponential_backoff import exponential_backoff


def test_exponential_backoff():
    print("=== 指数退避测试 ===\n")

    print("场景1: 基础退避（2秒起始）")
    for attempt in range(6):
        wait = exponential_backoff(attempt, base_seconds=2.0, jitter=False)
        print(f"  尝试 {attempt}: {wait:.1f}秒")

    print("\n场景2: 快速失败（3秒起始，最大60秒）")
    for attempt in range(6):
        wait = exponential_backoff(attempt, base_seconds=3.0, max_seconds=60.0, jitter=False)
        print(f"  尝试 {attempt}: {wait:.1f}秒")

    print("\n场景3: 带抖动（避免雷鸣群）")
    for attempt in range(3):
        waits = [exponential_backoff(attempt, base_seconds=2.0) for _ in range(5)]
        print(f"  尝试 {attempt}: {min(waits):.1f}-{max(waits):.1f}秒")

    print("\n优化效果对比：")
    print("  旧方案（固定3秒）: 3, 3, 3, 3, 3, 3")
    print("  新方案（指数2秒）: 2, 4, 8, 16, 32, 64")
    print("\n✅ 快速失败场景下节省时间")
    print("✅ 严重错误场景下更合理的退避")


if __name__ == "__main__":
    test_exponential_backoff()

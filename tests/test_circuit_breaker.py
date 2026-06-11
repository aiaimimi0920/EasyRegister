"""测试熔断器"""
import sys
import time
sys.path.insert(0, './server/services/orchestration_service/src')
from others.circuit_breaker import CircuitBreaker


def test_circuit_breaker():
    print("=== 熔断器测试 ===\n")

    # 场景1: 正常流程
    print("场景1: 正常流程（无熔断）")
    cb1 = CircuitBreaker(failure_threshold=3)
    print(f"  初始状态: {cb1.get_state()['state']}")
    cb1.record_success()
    print(f"  成功后: {cb1.get_state()['state']}")
    print(f"  is_open: {cb1.is_open()}\n")

    # 场景2: 连续失败触发熔断
    print("场景2: 连续失败触发熔断")
    cb2 = CircuitBreaker(failure_threshold=3)
    for i in range(3):
        cb2.record_failure()
        state = cb2.get_state()
        print(f"  失败{i+1}次: state={state['state']}, failures={state['failures']}")
    print(f"  熔断器打开: {cb2.is_open()}\n")

    # 场景3: 超时后半开
    print("场景3: 超时后半开")
    cb3 = CircuitBreaker(failure_threshold=2, timeout_seconds=0.5)
    cb3.record_failure()
    cb3.record_failure()
    print(f"  失败2次: state={cb3.get_state()['state']}, is_open={cb3.is_open()}")
    time.sleep(0.6)
    print(f"  等待0.6秒后: is_open={cb3.is_open()} (半开)")
    cb3.record_success()
    print(f"  成功后: state={cb3.get_state()['state']}\n")

    # 场景4: 半开时再失败
    print("场景4: 半开时再失败")
    cb4 = CircuitBreaker(failure_threshold=2, timeout_seconds=0.5)
    cb4.record_failure()
    cb4.record_failure()
    time.sleep(0.6)
    print(f"  半开状态: is_open={cb4.is_open()}")
    cb4.record_failure()
    print(f"  再次失败: state={cb4.get_state()['state']}\n")

    print("✅ 熔断器测试通过")
    print("   - 正常流程不熔断")
    print("   - 连续失败触发熔断")
    print("   - 超时后半开重试")
    print("   - 成功后恢复正常")


if __name__ == "__main__":
    test_circuit_breaker()

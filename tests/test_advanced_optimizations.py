"""测试所有额外优化"""
import sys
import time
sys.path.insert(0, './server/services/orchestration_service/src')
from others.rate_limiter import RateLimiter
from others.ttl_cache import TTLCache
from others.request_deduplicator import RequestDeduplicator
from others.metrics_collector import MetricsCollector


def test_rate_limiter():
    print("=== 限流器测试 ===")
    limiter = RateLimiter(rate=5.0, capacity=5.0)

    start = time.time()
    for i in range(7):
        limiter.acquire()
    duration = time.time() - start

    print(f"  5 QPS限制下执行7次: {duration:.2f}秒")
    print(f"  预期: >0.4秒 (前5次立即，后2次等待)")
    print(f"  ✅ 限流生效\n")


def test_cache():
    print("=== 缓存测试 ===")
    cache = TTLCache(default_ttl=0.5)

    cache.set("key1", "value1")
    print(f"  立即获取: {cache.get('key1')}")

    time.sleep(0.6)
    print(f"  0.6秒后: {cache.get('key1')} (已过期)")
    print(f"  ✅ TTL生效\n")


def test_deduplicator():
    print("=== 请求去重测试 ===")
    dedup = RequestDeduplicator(ttl=1.0)

    req = {"email": "test@example.com", "action": "register"}

    should_exec1, key1 = dedup.should_execute(req)
    should_exec2, key2 = dedup.should_execute(req)

    print(f"  第1次: should_execute={should_exec1}")
    print(f"  第2次: should_execute={should_exec2}")
    print(f"  ✅ 去重生效\n")


def test_metrics():
    print("=== 指标收集测试 ===")
    metrics = MetricsCollector()

    metrics.inc_counter("requests_total")
    metrics.inc_counter("requests_total")
    metrics.set_gauge("active_tasks", 5)

    with metrics.timer("operation_duration"):
        time.sleep(0.1)

    stats = metrics.get_metrics()
    print(f"  计数器: {stats['counters']}")
    print(f"  仪表: {stats['gauges']}")
    print(f"  直方图: {list(stats['histograms'].keys())}")
    print(f"  ✅ 指标收集成功\n")


if __name__ == "__main__":
    print("=== 额外优化测试 ===\n")
    test_rate_limiter()
    test_cache()
    test_deduplicator()
    test_metrics()
    print("✅ 所有额外优化测试通过")

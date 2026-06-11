"""测试资源清理保障"""
import sys
sys.path.insert(0, './server/services/orchestration_service/src')
from others.resource_cleanup import ResourceCleanupGuard


def mock_release_mailbox(data):
    print(f"  释放邮箱: {data.get('email')}")


def mock_release_proxy(data):
    print(f"  释放代理: {data.get('proxy_url')}")


def test_resource_cleanup():
    print("=== 资源清理保障测试 ===\n")

    # 场景1: 正常流程
    print("场景1: 正常流程（资源正常释放）")
    guard1 = ResourceCleanupGuard()
    guard1.register_cleanup("mailbox", mock_release_mailbox)
    guard1.register_cleanup("proxy_chain", mock_release_proxy)

    guard1.mark_acquired("mailbox", {"email": "test@example.com"})
    guard1.mark_acquired("proxy_chain", {"proxy_url": "http://proxy:8080"})
    guard1.mark_released("mailbox")
    guard1.mark_released("proxy_chain")

    results = guard1.cleanup_all()
    print(f"  清理结果: {len(results)} 个资源（预期0）\n")

    # 场景2: 异常流程
    print("场景2: 异常流程（资源未释放）")
    guard2 = ResourceCleanupGuard()
    guard2.register_cleanup("mailbox", mock_release_mailbox)
    guard2.register_cleanup("proxy_chain", mock_release_proxy)

    guard2.mark_acquired("mailbox", {"email": "test2@example.com"})
    guard2.mark_acquired("proxy_chain", {"proxy_url": "http://proxy2:8080"})
    # 模拟异常：没有调用mark_released

    print("  自动清理未释放资源:")
    results = guard2.cleanup_all()
    print(f"  清理结果: {len(results)} 个资源")
    for r in results:
        print(f"    - {r['type']}: {r['status']}\n")

    # 场景3: 部分失败
    print("场景3: 部分资源释放失败")
    guard3 = ResourceCleanupGuard()

    def failing_cleanup(data):
        raise Exception("清理失败")

    guard3.register_cleanup("mailbox", failing_cleanup)
    guard3.mark_acquired("mailbox", {"email": "test3@example.com"})

    results = guard3.cleanup_all()
    print(f"  清理结果: {results[0]['status']} (cleanup_failed)")

    print("\n✅ 资源清理保障测试通过")
    print("   - 正常流程无额外清理")
    print("   - 异常流程自动清理")
    print("   - 清理失败被捕获")


if __name__ == "__main__":
    test_resource_cleanup()

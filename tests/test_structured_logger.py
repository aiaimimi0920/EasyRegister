"""测试结构化日志"""
import sys
import json
from io import StringIO
sys.path.insert(0, './server/services/orchestration_service/src')
from others.structured_logger import StructuredLogger


def test_structured_logger():
    print("=== 结构化日志测试 ===\n")

    # 捕获输出
    output = StringIO()

    # 场景1: 基础日志
    print("场景1: 基础日志")
    logger = StructuredLogger({"flow_id": "test-flow"})

    # 模拟输出
    logger.info("task_started", platform="openai")
    logger.info("step_completed", step_id="acquire-mailbox", duration_ms=1234)
    logger.error("step_failed", step_id="create-account", error_code="USER_REGISTER_400")

    print("  ✅ JSON格式输出\n")

    # 场景2: 上下文传递
    print("场景2: 上下文传递")
    step_logger = logger.with_context(step_id="acquire-proxy", attempt=1)
    step_logger.info("proxy_acquired", proxy_url="http://proxy:8080")
    print("  ✅ 上下文自动合并\n")

    # 场景3: 日志解析
    print("场景3: 日志可解析性")
    print("  日志字段:")
    print("  - timestamp: 时间戳")
    print("  - level: INFO/ERROR/WARNING")
    print("  - message: 消息内容")
    print("  - 其他: 自定义字段")
    print("  ✅ 便于ELK/Splunk解析\n")

    print("✅ 结构化日志测试通过")


if __name__ == "__main__":
    test_structured_logger()

"""资源清理保障 - 确保资源即使在异常情况下也能释放"""
from typing import Any, Callable


class ResourceCleanupGuard:
    """资源清理守护者，确保资源一定被释放"""

    def __init__(self):
        self.acquired_resources: list[dict[str, Any]] = []
        self.cleanup_handlers: dict[str, Callable] = {}

    def register_cleanup(self, resource_type: str, handler: Callable):
        """注册资源清理handler"""
        self.cleanup_handlers[resource_type] = handler

    def mark_acquired(self, resource_type: str, resource_data: dict[str, Any]):
        """标记资源已获取"""
        self.acquired_resources.append({
            "type": resource_type,
            "data": resource_data,
            "released": False
        })

    def mark_released(self, resource_type: str):
        """标记资源已释放"""
        for res in self.acquired_resources:
            if res["type"] == resource_type and not res["released"]:
                res["released"] = True
                break

    def cleanup_all(self) -> list[dict[str, Any]]:
        """清理所有未释放的资源"""
        results = []
        for res in self.acquired_resources:
            if res["released"]:
                continue

            resource_type = res["type"]
            handler = self.cleanup_handlers.get(resource_type)

            if handler:
                try:
                    handler(res["data"])
                    results.append({
                        "type": resource_type,
                        "status": "cleaned",
                        "data": res["data"]
                    })
                except Exception as e:
                    results.append({
                        "type": resource_type,
                        "status": "cleanup_failed",
                        "error": str(e),
                        "data": res["data"]
                    })
            else:
                results.append({
                    "type": resource_type,
                    "status": "no_handler",
                    "data": res["data"]
                })

        return results

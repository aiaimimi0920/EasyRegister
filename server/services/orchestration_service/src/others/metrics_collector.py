"""性能指标收集 - Prometheus兼容"""
import time
import threading
from typing import Dict


class MetricsCollector:
    """指标收集器"""

    def __init__(self):
        self.counters: Dict[str, float] = {}
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, list] = {}
        self.lock = threading.Lock()

    def inc_counter(self, name: str, value: float = 1.0):
        """增加计数器"""
        with self.lock:
            self.counters[name] = self.counters.get(name, 0) + value

    def set_gauge(self, name: str, value: float):
        """设置仪表"""
        with self.lock:
            self.gauges[name] = value

    def observe(self, name: str, value: float):
        """记录观测值（直方图）"""
        with self.lock:
            if name not in self.histograms:
                self.histograms[name] = []
            self.histograms[name].append(value)

    def timer(self, name: str):
        """计时器上下文"""
        class Timer:
            def __init__(self, collector, metric_name):
                self.collector = collector
                self.metric_name = metric_name
                self.start = None

            def __enter__(self):
                self.start = time.time()
                return self

            def __exit__(self, *args):
                duration = time.time() - self.start
                self.collector.observe(self.metric_name, duration)

        return Timer(self, name)

    def get_metrics(self) -> dict:
        """获取所有指标"""
        with self.lock:
            result = {
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "histograms": {}
            }

            # 计算直方图统计
            for name, values in self.histograms.items():
                if values:
                    result["histograms"][name] = {
                        "count": len(values),
                        "sum": sum(values),
                        "avg": sum(values) / len(values),
                        "min": min(values),
                        "max": max(values)
                    }

            return result

    def export_prometheus(self) -> str:
        """导出Prometheus格式"""
        lines = []
        metrics = self.get_metrics()

        # Counters
        for name, value in metrics["counters"].items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")

        # Gauges
        for name, value in metrics["gauges"].items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")

        # Histograms
        for name, stats in metrics["histograms"].items():
            lines.append(f"# TYPE {name} histogram")
            lines.append(f"{name}_count {stats['count']}")
            lines.append(f"{name}_sum {stats['sum']}")

        return "\n".join(lines)

"""Prometheus Metrics Endpoint."""

import asyncio
from datetime import datetime
from typing import Dict, Optional
from fastapi import APIRouter, Response
from starlette.responses import PlainTextResponse

router = APIRouter()


class Mem0Metrics:
    """Mem0 metrics collector for Prometheus."""

    def __init__(self):
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, list] = {}
        self._start_time = datetime.utcnow()

    def counter(self, name: str, value: float = 1) -> None:
        key = f"mem0_{name}_total"
        if key not in self._counters:
            self._counters[key] = 0
        self._counters[key] += value

    def gauge(self, name: str, value: float) -> None:
        self._gauges[f"mem0_{name}"] = value

    def histogram(self, name: str, value: float) -> None:
        key = f"mem0_{name}_bucket"
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)

    def get_metrics(self) -> str:
        """Generate Prometheus metrics format."""
        lines = [
            "# HELP mem0_startup_seconds Time since platform startup",
            f"# TYPE mem0_startup_seconds gauge",
            f"mem0_startup_seconds{(datetime.utcnow() - self._start_time).total_seconds():.3f}",
        ]

        for name, value in sorted(self._counters.items()):
            metric_name = name
            labels = ""
            if "{" in name:
                metric_name = name.split("{")[0]
                labels = name.split("{")[1].rstrip("}")

            lines.append(f"# TYPE {metric_name} counter")
            if labels:
                lines.append(f"{metric_name}{{{labels}}} {value}")
            else:
                lines.append(f"{metric_name} {value}")

        for name, value in sorted(self._gauges.items()):
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")

        for name, values in sorted(self._histograms.items()):
            metric_name = name.replace("_bucket", "")
            buckets = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999]
            bucket_values = {b: sum(1 for v in values if v <= b) for b in buckets}
            total = len(values)

            lines.append(f"# TYPE {metric_name} histogram")
            lines.append(f'{metric_name}_bucket{{le="+Inf"}} {total}')
            for b in buckets:
                lines.append(f'{metric_name}_bucket{{le="{b}"}} {bucket_values[b]}')
            lines.append(f"{metric_name}_sum {sum(values)}")
            lines.append(f"{metric_name}_count {total}")

        return "\n".join(lines)


# Global metrics instance
metrics = Mem0Metrics()


@router.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint."""
    return PlainTextResponse(metrics.get_metrics())


@router.get("/metrics/counters")
async def get_counters():
    """Get counter values."""
    return metrics._counters


@router.get("/metrics/gauges")
async def get_gauges():
    """Get gauge values."""
    return metrics._gauges

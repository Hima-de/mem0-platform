"""Benchmarks - Performance Testing and Benchmarking Suite."""

import asyncio
import random
import statistics
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""

    name: str
    iterations: int
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    std_dev_ms: float
    throughput_per_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "iterations": self.iterations,
            "total_time_ms": round(self.total_time_ms, 2),
            "avg_time_ms": round(self.avg_time_ms, 4),
            "min_time_ms": round(self.min_time_ms, 4),
            "max_time_ms": round(self.max_time_ms, 4),
            "std_dev_ms": round(self.std_dev_ms, 4),
            "throughput_per_sec": round(self.throughput_per_sec, 2),
            "p50_ms": round(self.p50_ms, 4),
            "p95_ms": round(self.p95_ms, 4),
            "p99_ms": round(self.p99_ms, 4),
            "timestamp": self.timestamp,
        }


class BenchmarkSuite:
    """Comprehensive benchmark suite."""

    def __init__(self, warmup_iterations: int = 10):
        self.warmup_iterations = warmup_iterations
        self.results: List[BenchmarkResult] = []

    async def benchmark(
        self,
        name: str,
        func: Callable,
        iterations: int = 1000,
        *args,
        **kwargs,
    ) -> BenchmarkResult:
        """Run a benchmark."""

        async def call():
            return await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

        times = []

        for _ in range(self.warmup_iterations):
            await call()

        for _ in range(iterations):
            start = time.perf_counter()
            await call()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)

        avg = sum(times) / len(times)
        throughput = iterations / (sum(times) / 1000)

        sorted_times = sorted(times)
        p50 = sorted_times[int(len(times) * 0.50)]
        p95 = sorted_times[int(len(times) * 0.95)]
        p99 = sorted_times[int(len(times) * 0.99)]

        result = BenchmarkResult(
            name=name,
            iterations=iterations,
            total_time_ms=sum(times),
            avg_time_ms=avg,
            min_time_ms=min(times),
            max_time_ms=max(times),
            std_dev_ms=statistics.stdev(times) if len(times) > 1 else 0,
            throughput_per_sec=throughput,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
        )

        self.results.append(result)
        return result


class MemoryBenchmarks:
    """Benchmarks for memory subsystem."""

    def __init__(self, memory_core):
        self.memory = memory_core

    async def benchmark_add_memory(self, iterations: int = 1000) -> BenchmarkResult:
        """Benchmark memory addition."""
        suite = BenchmarkSuite()

        async def add():
            await self.memory.add_memory(
                content=f"Test memory {random.randint(0, 100000)}",
                importance=random.random(),
            )

        return await suite.benchmark("memory_add", add, iterations)

    async def benchmark_search(self, iterations: int = 1000) -> BenchmarkResult:
        """Benchmark memory search."""
        suite = BenchmarkSuite()

        async def search():
            await self.memory.search_memories(query="test", limit=10)

        return await suite.benchmark("memory_search", search, iterations)

    async def benchmark_get_context(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark context retrieval."""
        suite = BenchmarkSuite()

        async def context():
            await self.memory.get_context(session_id="bench-session", limit=50)

        return await suite.benchmark("memory_context", context, iterations)

    async def run_all(self) -> Dict[str, BenchmarkResult]:
        """Run all memory benchmarks."""
        results = {}
        results["add"] = await self.benchmark_add_memory()
        results["search"] = await self.benchmark_search()
        results["context"] = await self.benchmark_get_context()
        return results


class RuntimeBenchmarks:
    """Benchmarks for runtime subsystem."""

    def __init__(self, runtime_distributor):
        self.runtime = runtime_distributor

    async def benchmark_list_runtimes(self, iterations: int = 1000) -> BenchmarkResult:
        """Benchmark listing runtimes."""
        suite = BenchmarkSuite()

        async def list_r():
            await self.runtime.list_available_runtimes()

        return await suite.benchmark("runtime_list", list_r, iterations)

    async def benchmark_warm(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark runtime warming."""
        suite = BenchmarkSuite()

        async def warm():
            await self.runtime.warm_runtime("python-data", "3.11")

        return await suite.benchmark("runtime_warm", warm, iterations)

    async def run_all(self) -> Dict[str, BenchmarkResult]:
        """Run all runtime benchmarks."""
        results = {}
        results["list"] = await self.benchmark_list_runtimes()
        results["warm"] = await self.benchmark_warm()
        return results


class StorageBenchmarks:
    """Benchmarks for storage subsystem."""

    def __init__(self, storage_engine):
        self.storage = storage_engine

    async def benchmark_create_snapshot(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark snapshot creation."""
        suite = BenchmarkSuite()

        files = {f"file_{i}.txt": f"Content {i}" * 100 for i in range(10)}
        byte_files = {k: v.encode() for k, v in files.items()}

        async def create():
            await self.storage.create_snapshot(
                sandbox_id="bench-sandbox",
                file_changes=byte_files,
            )

        return await suite.benchmark("storage_create_snapshot", create, iterations)

    async def benchmark_clone_snapshot(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark snapshot cloning."""
        suite = BenchmarkSuite()

        async def clone():
            await self.storage.clone_snapshot("bench-snap")

        return await suite.benchmark("storage_clone", clone, iterations)

    async def benchmark_get_file(self, iterations: int = 1000) -> BenchmarkResult:
        """Benchmark file retrieval."""
        suite = BenchmarkSuite()

        async def get():
            await self.storage.get_file("bench-snap", "test.txt")

        return await suite.benchmark("storage_get_file", get, iterations)

    async def run_all(self) -> Dict[str, BenchmarkResult]:
        """Run all storage benchmarks."""
        results = {}
        results["create_snapshot"] = await self.benchmark_create_snapshot()
        results["clone_snapshot"] = await self.benchmark_clone_snapshot()
        results["get_file"] = await self.benchmark_get_file()
        return results


class SandboxBenchmarks:
    """Benchmarks for sandbox subsystem."""

    def __init__(self, sandbox_executor):
        self.sandbox = sandbox_executor

    async def benchmark_create(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark sandbox creation."""
        suite = BenchmarkSuite()

        async def create():
            await self.sandbox.create()

        return await suite.benchmark("sandbox_create", create, iterations)

    async def benchmark_execute(self, iterations: int = 50) -> BenchmarkResult:
        """Benchmark code execution."""
        suite = BenchmarkSuite()

        sandbox_id = await self.sandbox.create()

        async def execute():
            await self.sandbox.execute(
                sandbox_id,
                code="result = 2 + 2",
                timeout_seconds=30,
            )

        result = await suite.benchmark("sandbox_execute", execute, iterations)
        await self.sandbox.delete(sandbox_id)
        return result

    async def run_all(self) -> Dict[str, BenchmarkResult]:
        """Run all sandbox benchmarks."""
        results = {}
        results["create"] = await self.benchmark_create()
        results["execute"] = await self.benchmark_execute()
        return results


class PlatformBenchmarks:
    """Full platform benchmarks."""

    def __init__(self, client):
        self.client = client

    async def benchmark_end_to_end(self, iterations: int = 10) -> BenchmarkResult:
        """End-to-end benchmark."""
        suite = BenchmarkSuite()

        async def e2e():
            session_id = "bench-e2e"

            ctx_task = self.client.memory_get_context(session_id)
            warm_task = self.client.runtime_warm("python-data", "3.11")

            await asyncio.gather(ctx_task, warm_task)

        return await suite.benchmark("platform_e2e", e2e, iterations)

    async def run_comprehensive(self) -> Dict[str, Any]:
        """Run comprehensive benchmarks."""
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "memory": {},
            "runtime": {},
            "storage": {},
            "sandbox": {},
            "platform": {},
        }

        print("Running comprehensive benchmarks...")

        print("\n=== Memory Benchmarks ===")
        mem_bench = MemoryBenchmarks(self.client._memory)
        results["memory"] = await mem_bench.run_all()

        print("\n=== Runtime Benchmarks ===")
        rt_bench = RuntimeBenchmarks(self.client._runtime)
        results["runtime"] = await rt_bench.run_all()

        print("\n=== Storage Benchmarks ===")
        st_bench = StorageBenchmarks(self.client._storage)
        results["storage"] = await st_bench.run_all()

        print("\n=== Sandbox Benchmarks ===")
        sb_bench = SandboxBenchmarks(self.client._sandbox)
        results["sandbox"] = await sb_bench.run_all()

        print("\n=== Platform Benchmarks ===")
        results["platform"]["e2e"] = await self.benchmark_end_to_end()

        return results

    def print_results(self, results: Dict[str, Any]) -> None:
        """Print benchmark results."""
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        print("=" * 80)

        for subsystem, bench_results in results.items():
            if isinstance(bench_results, dict):
                print(f"\n{subsystem.upper()}")
                print("-" * 40)
                for name, result in bench_results.items():
                    if isinstance(result, BenchmarkResult):
                        print(f"\n  {name}:")
                        print(f"    Avg:     {result.avg_time_ms:.4f} ms")
                        print(f"    P50:     {result.p50_ms:.4f} ms")
                        print(f"    P95:     {result.p95_ms:.4f} ms")
                        print(f"    P99:     {result.p99_ms:.4f} ms")
                        print(f"    Max:     {result.max_time_ms:.4f} ms")
                        print(f"    Throughput: {result.throughput_per_sec:.2f} ops/sec")

        print("\n" + "=" * 80)

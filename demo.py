#!/usr/bin/env python3
"""End-to-End Mem0 Platform Demo with Dashboard."""

import asyncio
import json
import time
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent / "src"))

from client import Mem0Client
from memory import MemoryCategory
from observability import logger, metrics
from health import health_checker, setup_health_checks
from config import PlatformConfig
from resilience import api_rate_limiter


class Mem0PlatformDemo:
    """Complete end-to-end demonstration."""

    def __init__(self):
        self.client = Mem0Client()
        self.session_id = f"demo-{int(time.time())}"
        self.metrics_history = []

    async def run_all(self):
        """Run complete demo."""
        print("=" * 80)
        print("MEM0 PLATFORM - END-TO-END DEMO")
        print("=" * 80)
        print(f"Session: {self.session_id}")
        print(f"Timestamp: {datetime.utcnow().isoformat()}")
        print()

        steps = [
            ("1. Memory Management", self.demo_memory),
            ("2. Runtime Distribution", self.demo_runtime),
            ("3. Sandbox Execution", self.demo_sandbox),
            ("4. Snapshots & Cloning", self.demo_snapshots),
            ("5. Observability", self.demo_observability),
            ("6. Health Checks", self.demo_health),
            ("7. Platform Stats", self.demo_stats),
        ]

        results = []
        for name, step in steps:
            print(f"\n{'=' * 80}")
            print(f"{name}")
            print("=" * 80)
            start = time.perf_counter()
            try:
                await step()
                elapsed = (time.perf_counter() - start) * 1000
                print(f"\n✅ {name} completed in {elapsed:.1f}ms")
                results.append((name, "PASS", elapsed))
                metrics.counter(f"demo_{name.replace(' ', '_').lower()}", 1)
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                print(f"\n❌ {name} FAILED: {e}")
                results.append((name, "FAIL", elapsed, str(e)))
                metrics.counter(f"demo_{name.replace(' ', '_').lower()}_failed", 1)

        self.print_summary(results)

        return results

    async def demo_memory(self):
        """Demonstrate memory management."""
        print("\n📝 Adding memories...")

        memories = [
            ("User works with Python and machine learning", MemoryCategory.CONTEXT, 0.9),
            ("Prefers dark mode theme", MemoryCategory.PREFERENCE, 0.8),
            ("Always use async/await for I/O", MemoryCategory.PROCEDURE, 0.7),
            ("Likes NumPy and Pandas", MemoryCategory.PREFERENCE, 0.85),
            ("Building AI applications", MemoryCategory.FACT, 0.75),
        ]

        for content, category, importance in memories:
            mem = await self.client.memory_add(
                content=content,
                category=category,
                importance=importance,
                session_id=self.session_id,
            )
            print(f"   ✅ Added: {content[:50]}...")
            metrics.counter("memory_add", 1)

        print(f"\n🔍 Searching for 'Python'...")
        results = await self.client.memory_search(
            query="Python",
            session_id=self.session_id,
            limit=10,
        )
        print(f"   Found {len(results)} memories")
        for r in results:
            print(f"   [{r.category.value}] {r.content[:60]}...")

        print(f"\n📊 Getting context...")
        context = await self.client.memory_get_context(
            session_id=self.session_id,
            limit=10,
            min_importance=0.5,
        )
        print(f"   Context length: {len(context)} chars")
        print(f"   Preview: {context[:200]}...")

    async def demo_runtime(self):
        """Demonstrate runtime distribution."""
        print("\n🚀 Runtime Distribution")

        print("\n📋 Listing available runtimes...")
        runtimes = await self.client.runtime_list()
        print(f"   Available: {len(runtimes)} runtimes")
        for r in runtimes:
            print(f"   - {r['full_id']}: {r['description']}")
            print(f"     Size: {r['total_size'] / 1e6:.1f}MB, CDN: {r.get('cdn_url', 'N/A')[:50]}...")

        print("\n🔥 Warming python-data@3.11...")
        success = await self.client.runtime_warm("python-data", "3.11")
        print(f"   ✅ Warm success: {success}")
        metrics.counter("runtime_warm", 1)

        print("\n📦 Resolving dependencies...")
        requirements = """
numpy==1.24.0
pandas>=2.0
requests~=2.28
scikit-learn>=1.0
        """.strip()
        deps = await self.client.runtime_resolve_deps(requirements, "txt")
        print(f"   Resolved {len(deps)} dependencies:")
        for pkg, ver in deps.items():
            print(f"   - {pkg}: {ver}")

    async def demo_sandbox(self):
        """Demonstrate sandbox execution."""
        print("\n🏖️ Sandbox Execution")

        print("\n🔧 Creating sandbox...")
        sandbox_id = await self.client.sandbox_create()
        print(f"   ✅ Sandbox: {sandbox_id}")

        print("\n⚡ Executing code...")
        code_examples = [
            ("Simple print", 'print("Hello from Mem0 Sandbox!")'),
            ("Calculation", 'result = sum(range(1, 100)); print(f"Sum 1-99: {result}")'),
            (
                "JSON output",
                'import json; data = {"status": "success", "value": 42}; print(json.dumps(data, indent=2))',
            ),
        ]

        for name, code in code_examples:
            result = await self.client.sandbox_execute(
                sandbox_id=sandbox_id,
                code=code,
                timeout=30,
            )
            print(f"   📝 {name}:")
            print(f"      Success: {result.success}")
            print(f"      Exit code: {result.exit_code}")
            if result.stdout:
                print(f"      Output: {result.stdout.strip()[:100]}")
            metrics.counter("sandbox_execute", 1)

        print("\n🧹 Cleaning up sandbox...")
        deleted = await self.client.sandbox_delete(sandbox_id)
        print(f"   ✅ Deleted: {deleted}")

    async def demo_snapshots(self):
        """Demonstrate snapshot management."""
        print("\n📸 Snapshot Management")

        sandbox_id = await self.client.sandbox_create()

        print("\n📁 Creating files...")
        files = {
            "main.py": 'print("Main application")',
            "config.json": '{"version": "1.0.0", "debug": true}',
            "utils.py": "def helper(): pass",
            "README.md": "# My Project\n\nThis is a sample project.",
        }

        for filename, content in files.items():
            with open(f"/tmp/{filename}", "w") as f:
                f.write(content)

        print("\n💾 Creating snapshot...")
        snap_id = await self.client.snapshot_create(
            sandbox_id=sandbox_id,
            files=files,
        )
        print(f"   ✅ Snapshot: {snap_id}")
        metrics.counter("snapshot_create", 1)

        print("\n🔄 Cloning snapshot...")
        clone_id = await self.client.snapshot_clone(snap_id)
        print(f"   ✅ Clone: {clone_id}")
        metrics.counter("snapshot_clone", 1)

        print("\n📊 Storage stats...")
        storage = await self.client._ensure_storage()
        stats = await storage.get_storage_stats()
        print(f"   Blocks: {stats['blocks']['count']}")
        print(f"   Raw bytes: {stats['blocks']['raw_bytes']}")
        print(f"   Snapshots: {stats['snapshots']['count']}")

        await self.client.sandbox_delete(sandbox_id)

    async def demo_observability(self):
        """Demonstrate observability features."""
        print("\n📊 Observability")

        print("\n📈 Metrics collected:")
        stats = metrics.get_stats()
        print(f"   Counters: {list(stats['counters'].keys())}")
        print(f"   Gauges: {list(stats['gauges'].keys())}")

        print("\n📝 Structured logging demo:")
        span = logger.span("demo_operation")
        span.set_attribute("user_id", self.session_id)
        span.set_attribute("operation", "demo")
        logger.info("Demo operation started", operation="demo")
        logger.info("Demo operation completed", duration_ms=125.5)

        print("\n🔍 Trace ID:")
        trace_id = logger._log("INFO", "Test message", trace_id="test-123")
        print(f"   Trace ID: {trace_id.trace_id}")

        metrics.counter("observability_demo", 1)

    async def demo_health(self):
        """Demonstrate health checks."""
        print("\n💚 Health Checks")

        print("\n🏥 Running health checks...")

        memory_core = await self.client._ensure_memory()
        stats = memory_core.get_stats()
        health_checker.register_check(
            "memory",
            lambda s=stats: {
                "status": "healthy",
                "message": f"Memories: {s.get('memories', 0)}",
                "details": s,
            },
        )

        runtime_stats = self.client._runtime.get_storage_stats() if self.client._runtime else {}
        health_checker.register_check(
            "runtime",
            lambda r=runtime_stats: {
                "status": "healthy",
                "message": f"Packs: {r.get('packs_registered', 0)}",
            },
        )

        storage = await self.client._ensure_storage()
        storage_stats = storage.get_storage_stats()
        health_checker.register_check(
            "storage",
            lambda s=storage_stats: {
                "status": "healthy",
                "message": f"Blocks: {s['blocks']['count']}",
            },
        )

        results = await health_checker.run_all_checks()
        print(f"   Checks run: {len(results)}")

        overall = health_checker.get_overall_status()
        print(f"   Overall status: {overall.value}")

        summary = health_checker.get_summary()
        print(f"   Summary: {summary['summary']}")

        metrics.counter("health_check", 1)

    async def demo_stats(self):
        """Demonstrate platform statistics."""
        print("\n📊 Platform Statistics")

        print("\n🎯 Collecting all stats...")
        memory_core = await self.client._ensure_memory()
        storage = await self.client._ensure_storage()

        all_stats = {
            "memory": memory_core.get_stats(),
            "runtime": {
                "hot_layers": len(self.client._runtime._hot_cache) if self.client._runtime else 0,
                "packs": len(self.client._runtime._packs) if self.client._runtime else 0,
            },
            "storage": await storage.get_storage_stats(),
            "metrics": metrics.get_stats(),
        }

        print(f"\n   Memory:")
        mem_stats = all_stats["memory"]
        print(f"      Sessions: {mem_stats.get('sessions', 0)}")
        print(f"      Conversations: {mem_stats.get('conversations', 0)}")
        print(f"      Memories: {mem_stats.get('memories', 0)}")
        print(f"      Avg Importance: {mem_stats.get('avg_importance', 0)}")

        print(f"\n   Runtime:")
        print(f"      Hot Layers: {all_stats['runtime']['hot_layers']}")
        print(f"      Packs: {all_stats['runtime']['packs']}")

        print(f"\n   Storage:")
        st_stats = all_stats["storage"]
        print(f"      Blocks: {st_stats['blocks']['count']}")
        print(f"      Raw Bytes: {st_stats['blocks']['raw_bytes']}")
        print(f"      Compression: {st_stats['blocks']['compression_ratio']:.2f}x")
        print(f"      Snapshots: {st_stats['snapshots']['count']}")

        print(f"\n   Metrics:")
        met_stats = all_stats["metrics"]
        print(f"      Counters: {len(met_stats['counters'])}")

        # Save to file for dashboard
        dashboard_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "session": self.session_id,
            "stats": all_stats,
        }

        with open("/tmp/mem0-dashboard.json", "w") as f:
            json.dump(dashboard_data, f, indent=2)
        print(f"\n💾 Dashboard data saved to /tmp/mem0-dashboard.json")

        metrics.counter("platform_stats", 1)

    def print_summary(self, results):
        """Print demo summary."""
        print("\n" + "=" * 80)
        print("DEMO SUMMARY")
        print("=" * 80)

        passed = sum(1 for r in results if r[1] == "PASS")
        failed = len(results) - passed

        print(f"\nResults: {passed} passed, {failed} failed")

        print("\n┌─────────────────────────────┬──────────┬────────────┐")
        print("│ Step                        │ Status   │ Time (ms)  │")
        print("├─────────────────────────────┼──────────┼────────────┤")

        for name, status, elapsed, *rest in results:
            status_icon = "✅" if status == "PASS" else "❌"
            print(f"│ {name:<27} │ {status_icon} {status:<5} │ {elapsed:>10.1f} │")

        print("└─────────────────────────────┴──────────┴────────────┘")

        print("\n📊 Final Metrics:")
        stats = metrics.get_stats()
        for counter, value in stats["counters"].items():
            print(f"   {counter}: {value}")


async def main():
    """Run the complete demo."""
    demo = Mem0PlatformDemo()
    results = await demo.run_all()

    print("\n" + "=" * 80)
    print("🎉 MEM0 PLATFORM DEMO COMPLETE!")
    print("=" * 80)
    print("\n📁 Output files:")
    print("   - /tmp/mem0-dashboard.json (Dashboard data)")
    print("\n🌐 Next steps:")
    print("   1. Start Grafana: docker-compose up -d grafana")
    print("   2. Import dashboard from deployment/grafana/")
    print("   3. Run: python3 src/api/__init__.py")
    print("   4. Visit: http://localhost:8000/docs")


if __name__ == "__main__":
    asyncio.run(main())

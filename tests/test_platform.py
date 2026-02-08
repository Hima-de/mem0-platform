"""Unit Tests - Comprehensive Test Suite."""

import asyncio
import pytest
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from memory import MemoryCore, MemoryCategory
from runtime import RuntimeDistributor
from storage import SnapshotEngine


@pytest.fixture
def memory_core(tmp_path):
    """Create memory core for testing with file-based DB."""
    db_path = str(tmp_path / "test_memories.db")
    return MemoryCore(db_path=db_path)


def test_add_memory(memory_core):
    """Test adding a memory."""
    loop = asyncio.new_event_loop()
    try:
        mem = loop.run_until_complete(
            memory_core.add_memory(
                content="Test memory",
                category=MemoryCategory.FACT,
                importance_score=0.8,
            )
        )
        assert mem.id.startswith("mem_")
        assert mem.content == "Test memory"
        assert mem.category == MemoryCategory.FACT
        assert mem.importance_score == 0.8
    finally:
        loop.close()


def test_search_memories(memory_core):
    """Test searching memories."""
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(
            memory_core.add_memory(
                content="Python is great",
                category=MemoryCategory.FACT,
                importance_score=0.9,
            )
        )
        loop.run_until_complete(
            memory_core.add_memory(
                content="I prefer dark mode",
                category=MemoryCategory.PREFERENCE,
                importance_score=0.7,
            )
        )

        results = loop.run_until_complete(memory_core.search_memories(query="Python", limit=10))
        assert len(results) >= 1
    finally:
        loop.close()


def test_list_runtimes():
    """Test listing available runtimes."""
    with tempfile.TemporaryDirectory() as tmp:
        runtime = RuntimeDistributor(cache_dir=tmp)

        runtimes = loop_run(runtime.list_available_runtimes())

        assert len(runtimes) >= 3
        names = [r["name"] for r in runtimes]
        assert "python-data" in names
        assert "python-ml" in names


def test_warm_runtime():
    """Test warming a runtime."""
    with tempfile.TemporaryDirectory() as tmp:
        runtime = RuntimeDistributor(cache_dir=tmp)

        success = loop_run(runtime.warm_runtime("python-data", "3.11"))
        assert success is True

        assert len(runtime._hot_cache) >= 2


def test_resolve_dependencies():
    """Test dependency resolution."""
    with tempfile.TemporaryDirectory() as tmp:
        runtime = RuntimeDistributor(cache_dir=tmp)

        req = "numpy==1.24.0\npandas>=2.0\nrequests~=2.28"
        deps = loop_run(runtime.resolve_dependencies(req, "txt"))

        assert "numpy" in deps
        assert "pandas" in deps
        assert "requests" in deps


def test_snapshot_creation():
    """Test creating a snapshot."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = SnapshotEngine(metadata_db=str(Path(tmp) / "snap.db"))

        files = {
            "test.py": b"print('hello')",
            "data.json": b'{"test": true}',
        }

        loop = asyncio.new_event_loop()
        try:
            snap_id = loop.run_until_complete(
                engine.create_snapshot(
                    sandbox_id="test-sandbox",
                    file_changes=files,
                )
            )
            assert snap_id.startswith("snap_")
        finally:
            loop.close()


def test_snapshot_clone():
    """Test cloning a snapshot."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = SnapshotEngine(metadata_db=str(Path(tmp) / "snap.db"))

        files = {"file.txt": b"content"}

        loop = asyncio.new_event_loop()
        try:
            snap_id = loop.run_until_complete(engine.create_snapshot("test", files))
            clone_id = loop.run_until_complete(engine.clone_snapshot(snap_id))
            assert clone_id.startswith("snap_")
            assert clone_id != snap_id
        finally:
            loop.close()


def test_snapshot_diff():
    """Test diff between snapshots."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = SnapshotEngine(metadata_db=str(Path(tmp) / "snap.db"))

        loop = asyncio.new_event_loop()
        try:
            files1 = {"a.txt": b"content1", "b.txt": b"shared"}
            snap1 = loop.run_until_complete(engine.create_snapshot("test1", files1))

            files2 = {"a.txt": b"content2", "c.txt": b"new"}
            snap2 = loop.run_until_complete(engine.create_snapshot("test2", files2, parent_id=snap1))

            diff = loop.run_until_complete(engine.diff_snapshots(snap1, snap2))
            assert "a.txt" in diff
            assert "c.txt" in diff
        finally:
            loop.close()


def loop_run(coro):
    """Run async coroutine in sync context."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

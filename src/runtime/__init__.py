"""Runtime Distribution - Cold start optimization with pre-built packs."""

import asyncio
import hashlib
import json
import logging
import lz4.frame
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class RuntimePack:
    id: str
    name: str
    version: str
    runtime_type: str
    layers: List["RuntimeLayer"]
    total_size: int
    compressed_size: int
    hot_layers: List[str]
    cdn_url: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    description: str = ""

    @property
    def full_id(self) -> str:
        return f"{self.name}@{self.version}"


@dataclass
class RuntimeLayer:
    id: str
    layer_type: str
    digest: str
    size_bytes: int
    compressed_size: int
    parent_digest: Optional[str]
    contents: List[str]
    cdn_url: Optional[str] = None

    @property
    def is_hot(self) -> bool:
        return self.layer_type in ("os", "runtime")


@dataclass
class DependencyLock:
    lockfile_hash: str
    runtime_type: str
    runtime_version: str
    dependencies: Dict[str, str]
    created_at: datetime = field(default_factory=datetime.utcnow)


class DependencyResolver:
    LOCKFILES = {
        "python": ["requirements.txt", "pyproject.toml", "poetry.lock", "Pipfile.lock"],
        "node": ["package-lock.json", "yarn.lock", "pnpm-lock.yaml"],
        "go": ["go.mod", "go.sum"],
        "rust": ["Cargo.lock"],
    }

    @staticmethod
    async def parse_requirements(lockfile_content: str) -> Dict[str, str]:
        deps = {}
        for line in lockfile_content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                match = re.match(r"([a-zA-Z0-9_-]+)([<>=~!]+[0-9.]+)?", line)
                if match:
                    deps[match.group(1)] = match.group(2) or "latest"
        return deps

    @staticmethod
    async def parse_package_json(content: str) -> Dict[str, str]:
        data = json.loads(content)
        deps = {}
        deps.update(data.get("dependencies", {}))
        deps.update(data.get("devDependencies", {}))
        return deps

    @staticmethod
    async def parse_go_mod(content: str) -> Dict[str, str]:
        deps = {}
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("require ("):
                continue
            match = re.match(r"\t([a-zA-Z0-9/-]+) (v[0-9.]+)", line)
            if match:
                deps[match.group(1)] = match.group(2)
        return deps

    @staticmethod
    async def parse_cargo_lock(content: str) -> Dict[str, str]:
        deps = {}
        in_deps = False
        for line in content.splitlines():
            if "[dependencies]" in line:
                in_deps = True
            elif line.startswith("["):
                in_deps = False
            elif in_deps:
                match = re.match(r'(\w+) = \{ version = "([^"]+)"', line)
                if match:
                    deps[match.group(1)] = match.group(2)
        return deps

    @staticmethod
    async def resolve(lockfile_content: str, file_type: str) -> Dict[str, str]:
        parsers = {
            "txt": DependencyResolver.parse_requirements,
            "pyproject": DependencyResolver.parse_requirements,
            "json": DependencyResolver.parse_package_json,
            "mod": DependencyResolver.parse_go_mod,
            "lock": DependencyResolver.parse_cargo_lock,
        }
        parser = parsers.get(file_type)
        if parser:
            return await parser(lockfile_content)
        return {}


class RuntimeDistributor:
    def __init__(
        self,
        cache_dir: str = "/tmp/runtime-cache",
        hot_cache_size: int = 10,
        cdn_base: str = "https://cdn.mem0.ai/runtimes",
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.hot_cache_size = hot_cache_size
        self.cdn_base = cdn_base
        self._hot_cache: Dict[str, RuntimeLayer] = {}
        self._packs: Dict[str, RuntimePack] = {}
        self._init_default_packs()

    def _init_default_packs(self):
        python_data = RuntimePack(
            id=str(uuid4()),
            name="python-data",
            version="3.11",
            runtime_type="python",
            layers=[
                RuntimeLayer(
                    id="os",
                    layer_type="os",
                    digest="sha256:os-base",
                    size_bytes=100_000_000,
                    compressed_size=50_000_000,
                    parent_digest=None,
                    contents=["/usr", "/lib"],
                    cdn_url=f"{self.cdn_base}/layers/os-base.tar.lz4",
                ),
                RuntimeLayer(
                    id="runtime",
                    layer_type="runtime",
                    digest="sha256:python-3.11",
                    size_bytes=80_000_000,
                    compressed_size=40_000_000,
                    parent_digest="sha256:os-base",
                    contents=["/usr/bin/python"],
                    cdn_url=f"{self.cdn_base}/layers/python-3.11.tar.lz4",
                ),
            ],
            total_size=180_000_000,
            compressed_size=90_000_000,
            hot_layers=["os", "runtime"],
            cdn_url=f"{self.cdn_base}/packs/python-data@3.11.tar.lz4",
            description="Python 3.11 with standard library",
        )

        python_ml = RuntimePack(
            id=str(uuid4()),
            name="python-ml",
            version="3.11",
            runtime_type="python",
            layers=[
                RuntimeLayer(
                    id="os",
                    layer_type="os",
                    digest="sha256:os-base",
                    size_bytes=100_000_000,
                    compressed_size=50_000_000,
                    parent_digest=None,
                    contents=["/usr", "/lib"],
                    cdn_url=f"{self.cdn_base}/layers/os-base.tar.lz4",
                ),
                RuntimeLayer(
                    id="runtime",
                    layer_type="runtime",
                    digest="sha256:python-3.11-ml",
                    size_bytes=150_000_000,
                    compressed_size=80_000_000,
                    parent_digest="sha256:os-base",
                    contents=["/usr/bin/python"],
                    cdn_url=f"{self.cdn_base}/layers/python-3.11-ml.tar.lz4",
                ),
                RuntimeLayer(
                    id="deps",
                    layer_type="deps",
                    digest="sha256:numpy-pandas",
                    size_bytes=200_000_000,
                    compressed_size=100_000_000,
                    parent_digest="sha256:python-3.11-ml",
                    contents=["/usr/lib/python/site-packages/numpy", "/usr/lib/python/site-packages/pandas"],
                    cdn_url=f"{self.cdn_base}/layers/numpy-pandas.tar.lz4",
                ),
            ],
            total_size=450_000_000,
            compressed_size=230_000_000,
            hot_layers=["os", "runtime"],
            cdn_url=f"{self.cdn_base}/packs/python-ml@3.11.tar.lz4",
            description="Python 3.11 with NumPy, Pandas, SciPy",
        )

        node_web = RuntimePack(
            id=str(uuid4()),
            name="node-web",
            version="20",
            runtime_type="node",
            layers=[
                RuntimeLayer(
                    id="os",
                    layer_type="os",
                    digest="sha256:os-base",
                    size_bytes=100_000_000,
                    compressed_size=50_000_000,
                    parent_digest=None,
                    contents=["/usr", "/lib"],
                    cdn_url=f"{self.cdn_base}/layers/os-base.tar.lz4",
                ),
                RuntimeLayer(
                    id="runtime",
                    layer_type="runtime",
                    digest="sha256:node-20",
                    size_bytes=60_000_000,
                    compressed_size=30_000_000,
                    parent_digest="sha256:os-base",
                    contents=["/usr/bin/node"],
                    cdn_url=f"{self.cdn_base}/layers/node-20.tar.lz4",
                ),
            ],
            total_size=160_000_000,
            compressed_size=80_000_000,
            hot_layers=["os", "runtime"],
            cdn_url=f"{self.cdn_base}/packs/node-web@20.tar.lz4",
            description="Node.js 20 with npm",
        )

        self._packs["python-data@3.11"] = python_data
        self._packs["python-ml@3.11"] = python_ml
        self._packs["node-web@20"] = node_web

    async def warm_runtime(self, name: str, version: str) -> bool:
        key = f"{name}@{version}"
        if key not in self._packs:
            logger.warning(f"Unknown runtime: {key}")
            return False
        pack = self._packs[key]
        for layer in pack.layers:
            if layer.is_hot:
                self._hot_cache[layer.id] = layer
        logger.info(f"Warmed runtime: {key}")
        return True

    async def get_runtime(self, name: str, version: str) -> Optional[RuntimePack]:
        return self._packs.get(f"{name}@{version}")

    async def list_available_runtimes(self) -> List[Dict]:
        return [
            {
                "name": pack.name,
                "version": pack.version,
                "full_id": pack.full_id,
                "runtime_type": pack.runtime_type,
                "layer_count": len(pack.layers),
                "total_size": pack.total_size,
                "compressed_size": pack.compressed_size,
                "cdn_url": pack.cdn_url,
                "description": pack.description,
            }
            for pack in self._packs.values()
        ]

    async def download_from_cdn(self, pack: RuntimePack) -> bool:
        if pack.cdn_url:
            logger.info(f"Would download from CDN: {pack.cdn_url}")
            return True
        return False

    async def resolve_dependencies(self, lockfile_content: str, file_type: str) -> Dict[str, str]:
        return await DependencyResolver.resolve(lockfile_content, file_type)

    async def get_storage_stats(self) -> Dict:
        return {
            "hot_layers": len(self._hot_cache),
            "packs_registered": len(self._packs),
            "total_uncompressed_bytes": sum(p.total_size for p in self._packs.values()),
            "cache_hit_rate": 0.0,
        }

    def estimate_cold_start_ms(self, pack: RuntimePack) -> float:
        base_ms = 500
        per_layer_ms = 100
        per_mb_ms = 5
        return base_ms + (len(pack.layers) * per_layer_ms) + ((pack.compressed_size / 1_000_000) * per_mb_ms)

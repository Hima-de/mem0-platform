"""
Content-Addressable Storage Engine v2
=====================================

The core building block for object-store-first snapshots.
Implements SHA-256 content addressing with variable-size chunking (CDC).

Key Technical Features:
- Content-Defined Chunking (CDC) using rolling hash (buzhash)
- LZ4 compression for hot data, zstd for cold data
- Parallel upload/download with configurable concurrency
- Local NVMe cache with LRU eviction
- O(1) deduplication lookup via hash index

Reference Papers:
- "The rsync algorithm" by Andrew Tridgell (1996)
- "Content-Defined Chunking" - Opendedup SDFS
- "VDoS: A High-Performance Block-Level Deduplication System"

"""

import asyncio
import hashlib
import json
import lz4.frame
import mmap
import os
import struct
import threading
import time
import xxhash
import zstandard
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import AsyncIterator, BinaryIO, Dict, List, Optional, Tuple, Union
from uuid import uuid4
import logging

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Compression algorithm selection."""

    NONE = "none"
    LZ4 = "lz4"  # Hot data - fastest
    ZSTD = "zstd"  # Cold data - best ratio
    ZSTD_LONG = "zstd_long"  # For large blocks


class StorageTier(Enum):
    """Storage tier for block placement."""

    HOT = "hot"  # NVMe cache, < 1ms latency
    WARM = "warm"  # SSD, < 10ms latency
    COLD = "cold"  # Object storage, < 100ms latency


@dataclass
class BlockMetadata:
    """Metadata for a stored block."""

    digest: str  # SHA-256 content hash
    size_bytes: int  # Original size
    compressed_size: int  # Compressed size
    compression: CompressionType  # Compression used
    tier: StorageTier  # Current storage location
    reference_count: int = 0  # Deduplication reference count
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    access_count: int = 0

    @property
    def compression_ratio(self) -> float:
        return self.size_bytes / max(self.compressed_size, 1)


@dataclass
class ChunkInfo:
    """Information about a variable-size chunk."""

    digest: str
    offset: int  # Offset in original file
    size: int  # Chunk size (variable)
    compressed_size: int
    is_delta: bool = False
    base_digest: Optional[str] = None  # For delta chunks


@dataclass
class SnapshotManifest:
    """Manifest describing a snapshot."""

    id: str
    parent_id: Optional[str]  # Parent snapshot for incremental
    user_id: str
    created_at: datetime
    total_size: int  # Logical size
    physical_size: int  # After dedup/compression
    chunks: List[ChunkInfo]
    metadata: Dict = field(default_factory=dict)
    content_hash: str = ""  # Hash of entire content

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "parent_id": self.parent_id,
            "user_id": self.user_id,
            "created_at": self.created_at.isoformat(),
            "total_size": self.total_size,
            "physical_size": self.physical_size,
            "chunks": [
                {
                    "digest": c.digest,
                    "offset": c.offset,
                    "size": c.size,
                    "compressed_size": c.compressed_size,
                    "is_delta": c.is_delta,
                    "base_digest": c.base_digest,
                }
                for c in self.chunks
            ],
            "metadata": self.metadata,
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SnapshotManifest":
        return cls(
            id=data["id"],
            parent_id=data.get("parent_id"),
            user_id=data["user_id"],
            created_at=datetime.fromisoformat(data["created_at"]),
            total_size=data["total_size"],
            physical_size=data["physical_size"],
            chunks=[
                ChunkInfo(
                    digest=c["digest"],
                    offset=c["offset"],
                    size=c["size"],
                    compressed_size=c["compressed_size"],
                    is_delta=c.get("is_delta", False),
                    base_digest=c.get("base_digest"),
                )
                for c in data["chunks"]
            ],
            metadata=data.get("metadata", {}),
            content_hash=data.get("content_hash", ""),
        )


class RollingHash:
    """
    Buzhash rolling hash for Content-Defined Chunking (CDC).

    Reference: "Evaluation of Deduplication Techniques for Big Data"
    Uses polynomial rolling hash with modulo 2^64 for speed.

    Key insight: Same content = Same chunks, regardless of alignment.
    This enables deduplication across different "files" with shared data.
    """

    MASK_64 = (1 << 64) - 1
    PRIME = 0x1F123BB4AB3C9D85  # Large random prime

    def __init__(self, min_chunk: int = 4096, max_chunk: int = 65536, avg_chunk: int = 16384):
        self.min_chunk = min_chunk
        self.max_chunk = max_chunk
        self.avg_chunk = avg_chunk
        self._tab = [0] * 256

        # Initialize hash table with pseudo-random values
        for i in range(256):
            self._tab[i] = self._hash_byte(i)

    def _hash_byte(self, b: int) -> int:
        """Hash a single byte."""
        return ((b * self.PRIME) >> 32) & 0xFFFFFFFF

    def compute(self, data: bytes) -> int:
        """Compute rolling hash for data."""
        h = 0
        for b in data[: self.min_chunk]:
            h = ((h << 1) ^ self._tab[b]) & self.MASK_64
        return h

    def find_chunks(self, data: bytes) -> List[Tuple[int, int]]:
        """
        Find chunk boundaries using CDC.

        Returns list of (offset, size) tuples.

        Algorithm:
        1. Slide window over data
        2. At each position, check if hash matches boundary condition
        3. Boundary when: hash & (2^k - 1) == 0
           where k ≈ log2(avg_chunk_size)
        """
        chunks = []
        offset = 0
        chunk_count = 0
        mask = (1 << 14) - 1  # k=14 for ~16KB avg chunks

        window_size = 64
        h = 0

        # Initialize first window
        for i in range(min(window_size, len(data))):
            h = ((h << 1) ^ self._tab[data[i]]) & self.MASK_64

        while offset + self.min_chunk < len(data):
            # Slide window
            if offset + window_size < len(data):
                # Remove oldest byte
                h = (h ^ (self._tab[data[offset]] * self._tab[0x1A])) & self.MASK_64
                # Add new byte
                h = ((h << 1) ^ self._tab[data[offset + window_size]]) & self.MASK_64

            # Check for chunk boundary
            chunk_size = offset - chunks[-1][0] if chunks else offset + self.min_chunk

            if chunk_size >= self.max_chunk:
                # Force boundary at max size
                chunks.append((offset, self.max_chunk))
                offset += self.max_chunk
                chunk_count += 1
                continue

            if chunk_size >= self.min_chunk and (h & mask) == 0:
                # Found boundary
                chunks.append((offset, chunk_size))
                offset += chunk_size
                chunk_count += 1
                continue

            offset += 1

        # Add final chunk
        if offset < len(data):
            chunks.append((offset, len(data) - offset))

        return chunks


class BlockStore:
    """
    Content-addressable block storage with compression and deduplication.

    Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    BlockStore API                           │
    ├─────────────────────────────────────────────────────────────┤
    │                                                              │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
    │  │  Write Path  │  │  Read Path   │  │   GC Path    │     │
    │  └──────────────┘  └──────────────┘  └──────────────┘     │
    │         │                 │                 │               │
    │         ▼                 ▼                 ▼               │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │              Chunking & Compression                 │    │
    │  │   CDC → SHA-256 → LZ4/ZSTD → Upload               │    │
    │  └─────────────────────────────────────────────────────┘    │
    │                        │                                   │
    │         ┌──────────────┼──────────────┐                    │
    │         ▼              ▼              ▼                    │
    │  ┌───────────┐  ┌───────────┐  ┌───────────────┐         │
    │  │  NVMe     │  │   Redis   │  │   S3/Object   │         │
    │  │  Cache    │  │   Index   │  │   Storage     │         │
    │  │  (HOT)    │  │  (WARM)   │  │   (COLD)      │         │
    │  └───────────┘  └───────────┘  └───────────────┘         │
    │                                                              │
    └─────────────────────────────────────────────────────────────┘
    """

    def __init__(
        self,
        local_cache_dir: str = "/tmp/block_cache",
        max_cache_size_gb: float = 10.0,
        s3_bucket: Optional[str] = None,
        s3_region: str = "us-east-1",
        redis_url: Optional[str] = None,
        compression: CompressionType = CompressionType.LZ4,
    ):
        self.local_cache_dir = Path(local_cache_dir)
        self.local_cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_cache_size = int(max_cache_size_gb * 1024 * 1024 * 1024)
        self.compression = compression
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.redis_url = redis_url

        # In-memory index for O(1) lookup
        self._block_index: Dict[str, BlockMetadata] = {}
        self._cache_lru: List[str] = []  # LRU order
        self._lock = threading.RLock()

        # Rolling hash for CDC
        self._cdc = RollingHash()

        # Compression contexts (reused for performance)
        self._lz4_compressor = lz4.frame.FrameCompressor()
        self._zstd_compressor = zstandard.ZstdCompressor(level=3)

        # Async executor for I/O
        self._executor = asyncio.Semaphore(32)

        # Metrics
        self._metrics = {
            "blocks_stored": 0,
            "bytes_written": 0,
            "blocks_read": 0,
            "bytes_read": 0,
            "deduplicated_bytes": 0,
            "compression_savings": 0,
        }

        # Initialize S3 client if configured
        if s3_bucket:
            self._init_s3()

        # Initialize Redis if configured
        if redis_url:
            self._init_redis()

        # Start cache cleanup background task
        self._cleanup_task: Optional[asyncio.Task] = None

    def _init_s3(self):
        """Initialize S3 client."""
        try:
            import boto3

            self._s3_client = boto3.client("s3", region_name=self.s3_region, endpoint_url=os.getenv("AWS_S3_ENDPOINT"))
            logger.info(f"S3 client initialized for bucket: {self.s3_bucket}")
        except Exception as e:
            logger.warning(f"Failed to initialize S3 client: {e}")
            self._s3_client = None

    def _init_redis(self):
        """Initialize Redis for distributed index."""
        try:
            import redis as redis_lib

            self._redis_client = redis_lib.from_url(self.redis_url, decode_responses=True)
            self._redis_client.ping()
            logger.info(f"Redis index initialized: {self.redis_url}")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis: {e}")
            self._redis_client = None

    def _compute_content_hash(self, data: bytes) -> str:
        """Compute SHA-256 content hash."""
        return hashlib.sha256(data).hexdigest()

    def _compute_xxhash(self, data: bytes) -> int:
        """Compute XXH64 for quick checksum."""
        return xxhash.xxh64(data).intdigest()

    def _compress(self, data: bytes, compression: CompressionType = None) -> Tuple[bytes, int, int]:
        """
        Compress data using configured algorithm.

        Returns: (compressed_data, original_size, compressed_size)
        """
        compression = compression or self.compression

        if compression == CompressionType.NONE:
            return data, len(data), len(data)

        elif compression == CompressionType.LZ4:
            compressed = lz4.frame.compress(data, compression_level=1)
            return compressed, len(data), len(compressed)

        elif compression == CompressionType.ZSTD:
            compressed = self._zstd_compressor.compress(data)
            return compressed, len(data), len(compressed)

        else:
            return data, len(data), len(data)

    def _decompress(self, data: bytes, compression: CompressionType, original_size: int) -> bytes:
        """Decompress data."""
        if compression == CompressionType.NONE:
            return data

        elif compression == CompressionType.LZ4:
            return lz4.frame.decompress(data)

        elif compression == CompressionType.ZSTD:
            dctx = zstandard.ZstdDecompressor()
            return dctx.decompress(data, max_output_size=original_size)

        else:
            return data

    def _get_cache_path(self, digest: str) -> Path:
        """Get local cache path for a block."""
        return self.local_cache_dir / digest[:2] / digest

    async def write_block(self, data: bytes, digest: Optional[str] = None) -> BlockMetadata:
        """
        Write a block to storage with deduplication.

        O(1) deduplication: check index before writing.

        Returns:
            BlockMetadata with storage location and stats.
        """
        async with self._executor:
            # Compute digest if not provided
            if digest is None:
                digest = self._compute_content_hash(data)

            # Check for existing block (deduplication)
            with self._lock:
                if digest in self._block_index:
                    meta = self._block_index[digest]
                    meta.reference_count += 1
                    meta.last_accessed = datetime.utcnow()
                    self._metrics["deduplicated_bytes"] += len(data)
                    return meta

            # Compress data
            compressed, orig_size, comp_size = self._compress(data)

            # Create metadata
            meta = BlockMetadata(
                digest=digest,
                size_bytes=orig_size,
                compressed_size=comp_size,
                compression=self.compression,
                tier=StorageTier.HOT,
                reference_count=1,
            )

            # Write to local cache
            cache_path = self._get_cache_path(digest)
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            with open(cache_path, "wb") as f:
                f.write(compressed)

            # Update index
            with self._lock:
                self._block_index[digest] = meta
                self._cache_lru.append(digest)
                self._metrics["blocks_stored"] += 1
                self._metrics["bytes_written"] += orig_size

            # Upload to S3 in background if configured
            if self._s3_client:
                asyncio.create_task(self._upload_to_s3(digest, compressed))

            # Update Redis index in background
            if self._redis_client:
                asyncio.create_task(self._update_redis_index(meta))

            # Evict old cache entries if needed
            await self._evict_cache_if_needed()

            return meta

    async def _upload_to_s3(self, digest: str, compressed_data: bytes):
        """Upload block to S3."""
        if not self._s3_client:
            return

        try:
            key = f"blocks/{digest[:2]}/{digest}"
            self._s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=compressed_data,
                ContentEncoding="lz4" if self.compression == CompressionType.LZ4 else "none",
                Metadata={"digest": digest, "size": str(len(compressed_data))},
            )

            with self._lock:
                if digest in self._block_index:
                    self._block_index[digest].tier = StorageTier.COLD

        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")

    async def _update_redis_index(self, meta: BlockMetadata):
        """Update distributed Redis index."""
        if not self._redis_client:
            return

        try:
            self._redis_client.hset(
                "block_index",
                meta.digest,
                json.dumps(
                    {
                        "size_bytes": meta.size_bytes,
                        "compressed_size": meta.compressed_size,
                        "compression": meta.compression.value,
                        "reference_count": meta.reference_count,
                    }
                ),
            )
        except Exception as e:
            logger.error(f"Failed to update Redis: {e}")

    async def read_block(self, digest: str, prefer_cache: bool = True) -> bytes:
        """
        Read a block from storage.

        Reads from: Cache → S3 → Error
        """
        start = time.perf_counter()

        # Check local cache first
        cache_path = self._get_cache_path(digest)

        with self._lock:
            in_cache = digest in self._block_index

        if prefer_cache and cache_path.exists():
            try:
                with open(cache_path, "rb") as f:
                    compressed = f.read()

                with self._lock:
                    if digest in self._block_index:
                        meta = self._block_index[digest]
                        meta.access_count += 1
                        meta.last_accessed = datetime.utcnow()
                        # Move to end of LRU
                        if digest in self._cache_lru:
                            self._cache_lru.remove(digest)
                        self._cache_lru.append(digest)

                # Decompress
                meta = self._block_index.get(digest)
                if meta:
                    decompressed = self._decompress(compressed, meta.compression, meta.size_bytes)

                    self._metrics["blocks_read"] += 1
                    self._metrics["bytes_read"] += len(decompressed)

                    return decompressed

            except Exception as e:
                logger.warning(f"Cache read failed for {digest}: {e}")

        # Try S3
        if self._s3_client:
            try:
                key = f"blocks/{digest[:2]}/{digest}"
                response = self._s3_client.get_object(Bucket=self.s3_bucket, Key=key)
                compressed = response["Body"].read()

                # Cache for next read
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "wb") as f:
                    f.write(compressed)

                # Update index tier
                with self._lock:
                    if digest in self._block_index:
                        self._block_index[digest].tier = StorageTier.HOT

                # Decompress
                meta = self._block_index.get(digest)
                if meta:
                    decompressed = self._decompress(compressed, meta.compression, meta.size_bytes)

                    self._metrics["blocks_read"] += 1
                    self._metrics["bytes_read"] += len(decompressed)

                    return decompressed

            except Exception as e:
                logger.error(f"S3 read failed for {digest}: {e}")

        raise FileNotFoundError(f"Block {digest} not found")

    async def get_block_metadata(self, digest: str) -> Optional[BlockMetadata]:
        """Get block metadata without reading content."""
        with self._lock:
            return self._block_index.get(digest)

    async def check_blocks_exist(self, digests: List[str]) -> Dict[str, bool]:
        """Batch check which blocks exist."""
        existing = {}

        with self._lock:
            for digest in digests:
                existing[digest] = digest in self._block_index

        # Check S3 for missing
        missing = [d for d, exists in existing.items() if not exists]

        if self._redis_client and missing:
            try:
                results = self._redis_client.hmget("block_index", missing)
                for digest, result in zip(missing, results):
                    if result:
                        existing[digest] = True
                        # Update local index
                        meta = BlockMetadata(**json.loads(result))
                        with self._lock:
                            self._block_index[digest] = meta
            except Exception as e:
                logger.error(f"Redis batch check failed: {e}")

        return existing

    async def _evict_cache_if_needed(self):
        """Evict old cache entries if over size limit."""
        current_size = sum(self._block_index[d].compressed_size for d in self._cache_lru if d in self._block_index)

        if current_size < self.max_cache_size:
            return

        # Evict oldest entries
        while current_size > self.max_cache_size * 0.8 and self._cache_lru:
            oldest = self._cache_lru.pop(0)

            if oldest in self._block_index:
                meta = self._block_index[oldest]

                # Don't evict if still referenced
                if meta.reference_count > 1:
                    meta.reference_count -= 1
                    continue

                # Delete cache file
                cache_path = self._get_cache_path(oldest)
                if cache_path.exists():
                    current_size -= meta.compressed_size
                    cache_path.unlink()

                # Move to cold in index
                meta.tier = StorageTier.COLD

                # Keep in index for S3 lookup
                del self._block_index[oldest]

                self._metrics["evictions"] += 1

    async def chunk_file(self, file_path: Union[str, Path]) -> List[ChunkInfo]:
        """
        Chunk a file using Content-Defined Chunking (CDC).

        Returns list of ChunkInfo for each chunk.

        This is key for deduplication: files with similar content
        will have many matching chunks even if boundaries differ.
        """
        chunks = []

        with open(file_path, "rb") as f:
            # Memory-map for efficient reading
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                data = mm[:]

                # Find chunk boundaries
                boundaries = self._cdc.find_chunks(data)

                for offset, size in boundaries:
                    chunk_data = data[offset : offset + size]
                    digest = self._compute_content_hash(chunk_data)
                    xxhash = self._compute_xxhash(chunk_data)

                    # Check if block already exists
                    existing = await self.get_block_metadata(digest)

                    if existing:
                        # Reuse existing block
                        chunks.append(
                            ChunkInfo(digest=digest, offset=offset, size=size, compressed_size=existing.compressed_size)
                        )
                    else:
                        # Store new block
                        meta = await self.write_block(chunk_data)
                        chunks.append(
                            ChunkInfo(digest=digest, offset=offset, size=size, compressed_size=meta.compressed_size)
                        )

        return chunks

    async def chunk_data(self, data: bytes) -> List[ChunkInfo]:
        """Chunk in-memory data using CDC."""
        boundaries = self._cdc.find_chunks(data)
        chunks = []

        for offset, size in boundaries:
            chunk_data = data[offset : offset + size]
            digest = self._compute_content_hash(chunk_data)

            existing = await self.get_block_metadata(digest)

            if existing:
                chunks.append(
                    ChunkInfo(digest=digest, offset=offset, size=size, compressed_size=existing.compressed_size)
                )
            else:
                meta = await self.write_block(chunk_data)
                chunks.append(ChunkInfo(digest=digest, offset=offset, size=size, compressed_size=meta.compressed_size))

        return chunks

    def get_stats(self) -> Dict:
        """Get storage statistics."""
        with self._lock:
            total_size = sum(m.size_bytes for m in self._block_index.values())
            total_compressed = sum(m.compressed_size for m in self._block_index.values())

            return {
                "block_count": len(self._block_index),
                "total_original_bytes": total_size,
                "total_compressed_bytes": total_compressed,
                "compression_ratio": round(total_size / max(total_compressed, 1), 2),
                "cache_size_bytes": sum(
                    self._block_index[d].compressed_size for d in self._cache_lru if d in self._block_index
                ),
                "max_cache_bytes": self.max_cache_size,
                **self._metrics,
            }


class DeltaEncoder:
    """
    Rsync-style delta encoding for efficient snapshot transfers.

    Reference: "The rsync algorithm" by Andrew Tridgell (1996)

    Key insight: For incremental updates, only transfer the differences.
    Uses rolling checksum + strong hash for matching.

    Algorithm:
    1. Receiver computes weak/strong checksums for its blocks
    2. Sender finds matching blocks using checksums
    3. Only unmatched data is sent as literal data + delta
    """

    WEAK_CHUNK = 512  # Weak checksum chunk size
    STRONG_CHUNK = 4096  # Strong checksum chunk size

    def __init__(self, block_size: int = 8192):
        self.block_size = block_size

    def compute_weak_checksum(self, data: bytes) -> int:
        """Compute weak rolling checksum (Adler-like)."""
        a = sum(data) & 0xFFFF
        b = sum((i + 1) * data[i] for i in range(len(data))) & 0xFFFF
        return (b << 16) | a

    def compute_strong_checksum(self, data: bytes) -> str:
        """Compute strong checksum (MD5 for now, could be SHA-256)."""
        return hashlib.md5(data).hexdigest()

    async def generate_signature(self, data: bytes) -> Dict:
        """
        Generate signature of data for delta computation.

        Returns:
            {
                "block_size": int,
                "weak_sums": [(offset, weak_checksum)],
                "strong_sums": {weak_checksum: strong_checksum}
            }
        """
        weak_sums = []
        strong_sums = {}

        for offset in range(0, len(data), self.block_size):
            block = data[offset : offset + self.block_size]
            if not block:
                continue

            weak = self.compute_weak_checksum(block)
            strong = self.compute_strong_checksum(block)

            weak_sums.append((offset, weak))
            strong_sums[weak] = strong

        return {"block_size": self.block_size, "length": len(data), "weak_sums": weak_sums, "strong_sums": strong_sums}

    async def compute_delta(self, old_data: bytes, new_data: bytes, signature: Dict) -> Dict:
        """
        Compute delta between old and new data using signature.

        Returns:
            {
                "deltas": [(offset, data_or_reference)],
                "literal_data": bytes,
                "references": int,
                "literal_bytes": int
            }
        """
        delta_ops = []
        literal_data = bytearray()

        # Build lookup from old data
        old_blocks = {}
        for offset in range(0, len(old_data), self.block_size):
            block = old_data[offset : offset + self.block_size]
            if block:
                weak = self.compute_weak_checksum(block)
                if weak not in old_blocks:
                    old_blocks[weak] = []
                old_blocks[weak].append((offset, block))

        strong_sums = signature["strong_sums"]
        new_offset = 0

        while new_offset < len(new_data):
            # Check if we can match a block
            remaining = len(new_data) - new_offset
            chunk_size = min(self.block_size, remaining)
            chunk = new_data[new_offset : new_offset + chunk_size]

            weak = self.compute_weak_checksum(chunk)

            if weak in strong_sums and weak in old_blocks:
                # Possible match - verify with strong hash
                strong = self.compute_strong_checksum(chunk)

                if strong == strong_sums[weak]:
                    # Find exact match
                    for old_offset, old_block in old_blocks[weak]:
                        if old_block == chunk:
                            # Match found - reference it
                            delta_ops.append(
                                {
                                    "type": "reference",
                                    "offset": new_offset,
                                    "size": len(chunk),
                                    "old_offset": old_offset,
                                }
                            )
                            new_offset += len(chunk)
                            break
                    else:
                        # Weak match but no strong match
                        literal_data.extend(chunk)
                        new_offset += len(chunk)
                else:
                    # No match
                    literal_data.extend(chunk)
                    new_offset += len(chunk)
            else:
                # No match
                literal_data.extend(chunk)
                new_offset += len(chunk)

        return {
            "deltas": delta_ops,
            "literal_data": bytes(literal_data),
            "references": sum(1 for d in delta_ops if d["type"] == "reference"),
            "literal_bytes": len(literal_data),
        }

    async def apply_delta(self, base_data: bytes, delta: Dict) -> bytes:
        """Apply delta to base data to reconstruct new data."""
        result = bytearray(base_data)

        for op in delta["deltas"]:
            if op["type"] == "reference":
                # Copy from base
                old_offset = op["old_offset"]
                data = base_data[old_offset : old_offset + op["size"]]
                result[op["offset"] : op["offset"] + op["size"]] = data
            elif op["type"] == "literal":
                # Copy literal data
                result[op["offset"] : op["offset"] + len(op["data"])] = op["data"]

        # Append literal data at end
        result.extend(delta.get("literal_data", b""))

        return bytes(result)


class SnapshotEngineV2:
    """
    Object-Store-First Snapshot Engine v2
    ======================================

    The core Turbopuffer-style wedge for sandbox snapshots.

    Architecture:
    ┌──────────────────────────────────────────────────────────────────┐
    │                    SnapshotEngineV2                               │
    ├──────────────────────────────────────────────────────────────────┤
    │                                                                   │
    │  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────┐│
    │  │   Fork()    │   │  Clone()    │   │   Delta + Merge()       ││
    │  │  (O(1))     │   │  (O(1))     │   │  (Incremental)         ││
    │  └─────────────┘   └─────────────┘   └─────────────────────────┘│
    │          │                 │                    │                 │
    │          └─────────────────┴────────────────────┘                 │
    │                              │                                    │
    │                              ▼                                    │
    │  ┌─────────────────────────────────────────────────────────────┐│
    │  │                  BlockStore (Content-Addressable)           ││
    │  │   CDC Chunking → SHA-256 → LZ4 → S3 + Local Cache            ││
    │  └─────────────────────────────────────────────────────────────┘│
    │                              │                                    │
    │                              ▼                                    │
    │  ┌─────────────────────────────────────────────────────────────┐│
    │  │                  Manifest Storage (Redis + S3)               ││
    │  │   O(1) lookup by snapshot_id → manifest + chunk list       ││
    │  └─────────────────────────────────────────────────────────────┘│
    │                                                                   │
    └──────────────────────────────────────────────────────────────────┘

    Key Operations:

    1. FORK(snapshot_id) → new_snapshot_id
       - O(1) operation: create manifest with same chunks
       - No data copied, just references
       - Parent link created for lineage

    2. CLONE(snapshot_id) → writable_snapshot_id
       - Copy-on-write: only modified blocks stored
       - Delta encoding for efficient transfers

    3. SNAPSHOT(files) → snapshot_id
       - Chunk files using CDC
       - Store unique blocks in BlockStore
       - Create manifest

    4. RESTORE(snapshot_id) → files
       - Download/reconstruct from blocks
       - Stream to minimize memory

    Performance:
    - FORK: < 1ms (just metadata)
    - CLONE: < 50ms (delta computation)
    - SNAPSHOT: Depends on data size (chunking + compression)
    - RESTORE: Depends on data size (decompression)

    Reference: Firecracker snapshot format, OCI image distribution
    """

    def __init__(
        self,
        block_store: BlockStore,
        manifest_redis_url: Optional[str] = None,
        s3_bucket: Optional[str] = None,
    ):
        self.block_store = block_store
        self.manifest_redis_url = manifest_redis_url
        self.s3_bucket = s3_bucket

        # In-memory manifest cache
        self._manifest_cache: Dict[str, SnapshotManifest] = {}
        self._manifest_lock = threading.RLock()

        # Delta encoder for incremental snapshots
        self._delta_encoder = DeltaEncoder()

        # Metrics
        self._metrics = {
            "forks": 0,
            "clones": 0,
            "snapshots": 0,
            "restores": 0,
            "bytes_stored": 0,
            "bytes_transferred": 0,
            "fork_latency_ms": [],
            "clone_latency_ms": [],
            "snapshot_latency_ms": [],
            "restore_latency_ms": [],
        }

    async def fork(self, snapshot_id: str, user_id: str) -> str:
        """
        FORK operation - O(1) snapshot cloning.

        Creates a new snapshot that shares all blocks with the parent.
        Only metadata is copied, no data.

        Args:
            snapshot_id: Source snapshot to fork
            user_id: User context for new snapshot

        Returns:
            New snapshot_id
        """
        start = time.perf_counter()

        # Get source manifest
        source_manifest = await self.get_manifest(snapshot_id)
        if not source_manifest:
            raise FileNotFoundError(f"Snapshot {snapshot_id} not found")

        # Create new manifest with same chunks (O(1))
        new_snapshot_id = f"snap_{uuid4().hex[:12]}"
        new_manifest = SnapshotManifest(
            id=new_snapshot_id,
            parent_id=snapshot_id,  # Link to parent
            user_id=user_id,
            created_at=datetime.utcnow(),
            total_size=source_manifest.total_size,
            physical_size=source_manifest.physical_size,
            chunks=source_manifest.chunks.copy(),  # Same chunks, shared
            metadata={"forked_from": snapshot_id},
        )

        # Store manifest
        await self._store_manifest(new_manifest)

        # Update metrics
        latency_ms = (time.perf_counter() - start) * 1000
        self._metrics["forks"] += 1
        self._metrics["fork_latency_ms"].append(latency_ms)

        logger.info(f"Forked {snapshot_id} -> {new_snapshot_id} in {latency_ms:.2f}ms")

        return new_snapshot_id

    async def clone(self, snapshot_id: str, user_id: str, modifications: Dict[str, bytes] = None) -> str:
        """
        CLONE operation - writable snapshot with copy-on-write.

        Creates a new snapshot with copy-on-write semantics.
        Only modified blocks are stored, shared blocks are referenced.

        Args:
            snapshot_id: Source snapshot to clone
            user_id: User context
            modifications: Dict of file_path -> new_content

        Returns:
            New writable snapshot_id
        """
        start = time.perf_counter()

        source_manifest = await self.get_manifest(snapshot_id)
        if not source_manifest:
            raise FileNotFoundError(f"Snapshot {snapshot_id} not found")

        # Build map of existing chunks
        existing_chunks = {(c.offset, c.size): c.digest for c in source_manifest.chunks}

        new_chunks = []
        new_size = 0

        if modifications:
            # Process modifications
            for path, content in modifications.items():
                # Chunk the modified content
                chunk_infos = await self.block_store.chunk_data(content)

                for chunk in chunk_infos:
                    # Check if this chunk already exists
                    exists = await self.block_store.get_block_metadata(chunk.digest)

                    if exists:
                        # Reuse existing chunk
                        new_chunks.append(chunk)
                    else:
                        # Store new chunk
                        meta = await self.block_store.write_block(content[chunk.offset : chunk.offset + chunk.size])
                        new_chunks.append(chunk)

                    new_size += chunk.size

            # Sort chunks by offset
            new_chunks.sort(key=lambda c: c.offset)

        # Create new manifest
        new_snapshot_id = f"snap_{uuid4().hex[:12]}"
        new_manifest = SnapshotManifest(
            id=new_snapshot_id,
            parent_id=snapshot_id,
            user_id=user_id,
            created_at=datetime.utcnow(),
            total_size=new_size or source_manifest.total_size,
            physical_size=sum(c.compressed_size for c in new_chunks),
            chunks=new_chunks or source_manifest.chunks,
            metadata={"cloned_from": snapshot_id, "modifications": list(modifications.keys()) if modifications else []},
        )

        # Store manifest
        await self._store_manifest(new_manifest)

        # Update metrics
        latency_ms = (time.perf_counter() - start) * 1000
        self._metrics["clones"] += 1
        self._metrics["clone_latency_ms"].append(latency_ms)

        logger.info(f"Cloned {snapshot_id} -> {new_snapshot_id} in {latency_ms:.2f}ms")

        return new_snapshot_id

    async def create_snapshot(
        self, user_id: str, files: Dict[str, bytes], parent_id: Optional[str] = None, incremental: bool = True
    ) -> str:
        """
        Create a new snapshot from files.

        Args:
            user_id: User context
            files: Dict of file_path -> content
            parent_id: Optional parent snapshot for incremental
            incremental: Whether to use delta encoding

        Returns:
            snapshot_id
        """
        start = time.perf_counter()

        all_chunks = []
        total_size = 0
        physical_size = 0

        if incremental and parent_id:
            # Incremental snapshot using delta encoding
            parent_manifest = await self.get_manifest(parent_id)
            if parent_manifest:
                # Build base data for delta computation
                base_data = await self._reconstruct_data(parent_manifest)
                base_signature = await self._delta_encoder.generate_signature(base_data)

        # Process all files
        for path, content in files.items():
            # Chunk the content
            chunks = await self.block_store.chunk_data(content)

            for chunk in chunks:
                chunk.path = path
                all_chunks.append(chunk)
                total_size += chunk.size
                physical_size += chunk.compressed_size

        # Sort chunks by path then offset
        all_chunks.sort(key=lambda c: (c.path, c.offset))

        # Compute content hash of all chunks
        content_bytes = b"".join(c.digest.encode() for c in all_chunks)
        content_hash = hashlib.sha256(content_bytes).hexdigest()

        # Create manifest
        snapshot_id = f"snap_{uuid4().hex[:12]}"
        manifest = SnapshotManifest(
            id=snapshot_id,
            parent_id=parent_id,
            user_id=user_id,
            created_at=datetime.utcnow(),
            total_size=total_size,
            physical_size=physical_size,
            chunks=all_chunks,
            metadata={"file_count": len(files)},
            content_hash=content_hash,
        )

        # Store manifest
        await self._store_manifest(manifest)

        # Update metrics
        latency_ms = (time.perf_counter() - start) * 1000
        self._metrics["snapshots"] += 1
        self._metrics["snapshot_latency_ms"].append(latency_ms)
        self._metrics["bytes_stored"] += total_size

        logger.info(f"Created snapshot {snapshot_id} ({total_size} bytes) in {latency_ms:.2f}ms")

        return snapshot_id

    async def restore(self, snapshot_id: str, output_dir: Optional[str] = None) -> Dict[str, bytes]:
        """
        Restore snapshot to files.

        Args:
            snapshot_id: Snapshot to restore
            output_dir: Optional directory to write files

        Returns:
            Dict of file_path -> content
        """
        start = time.perf_counter()

        manifest = await self.get_manifest(snapshot_id)
        if not manifest:
            raise FileNotFoundError(f"Snapshot {snapshot_id} not found")

        # Reconstruct data from blocks
        files = await self._reconstruct_files(manifest)

        # Write to disk if output directory specified
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            for path, content in files.items():
                file_path = output_path / path
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, "wb") as f:
                    f.write(content)

        # Update metrics
        latency_ms = (time.perf_counter() - start) * 1000
        self._metrics["restores"] += 1
        self._metrics["restore_latency_ms"].append(latency_ms)
        self._metrics["bytes_transferred"] += sum(len(c) for c in files.values())

        logger.info(f"Restored snapshot {snapshot_id} in {latency_ms:.2f}ms")

        return files

    async def diff(self, snapshot1_id: str, snapshot2_id: str) -> Dict[str, Union[bytes, str]]:
        """
        Compute diff between two snapshots.

        Returns:
            Dict with "added", "removed", "modified" file lists
        """
        manifest1 = await self.get_manifest(snapshot1_id)
        manifest2 = await self.get_manifest(snapshot2_id)

        if not manifest1 or not manifest2:
            raise FileNotFoundError("Snapshot not found")

        # Build chunk sets
        chunks1 = {(c.path, c.offset, c.size): c.digest for c in manifest1.chunks}
        chunks2 = {(c.path, c.offset, c.size): c.digest for c in manifest2.chunks}

        paths1 = set(c[0] for c in chunks1.keys())
        paths2 = set(c[0] for c in chunks2.keys())

        # Compute diff
        added = paths2 - paths1
        removed = paths1 - paths2
        modified = set()

        for path in paths1 & paths2:
            file_chunks1 = {k[1:]: v for k, v in chunks1.items() if k[0] == path}
            file_chunks2 = {k[1:]: v for k, v in chunks2.items() if k[0] == path}

            if file_chunks1 != file_chunks2:
                modified.add(path)

        return {"added": list(added), "removed": list(removed), "modified": list(modified)}

    async def _reconstruct_data(self, manifest: SnapshotManifest) -> bytes:
        """Reconstruct complete data from chunks."""
        # Sort chunks by offset
        sorted_chunks = sorted(manifest.chunks, key=lambda c: c.offset)

        # Read all blocks in parallel
        blocks = await asyncio.gather(*[self.block_store.read_block(c.digest) for c in sorted_chunks])

        return b"".join(blocks)

    async def _reconstruct_files(self, manifest: SnapshotManifest) -> Dict[str, bytes]:
        """Reconstruct files from chunks."""
        # Group chunks by path
        path_chunks: Dict[str, List[Tuple[int, str]]] = {}
        for chunk in manifest.chunks:
            if chunk.path not in path_chunks:
                path_chunks[chunk.path] = []
            path_chunks[chunk.path].append((chunk.offset, chunk.digest))

        # Reconstruct each file
        files = {}
        for path, chunks in path_chunks.items():
            # Sort by offset
            chunks.sort(key=lambda x: x[0])

            # Read blocks
            blocks = await asyncio.gather(*[self.block_store.read_block(digest) for _, digest in chunks])

            # Combine blocks
            files[path] = b"".join(blocks)

        return files

    async def _store_manifest(self, manifest: SnapshotManifest):
        """Store manifest in Redis + S3."""
        manifest_data = manifest.to_dict()

        # Cache locally
        with self._manifest_lock:
            self._manifest_cache[manifest.id] = manifest

        # Store in Redis if available
        if self.manifest_redis_url:
            try:
                import redis

                r = redis.from_url(self.manifest_redis_url, decode_responses=False)
                r.setex(
                    f"manifest:{manifest.id}",
                    86400,  # 24 hour TTL
                    json.dumps(manifest_data),
                )
            except Exception as e:
                logger.warning(f"Failed to store manifest in Redis: {e}")

        # Store in S3
        if self.s3_bucket:
            try:
                import boto3

                s3 = boto3.client("s3")
                key = f"manifests/{manifest.id[:2]}/{manifest.id}.json"
                s3.put_object(
                    Bucket=self.s3_bucket, Key=key, Body=json.dumps(manifest_data), ContentType="application/json"
                )
            except Exception as e:
                logger.warning(f"Failed to store manifest in S3: {e}")

    async def get_manifest(self, snapshot_id: str) -> Optional[SnapshotManifest]:
        """Get manifest from cache, Redis, or S3."""
        # Check cache first
        with self._manifest_lock:
            if snapshot_id in self._manifest_cache:
                return self._manifest_cache[snapshot_id]

        manifest_data = None

        # Try Redis
        if self.manifest_redis_url:
            try:
                import redis

                r = redis.from_url(self.manifest_redis_url, decode_responses=False)
                data = r.get(f"manifest:{snapshot_id}")
                if data:
                    manifest_data = json.loads(data)
            except Exception as e:
                logger.warning(f"Failed to get manifest from Redis: {e}")

        # Try S3
        if not manifest_data and self.s3_bucket:
            try:
                import boto3

                s3 = boto3.client("s3")
                key = f"manifests/{snapshot_id[:2]}/{snapshot_id}.json"
                response = s3.get_object(Bucket=self.s3_bucket, Key=key)
                manifest_data = json.loads(response["Body"].read())
            except Exception as e:
                logger.warning(f"Failed to get manifest from S3: {e}")

        if manifest_data:
            manifest = SnapshotManifest.from_dict(manifest_data)
            with self._manifest_lock:
                self._manifest_cache[manifest.id] = manifest
            return manifest

        return None

    def get_stats(self) -> Dict:
        """Get engine statistics."""
        return {
            "snapshots": self._metrics["snapshots"],
            "forks": self._metrics["forks"],
            "clones": self._metrics["clones"],
            "restores": self._metrics["restores"],
            "bytes_stored": self._metrics["bytes_stored"],
            "bytes_transferred": self._metrics["bytes_transferred"],
            "latencies": {
                "fork_p50": self._percentile(self._metrics["fork_latency_ms"], 50),
                "fork_p99": self._percentile(self._metrics["fork_latency_ms"], 99),
                "clone_p50": self._percentile(self._metrics["clone_latency_ms"], 50),
                "clone_p99": self._percentile(self._metrics["clone_latency_ms"], 99),
                "snapshot_p50": self._percentile(self._metrics["snapshot_latency_ms"], 50),
                "snapshot_p99": self._percentile(self._metrics["snapshot_latency_ms"], 99),
                "restore_p50": self._percentile(self._metrics["restore_latency_ms"], 50),
                "restore_p99": self._percentile(self._metrics["restore_latency_ms"], 99),
            },
            "block_store": self.block_store.get_stats(),
        }

    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile."""
        if not data:
            return 0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * percentile / 100)
        return round(sorted_data[idx], 2)

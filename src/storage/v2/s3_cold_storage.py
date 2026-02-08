"""
S3 Cold Storage Repository for Mem0
====================================

Tiered storage implementation for long-term snapshot archival.
Provides cost-effective storage for infrequently accessed snapshots
with automatic lifecycle management.

Key Features:
- S3-compatible storage for snapshots and blocks
- Tiered storage classes (Standard, Infrequent Access, Glacier)
- Automatic migration based on access patterns
- Encryption at rest (AES-256, SSE-KMS)
- Versioning and retention policies
- Multi-part uploads for large files

Usage:
    from src.storage.v2.s3_cold_storage import S3ColdStorageConfig, S3ColdStorageRepository

    config = S3ColdStorageConfig(
        bucket="mem0-cold-storage",
        region="us-east-1",
        storage_class="STANDARD_IA",
        lifecycle_days=90,
        glacier_restore_days=365
    )

    storage = S3ColdStorageRepository(config)
    await storage.upload_snapshot(snapshot_id, manifest, blocks)
    await storage.download_snapshot(snapshot_id)

Architecture:
┌─────────────────────────────────────────────────────────────┐
│                   S3 Cold Storage Repository                │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Snapshot    │  │    Block    │  │  Manifest   │       │
│  │  Storage     │  │  Storage    │  │  Storage   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│         │                 │                 │                │
│         └─────────────────┼─────────────────┘                │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              S3 Storage Layer                           │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │ │
│  │  │Standard │  │INTELLIGENT│ │  S3 IA  │  │Glacier │   │ │
│  │  │         │  │ TIERING │  │         │  │         │   │ │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │ │
│  └─────────────────────────────────────────────────────────┘ │
│                           │                                  │
│         ┌─────────────────┼─────────────────┐               │
│         ▼                 ▼                 ▼               │
│  ┌───────────┐    ┌───────────┐    ┌───────────────┐       │
│  │ Lifecycle │    │ Encryption│    │    Versioning │       │
│  │  Policy   │    │   (SSE)   │    │    & Lock     │       │
│  └───────────┘    └───────────┘    └───────────────┘       │
│                                                              │
└─────────────────────────────────────────────────────────────┘

"""

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, Union
from uuid import uuid4

logger = logging.getLogger(__name__)


class S3StorageClass(str, Enum):
    """S3 storage classes for cost optimization."""

    STANDARD = "STANDARD"
    STANDARD_IA = "STANDARD_IA"
    ONEZONE_IA = "ONEZONE_IA"
    INTELLIGENT_TIERING = "INTELLIGENT_TIERING"
    GLACIER = "GLACIER"
    GLACIER_DEEP_ARCHIVE = "DEEP_ARCHIVE"
    GLACIER_IR = "GLACIER_IR"


class S3EndpointStyle(str, Enum):
    """S3 endpoint style options."""

    AWS_GLOBAL = "aws_global"  # s3.amazonaws.com
    AWS_REGION = "aws_region"  # s3.{region}.amazonaws.com
    MINIO = "minio"  # MinIO/S3 compatible
    CUSTOM = "custom"  # Custom endpoint URL


@dataclass
class S3ColdStorageConfig:
    """Configuration for S3 cold storage repository."""

    bucket: str
    region: str = "us-east-1"

    storage_class: str = S3StorageClass.STANDARD_IA.value
    glacier_restore_days: int = 1

    lifecycle_days_to_ia: int = 30
    lifecycle_days_to_glacier: int = 90
    lifecycle_days_to_delete: int = 365

    enable_versioning: bool = True
    enable_encryption: bool = True
    encryption_key_id: Optional[str] = None

    endpoint_url: Optional[str] = None
    endpoint_style: str = S3EndpointStyle.AWS_REGION.value
    force_path_style: bool = False

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None

    max_concurrent_uploads: int = 10
    max_concurrent_downloads: int = 10
    multipart_threshold_mb: int = 100
    multipart_chunk_mb: int = 50

    checksum_algorithm: str = "SHA256"

    def get_s3_client_kwargs(self) -> Dict[str, Any]:
        """Get kwargs for boto3 S3 client."""
        kwargs = {
            "region_name": self.region,
            "config": None,
        }

        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url

        if self.force_path_style:
            from botocore.config import Config

            kwargs["config"] = Config(s3={"addressing_style": "path"})

        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            kwargs["aws_session_token"] = self.aws_session_token

        return kwargs


@dataclass
class S3ObjectMetadata:
    """Metadata for an S3 object."""

    key: str
    size_bytes: int
    etag: str
    last_modified: datetime
    storage_class: str
    version_id: Optional[str] = None
    checksum: Optional[str] = None
    is_restored: bool = False
    restore_expiry: Optional[datetime] = None


@dataclass
class SnapshotArchive:
    """Archive of a snapshot with all its blocks."""

    snapshot_id: str
    manifest_key: str
    block_keys: List[str]
    total_size_bytes: int
    created_at: datetime
    metadata: Dict = field(default_factory=dict)
    checksum: str = ""


@dataclass
class ArchiveStats:
    """Statistics for archived snapshots."""

    total_snapshots: int = 0
    total_size_bytes: int = 0
    total_archives: int = 0
    storage_class_breakdown: Dict[str, int] = field(default_factory=dict)
    oldest_snapshot: Optional[datetime] = None
    newest_snapshot: Optional[datetime] = None


class S3ColdStorageRepository:
    """
    S3-based cold storage for Mem0 snapshots.

    Provides cost-effective archival storage with:
    - Automatic tiered storage
    - Glacier restore support
    - Lifecycle policies
    - Encryption and versioning

    Usage:
        config = S3ColdStorageConfig(
            bucket="mem0-cold-storage",
            storage_class=S3StorageClass.STANDARD_IA
        )
        storage = S3ColdStorageRepository(config)
        await storage.archive_snapshot(snapshot_id, manifest, blocks)
    """

    def __init__(self, config: S3ColdStorageConfig):
        self.config = config
        self._s3_client = None
        self._transfer_config = None
        self._initialized = False

        self._local_cache_dir = Path(f"/tmp/mem0_cold_storage_{uuid4().hex[:8]}")
        self._local_cache_dir.mkdir(parents=True, exist_ok=True)

        self._stats: Dict[str, Any] = {
            "uploads": 0,
            "downloads": 0,
            "restores": 0,
            "bytes_uploaded": 0,
            "bytes_downloaded": 0,
            "archive_operations": 0,
        }

    def _get_s3_client(self):
        """Lazy initialization of S3 client."""
        if self._s3_client is None:
            try:
                import boto3
                import boto3.s3.transfer
                from botocore.config import Config

                client_kwargs = self.config.get_s3_client_kwargs()

                self._s3_client = boto3.client(
                    "s3",
                    region_name=client_kwargs.pop("region_name", "us-east-1"),
                    endpoint_url=client_kwargs.pop("endpoint_url", None),
                    aws_access_key_id=client_kwargs.pop("aws_access_key_id", None),
                    aws_secret_access_key=client_kwargs.pop("aws_secret_access_key", None),
                    aws_session_token=client_kwargs.pop("aws_session_token", None),
                    config=Config(
                        max_pool_connections=50,
                        retries={"max_attempts": 3, "mode": "adaptive"},
                    ),
                )

                self._transfer_config = boto3.s3.transfer.TransferConfig(
                    multipart_threshold=self.config.multipart_threshold_mb * 1024 * 1024,
                    max_concurrency=self.config.max_concurrent_uploads,
                    multipart_chunksize=self.config.multipart_chunk_mb * 1024 * 1024,
                    use_threads=True,
                )

                logger.info(f"S3 client initialized for bucket: {self.config.bucket}")

            except ImportError:
                raise ImportError("boto3 is required for S3 cold storage. Install it with: pip install boto3")

        return self._s3_client

    async def initialize(self):
        """Initialize S3 client and verify bucket access."""
        if self._initialized:
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._get_s3_client)

        try:
            s3 = self._get_s3_client()
            await loop.run_in_executor(None, lambda: s3.head_bucket(Bucket=self.config.bucket))
            logger.info(f"Verified bucket access: {self.config.bucket}")

            await self._setup_lifecycle_policy()
            await self._setup_bucket_versioning()

        except Exception as e:
            logger.warning(f"Failed to initialize S3 bucket: {e}")
            raise

        self._initialized = True

    async def _setup_lifecycle_policy(self):
        """Configure S3 lifecycle policy for tiered storage."""
        s3 = self._get_s3_client()

        lifecycle_rule = {
            "ID": "Mem0ColdStorageLifecycle",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Transitions": [
                {"Days": self.config.lifecycle_days_to_ia, "StorageClass": "STANDARD_IA"},
                {"Days": self.config.lifecycle_days_to_glacier, "StorageClass": "GLACIER"},
            ],
            "NoncurrentVersionExpiration": {"NoncurrentDays": self.config.lifecycle_days_to_delete},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
        }

        try:
            existing = s3.get_bucket_lifecycle_configuration(Bucket=self.config.bucket)
            rules = existing.get("Rules", [])

            existing_ids = {r["ID"] for r in rules}
            if "Mem0ColdStorageLifecycle" not in existing_ids:
                rules.append(lifecycle_rule)
                s3.put_bucket_lifecycle_configuration(
                    Bucket=self.config.bucket, LifecycleConfiguration={"Rules": rules}
                )
                logger.info("Lifecycle policy configured")

        except Exception as e:
            if "NoSuchLifecycleConfiguration" in str(e):
                s3.put_bucket_lifecycle_configuration(
                    Bucket=self.config.bucket, LifecycleConfiguration={"Rules": [lifecycle_rule]}
                )
                logger.info("Lifecycle policy created")
            else:
                logger.warning(f"Failed to setup lifecycle policy: {e}")

    async def _setup_bucket_versioning(self):
        """Enable bucket versioning if configured."""
        if not self.config.enable_versioning:
            return

        try:
            s3 = self._get_s3_client()
            status = s3.get_bucket_versioning(Bucket=self.config.bucket)

            if status.get("Status") != "Enabled":
                s3.put_bucket_versioning(
                    Bucket=self.config.bucket, VersioningConfiguration={"Status": "Enabled", "MFADelete": "Disabled"}
                )
                logger.info("Bucket versioning enabled")

        except Exception as e:
            logger.warning(f"Failed to enable versioning: {e}")

    def _get_snapshot_prefix(self, snapshot_id: str) -> str:
        """Get S3 prefix for a snapshot."""
        return f"snapshots/{snapshot_id[:2]}/{snapshot_id}"

    def _get_block_key(self, digest: str) -> str:
        """Get S3 key for a block."""
        return f"blocks/{digest[:2]}/{digest}"

    def _compute_checksum(self, data: bytes) -> str:
        """Compute checksum for data."""
        if self.config.checksum_algorithm == "SHA256":
            return hashlib.sha256(data).hexdigest()
        elif self.config.checksum_algorithm == "SHA1":
            return hashlib.sha1(data).hexdigest()
        else:
            return hashlib.md5(data).hexdigest()

    async def upload_snapshot(
        self,
        snapshot_id: str,
        manifest_data: Dict[str, Any],
        blocks: Dict[str, bytes],
        metadata: Optional[Dict[str, str]] = None,
    ) -> SnapshotArchive:
        """
        Archive a snapshot to S3 cold storage.

        Args:
            snapshot_id: Unique snapshot identifier
            manifest_data: Snapshot manifest as dict
            blocks: Dict of block_digest -> content
            metadata: Optional custom metadata

        Returns:
            SnapshotArchive with storage details
        """
        await self.initialize()

        start_time = time.perf_counter()
        prefix = self._get_snapshot_prefix(snapshot_id)

        manifest_key = f"{prefix}/manifest.json"
        block_keys = []

        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        manifest_bytes = json.dumps(manifest_data, indent=2).encode()
        manifest_checksum = self._compute_checksum(manifest_bytes)

        extra_args = {
            "ContentType": "application/json",
            "Metadata": {
                "snapshot_id": snapshot_id,
                "checksum": manifest_checksum,
                "created_at": datetime.utcnow().isoformat(),
            },
        }

        if self.config.enable_encryption:
            if self.config.encryption_key_id:
                extra_args["SSEKMSKeyId"] = self.config.encryption_key_id
                extra_args["ServerSideEncryption"] = "aws:kms"
            else:
                extra_args["ServerSideEncryption"] = "AES256"

        await loop.run_in_executor(
            None,
            lambda: s3.put_object(
                Bucket=self.config.bucket,
                Key=manifest_key,
                Body=manifest_bytes,
                StorageClass=self.config.storage_class,
                **extra_args,
            ),
        )

        self._stats["uploads"] += 1
        self._stats["bytes_uploaded"] += len(manifest_bytes)

        total_size = len(manifest_bytes)
        tasks = []

        for digest, block_data in blocks.items():
            block_key = self._get_block_key(digest)
            block_keys.append(block_key)

            checksum = self._compute_checksum(block_data)
            block_extra_args = extra_args.copy()
            block_extra_args["Metadata"] = {
                "block_digest": digest,
                "checksum": checksum,
            }

            if len(block_data) >= self.config.multipart_threshold_mb * 1024 * 1024:
                tasks.append(self._upload_large_object(block_key, block_data, block_extra_args))
            else:
                tasks.append(
                    loop.run_in_executor(
                        None,
                        lambda bk=block_key, bd=block_data, bea=block_extra_args: s3.put_object(
                            Bucket=self.config.bucket, Key=bk, Body=bd, StorageClass=self.config.storage_class, **bea
                        ),
                    )
                )

            total_size += len(block_data)

        await asyncio.gather(*tasks)

        archive = SnapshotArchive(
            snapshot_id=snapshot_id,
            manifest_key=manifest_key,
            block_keys=block_keys,
            total_size_bytes=total_size,
            created_at=datetime.utcnow(),
            metadata=metadata or {},
            checksum=self._compute_checksum(manifest_bytes + b"".join(blocks.values())),
        )

        self._stats["archive_operations"] += 1
        self._stats["bytes_uploaded"] += total_size

        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.info(f"Archived snapshot {snapshot_id}: {total_size / 1024 / 1024:.2f}MB in {elapsed_ms:.0f}ms")

        return archive

    async def _upload_large_object(self, key: str, data: bytes, extra_args: Dict[str, Any]):
        """Upload large object using multipart upload."""
        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        mpu = await loop.run_in_executor(
            None, lambda: s3.create_multipart_upload(Bucket=self.config.bucket, Key=key, **extra_args)
        )

        mpu_id = mpu["UploadId"]
        parts = []

        chunk_size = self.config.multipart_chunk_mb * 1024 * 1024
        for i in range(0, len(data), chunk_size):
            part_data = data[i : i + chunk_size]
            part_num = len(parts) + 1

            etag = await loop.run_in_executor(
                None,
                lambda pd=part_data, pn=part_num: s3.upload_part(
                    Bucket=self.config.bucket,
                    Key=key,
                    PartNumber=pn,
                    UploadId=mpu_id,
                    Body=pd,
                )["ETag"],
            )

            parts.append({"PartNumber": part_num, "ETag": etag})

        await loop.run_in_executor(
            None,
            lambda: s3.complete_multipart_upload(
                Bucket=self.config.bucket, Key=key, UploadId=mpu_id, MultipartUpload={"Parts": parts}
            ),
        )

    async def download_snapshot(
        self, snapshot_id: str, output_dir: Optional[str] = None
    ) -> Tuple[Dict[str, Any], Dict[str, bytes]]:
        """
        Download a snapshot from S3 cold storage.

        Args:
            snapshot_id: Snapshot to download
            output_dir: Optional directory to write files

        Returns:
            Tuple of (manifest_data, blocks)
        """
        await self.initialize()

        start_time = time.perf_counter()
        prefix = self._get_snapshot_prefix(snapshot_id)
        manifest_key = f"{prefix}/manifest.json"

        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        manifest_response = await loop.run_in_executor(
            None, lambda: s3.get_object(Bucket=self.config.bucket, Key=manifest_key)
        )

        manifest_data = json.loads(manifest_response["Body"].read())

        blocks = {}
        block_prefix = f"{prefix}/blocks/"

        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=block_prefix):
            for obj in page.get("Contents", []):
                block_key = obj["Key"]

                if block_key.endswith("/"):
                    continue

                response = await loop.run_in_executor(
                    None, lambda bk=block_key: s3.get_object(Bucket=self.config.bucket, Key=bk)
                )

                blocks[block_key] = response["Body"].read()

        self._stats["downloads"] += 1
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.info(f"Downloaded snapshot {snapshot_id} in {elapsed_ms:.0f}ms")

        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            manifest_path = output_path / "manifest.json"
            with open(manifest_path, "w") as f:
                json.dump(manifest_data, f, indent=2)

            blocks_dir = output_path / "blocks"
            blocks_dir.mkdir(exist_ok=True)

            for key, data in blocks.items():
                block_path = blocks_dir / Path(key).name
                with open(block_path, "wb") as f:
                    f.write(data)

        return manifest_data, blocks

    async def restore_from_glacier(self, snapshot_id: str, tier: str = "Standard") -> bool:
        """
        Initiate Glacier restore for a snapshot.

        Args:
            snapshot_id: Snapshot to restore
            tier: Restore tier (Expedited, Standard, Bulk)

        Returns:
            True if restore initiated successfully
        """
        await self.initialize()

        prefix = self._get_snapshot_prefix(snapshot_id)
        manifest_key = f"{prefix}/manifest.json"

        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        tier_map = {
            "Expedited": {"Days": 1, "Tier": "Expedited"},
            "Standard": {"Days": 3, "Tier": "Standard"},
            "Bulk": {"Days": 5, "Tier": "Bulk"},
        }

        restore_params = tier_map.get(tier, tier_map["Standard"])

        try:
            await loop.run_in_executor(
                None,
                lambda: s3.restore_object(
                    Bucket=self.config.bucket,
                    Key=manifest_key,
                    RestoreRequest={
                        "Days": restore_params["Days"],
                        "GlacierJobParameters": {"Tier": restore_params["Tier"]},
                    },
                ),
            )

            self._stats["restores"] += 1
            logger.info(f"Glacier restore initiated for {snapshot_id} (Tier: {tier})")
            return True

        except Exception as e:
            logger.error(f"Failed to initiate restore: {e}")
            return False

    async def check_restore_status(self, snapshot_id: str) -> Optional[datetime]:
        """Check if a Glacier object is restored."""
        await self.initialize()

        prefix = self._get_snapshot_prefix(snapshot_id)
        manifest_key = f"{prefix}/manifest.json"

        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        try:
            response = await loop.run_in_executor(
                None, lambda: s3.head_object(Bucket=self.config.bucket, Key=manifest_key)
            )

            restore = response.get("Restore")
            if restore and 'ongoing-request="false"' in restore:
                expiry = response.get("RestoreExpiration")
                if expiry:
                    return datetime.fromisoformat(expiry.replace("Z", "+00:00"))

            return None

        except Exception:
            return None

    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot from S3 storage."""
        await self.initialize()

        prefix = self._get_snapshot_prefix(snapshot_id)
        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        try:
            objects_to_delete = []

            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if not objects_to_delete:
                return False

            while objects_to_delete:
                batch = objects_to_delete[:1000]
                await loop.run_in_executor(
                    None, lambda: s3.delete_objects(Bucket=self.config.bucket, Delete={"Objects": batch, "Quiet": True})
                )
                objects_to_delete = objects_to_delete[1000:]

            logger.info(f"Deleted snapshot {snapshot_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete snapshot: {e}")
            return False

    async def list_snapshots(self, prefix: str = "", max_keys: int = 1000) -> List[SnapshotArchive]:
        """List archived snapshots."""
        await self.initialize()

        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        list_prefix = prefix or "snapshots/"
        archives = []

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=list_prefix, MaxKeys=max_keys):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if key.endswith("/manifest.json"):
                    snapshot_id = key.split("/")[-2]
                    archive = SnapshotArchive(
                        snapshot_id=snapshot_id,
                        manifest_key=key,
                        block_keys=[],
                        total_size_bytes=obj["Size"],
                        created_at=obj["LastModified"],
                        metadata=obj.get("Metadata", {}),
                    )
                    archives.append(archive)

        return archives

    async def get_archive_stats(self) -> ArchiveStats:
        """Get statistics about archived snapshots."""
        archives = await self.list_snapshots(max_keys=5000)

        stats = ArchiveStats(
            total_snapshots=len(archives),
            total_archives=len(archives),
            oldest_snapshot=None,
            newest_snapshot=None,
        )

        for archive in archives:
            stats.total_size_bytes += archive.total_size_bytes

        if archives:
            sorted_archives = sorted(archives, key=lambda a: a.created_at)
            stats.oldest_snapshot = sorted_archives[0].created_at
            stats.newest_snapshot = sorted_archives[-1].created_at

        return stats

    async def get_object_metadata(self, snapshot_id: str) -> Optional[S3ObjectMetadata]:
        """Get metadata for a snapshot's manifest."""
        await self.initialize()

        prefix = self._get_snapshot_prefix(snapshot_id)
        manifest_key = f"{prefix}/manifest.json"

        s3 = self._get_s3_client()
        loop = asyncio.get_event_loop()

        try:
            response = await loop.run_in_executor(
                None, lambda: s3.head_object(Bucket=self.config.bucket, Key=manifest_key)
            )

            return S3ObjectMetadata(
                key=manifest_key,
                size_bytes=response["ContentLength"],
                etag=response["ETag"].strip('"'),
                last_modified=response["LastModified"],
                storage_class=response.get("StorageClass", "STANDARD"),
                version_id=response.get("VersionId"),
                checksum=response["Metadata"].get("checksum"),
            )

        except Exception:
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return {
            **self._stats,
            "initialized": self._initialized,
            "cache_dir": str(self._local_cache_dir),
        }


class S3StorageFactory:
    """Factory for creating S3 storage repositories based on configuration."""

    @staticmethod
    def create_from_env() -> S3ColdStorageRepository:
        """Create S3 storage from environment variables."""
        config = S3ColdStorageConfig(
            bucket=os.getenv("MEM0_S3_BUCKET", "mem0-cold-storage"),
            region=os.getenv("MEM0_S3_REGION", "us-east-1"),
            storage_class=os.getenv("MEM0_S3_STORAGE_CLASS", S3StorageClass.STANDARD_IA.value),
            endpoint_url=os.getenv("MEM0_S3_ENDPOINT"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )

        return S3ColdStorageRepository(config)

    @staticmethod
    def create(config: S3ColdStorageConfig) -> S3ColdStorageRepository:
        """Create S3 storage from configuration."""
        return S3ColdStorageRepository(config)


__all__ = [
    "S3ColdStorageConfig",
    "S3ColdStorageRepository",
    "S3StorageClass",
    "S3StorageFactory",
    "SnapshotArchive",
    "S3ObjectMetadata",
    "ArchiveStats",
]

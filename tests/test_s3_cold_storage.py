"""
Tests for S3 Cold Storage Repository
=====================================

Comprehensive tests for S3 cold storage functionality.
"""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.v2.s3_cold_storage import (
    S3ColdStorageConfig,
    S3ColdStorageRepository,
    S3StorageClass,
    S3StorageFactory,
    SnapshotArchive,
    S3ObjectMetadata,
    ArchiveStats,
)


class TestS3ColdStorageConfig(unittest.TestCase):
    """Tests for S3ColdStorageConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = S3ColdStorageConfig(bucket="test-bucket")

        self.assertEqual(config.bucket, "test-bucket")
        self.assertEqual(config.region, "us-east-1")
        self.assertEqual(config.storage_class, S3StorageClass.STANDARD_IA.value)
        self.assertEqual(config.lifecycle_days_to_ia, 30)
        self.assertEqual(config.lifecycle_days_to_glacier, 90)
        self.assertTrue(config.enable_versioning)
        self.assertTrue(config.enable_encryption)

    def test_custom_config(self):
        """Test custom configuration values."""
        config = S3ColdStorageConfig(
            bucket="my-bucket",
            region="eu-west-1",
            storage_class=S3StorageClass.GLACIER.value,
            lifecycle_days_to_ia=60,
            lifecycle_days_to_glacier=180,
            glacier_restore_days=3,
            endpoint_url="https://minio.local:9000",
            endpoint_style="minio",
        )

        self.assertEqual(config.region, "eu-west-1")
        self.assertEqual(config.storage_class, "GLACIER")
        self.assertEqual(config.lifecycle_days_to_ia, 60)
        self.assertEqual(config.endpoint_url, "https://minio.local:9000")

    def test_get_s3_client_kwargs(self):
        """Test S3 client kwargs generation."""
        config = S3ColdStorageConfig(
            bucket="test-bucket",
            aws_access_key_id="AKIA123",
            aws_secret_access_key="secret123",
            endpoint_url="https://custom.s3.example.com",
        )

        kwargs = config.get_s3_client_kwargs()

        self.assertEqual(kwargs["region_name"], "us-east-1")
        self.assertEqual(kwargs["aws_access_key_id"], "AKIA123")
        self.assertEqual(kwargs["aws_secret_access_key"], "secret123")
        self.assertEqual(kwargs["endpoint_url"], "https://custom.s3.example.com")


class TestSnapshotArchive(unittest.TestCase):
    """Tests for SnapshotArchive dataclass."""

    def test_snapshot_archive_creation(self):
        """Test creating a snapshot archive."""
        archive = SnapshotArchive(
            snapshot_id="snap_abc123",
            manifest_key="snapshots/ab/snap_abc123/manifest.json",
            block_keys=["blocks/ab/abc123def456"],
            total_size_bytes=1024,
            created_at=datetime.utcnow(),
            metadata={"user_id": "user_123"},
            checksum="abc123",
        )

        self.assertEqual(archive.snapshot_id, "snap_abc123")
        self.assertEqual(len(archive.block_keys), 1)
        self.assertEqual(archive.total_size_bytes, 1024)
        self.assertEqual(archive.metadata["user_id"], "user_123")

    def test_snapshot_archive_default_values(self):
        """Test default values for snapshot archive."""
        archive = SnapshotArchive(
            snapshot_id="snap_test",
            manifest_key="snapshots/te/snap_test/manifest.json",
            block_keys=[],
            total_size_bytes=0,
            created_at=datetime.utcnow(),
        )

        self.assertEqual(archive.metadata, {})
        self.assertEqual(archive.checksum, "")


class TestS3ObjectMetadata(unittest.TestCase):
    """Tests for S3ObjectMetadata dataclass."""

    def test_object_metadata_creation(self):
        """Test creating S3 object metadata."""
        metadata = S3ObjectMetadata(
            key="snapshots/ab/snap_test/manifest.json",
            size_bytes=2048,
            etag='"abc123"',
            last_modified=datetime.utcnow(),
            storage_class="STANDARD_IA",
            version_id="v1.2",
            checksum="sha256:xyz",
            is_restored=True,
            restore_expiry=datetime.utcnow() + timedelta(days=1),
        )

        self.assertEqual(metadata.key, "snapshots/ab/snap_test/manifest.json")
        self.assertEqual(metadata.size_bytes, 2048)
        self.assertTrue(metadata.is_restored)


class TestS3StorageFactory(unittest.TestCase):
    """Tests for S3StorageFactory."""

    @patch.dict(os.environ, {"MEM0_S3_BUCKET": "env-bucket", "MEM0_S3_REGION": "eu-west-1"})
    def test_create_from_env(self):
        """Test creating storage from environment variables."""
        with patch.dict(os.environ, {"MEM0_S3_BUCKET": "env-bucket", "MEM0_S3_REGION": "eu-west-1"}):
            factory = S3StorageFactory()
            storage = factory.create_from_env()

            self.assertEqual(storage.config.bucket, "env-bucket")
            self.assertEqual(storage.config.region, "eu-west-1")

    def test_create_from_config(self):
        """Test creating storage from config object."""
        config = S3ColdStorageConfig(bucket="config-bucket", region="ap-southeast-1")
        storage = S3StorageFactory.create(config)

        self.assertEqual(storage.config.bucket, "config-bucket")
        self.assertEqual(storage.config.region, "ap-southeast-1")


class TestS3ColdStorageRepository(unittest.TestCase):
    """Tests for S3ColdStorageRepository."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = S3ColdStorageConfig(
            bucket="test-bucket",
            region="us-east-1",
            storage_class=S3StorageClass.STANDARD_IA.value,
        )
        self.repository = S3ColdStorageRepository(self.config)

    def test_initialization(self):
        """Test repository initialization."""
        self.assertFalse(self.repository._initialized)
        self.assertEqual(self.repository.config.bucket, "test-bucket")
        self.assertTrue(self.repository._local_cache_dir.exists())

    def test_get_snapshot_prefix(self):
        """Test snapshot prefix generation."""
        prefix = self.repository._get_snapshot_prefix("snap_abc123def456")

        self.assertEqual(prefix, "snapshots/sn/snap_abc123def456")

    def test_get_block_key(self):
        """Test block key generation."""
        key = self.repository._get_block_key("abc123def456abc123def456abc123def456abc123def456")

        self.assertEqual(key, "blocks/ab/abc123def456abc123def456abc123def456abc123def456")

    def test_compute_checksum_sha256(self):
        """Test SHA256 checksum computation."""
        data = b"test data for checksum"
        checksum = self.repository._compute_checksum(data)

        self.assertEqual(len(checksum), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in checksum))

    def test_compute_checksum_md5(self):
        """Test MD5 checksum computation with different algorithm."""
        self.config.checksum_algorithm = "MD5"
        repository = S3ColdStorageRepository(self.config)

        data = b"test data"
        checksum = repository._compute_checksum(data)

        self.assertEqual(len(checksum), 32)

    def test_get_stats(self):
        """Test getting repository statistics."""
        stats = self.repository.get_stats()

        self.assertIn("uploads", stats)
        self.assertIn("downloads", stats)
        self.assertIn("restores", stats)
        self.assertIn("bytes_uploaded", stats)
        self.assertIn("initialized", stats)
        self.assertFalse(stats["initialized"])


class TestS3StorageRepositoryAsync(unittest.IsolatedAsyncioTestCase):
    """Async tests for S3ColdStorageRepository with proper mocking."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = S3ColdStorageConfig(
            bucket="test-bucket",
            region="us-east-1",
        )
        self.repository = S3ColdStorageRepository(self.config)

        self.mock_s3_client = MagicMock()
        self.repository._s3_client = self.mock_s3_client

    async def test_initialize_already_initialized(self):
        """Test that initialize is idempotent."""
        self.repository._initialized = True

        await self.repository.initialize()

        self.assertTrue(self.repository._initialized)

    async def test_delete_snapshot_no_objects(self):
        """Test deleting snapshot with no objects."""
        self.mock_s3_client.list_objects_v2.return_value = {"Contents": []}

        result = await self.repository.delete_snapshot("snap_nonexistent")

        self.assertFalse(result)

    async def test_download_snapshot_invalid_snapshot(self):
        """Test downloading non-existent snapshot."""
        self.mock_s3_client.get_object.side_effect = Exception("Not found")

        with self.assertRaises(Exception):
            await self.repository.download_snapshot("snap_nonexistent")

    async def test_list_snapshots_empty(self):
        """Test listing snapshots with empty bucket."""
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

        snapshots = await self.repository.list_snapshots()

        self.assertEqual(len(snapshots), 0)

    async def test_list_snapshots_with_data(self):
        """Test listing snapshots with existing data."""
        last_modified = datetime.utcnow()
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "snapshots/ab/snap_test123/manifest.json",
                        "Size": 1024,
                        "LastModified": last_modified,
                        "Metadata": {},
                    }
                ]
            }
        ]

        snapshots = await self.repository.list_snapshots()

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].snapshot_id, "snap_test123")

    async def test_get_archive_stats_empty(self):
        """Test getting archive stats with no archives."""
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

        stats = await self.repository.get_archive_stats()

        self.assertEqual(stats.total_snapshots, 0)
        self.assertEqual(stats.total_size_bytes, 0)

    async def test_get_archive_stats_with_data(self):
        """Test getting archive stats with archives."""
        last_modified = datetime.utcnow()
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "snapshots/ab/snap_abc/manifest.json",
                        "Size": 1024,
                        "LastModified": last_modified,
                        "Metadata": {},
                    },
                    {
                        "Key": "snapshots/de/snap_def/manifest.json",
                        "Size": 2048,
                        "LastModified": last_modified + timedelta(hours=1),
                        "Metadata": {},
                    },
                ]
            }
        ]

        stats = await self.repository.get_archive_stats()

        self.assertEqual(stats.total_snapshots, 2)
        self.assertEqual(stats.total_size_bytes, 3072)
        self.assertIsNotNone(stats.oldest_snapshot)
        self.assertIsNotNone(stats.newest_snapshot)

    async def test_get_object_metadata_not_found(self):
        """Test getting metadata for non-existent object."""
        self.mock_s3_client.head_object.side_effect = Exception("Not found")

        metadata = await self.repository.get_object_metadata("snap_nonexistent")

        self.assertIsNone(metadata)

    async def test_get_object_metadata_found(self):
        """Test getting metadata for existing object."""
        last_modified = datetime.utcnow()
        self.mock_s3_client.head_object.return_value = {
            "ContentLength": 2048,
            "ETag": '"abc123"',
            "LastModified": last_modified,
            "StorageClass": "STANDARD_IA",
            "VersionId": "v1.0",
            "Metadata": {"checksum": "sha256:xyz"},
        }

        metadata = await self.repository.get_object_metadata("snap_test123")

        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.size_bytes, 2048)
        self.assertEqual(metadata.storage_class, "STANDARD_IA")

    async def test_restore_from_glacier_expedited(self):
        """Test Glacier restore with Expedited tier."""
        result = await self.repository.restore_from_glacier("snap_test123", tier="Expedited")

        self.assertTrue(result)
        self.mock_s3_client.restore_object.assert_called_once()

    async def test_restore_from_glacier_standard(self):
        """Test Glacier restore with Standard tier."""
        result = await self.repository.restore_from_glacier("snap_test123", tier="Standard")

        self.assertTrue(result)

        call_args = self.mock_s3_client.restore_object.call_args
        restore_request = call_args[1]["RestoreRequest"]
        self.assertEqual(restore_request["GlacierJobParameters"]["Tier"], "Standard")
        self.assertEqual(restore_request["Days"], 3)


class TestS3StorageClass(unittest.TestCase):
    """Tests for S3StorageClass enum."""

    def test_storage_classes(self):
        """Test all storage classes are defined."""
        self.assertEqual(S3StorageClass.STANDARD.value, "STANDARD")
        self.assertEqual(S3StorageClass.STANDARD_IA.value, "STANDARD_IA")
        self.assertEqual(S3StorageClass.ONEZONE_IA.value, "ONEZONE_IA")
        self.assertEqual(S3StorageClass.INTELLIGENT_TIERING.value, "INTELLIGENT_TIERING")
        self.assertEqual(S3StorageClass.GLACIER.value, "GLACIER")
        self.assertEqual(S3StorageClass.GLACIER_DEEP_ARCHIVE.value, "DEEP_ARCHIVE")
        self.assertEqual(S3StorageClass.GLACIER_IR.value, "GLACIER_IR")


class TestIntegrationScenarios(unittest.IsolatedAsyncioTestCase):
    """Integration test scenarios for S3 cold storage."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = S3ColdStorageConfig(
            bucket="integration-test-bucket",
            region="us-east-1",
            storage_class=S3StorageClass.STANDARD_IA.value,
            lifecycle_days_to_ia=30,
            lifecycle_days_to_glacier=90,
        )
        self.repository = S3ColdStorageRepository(self.config)
        self.mock_s3_client = MagicMock()
        self.repository._s3_client = self.mock_s3_client

    async def test_full_snapshot_archive_flow(self):
        """Test complete snapshot archive and restore flow."""
        snapshot_id = "snap_integration_test"
        manifest = {
            "id": snapshot_id,
            "user_id": "test_user",
            "total_size": 1024,
            "chunks": [
                {"digest": "abc123", "offset": 0, "size": 512},
                {"digest": "def456", "offset": 512, "size": 512},
            ],
        }
        blocks = {
            "abc123": b"a" * 512,
            "def456": b"b" * 512,
        }

        put_objects = []

        def capture_put_object(**kwargs):
            put_objects.append(kwargs)
            return {"ETag": '"abc123"'}

        self.mock_s3_client.put_object = capture_put_object
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

        def mock_get_object(**kwargs):
            key = kwargs.get("Key", "")
            if "manifest" in key:
                return {"Body": MagicMock(read=MagicMock(return_value=json.dumps(manifest).encode()))}
            else:
                digest = key.split("/")[-1]
                block_data = blocks.get(digest, b"")
                return {"Body": MagicMock(read=MagicMock(return_value=block_data))}

        self.mock_s3_client.get_object = mock_get_object

        archive = await self.repository.upload_snapshot(
            snapshot_id,
            manifest,
            blocks,
            metadata={"test": "integration"},
        )

        self.assertEqual(archive.snapshot_id, snapshot_id)
        self.assertEqual(len(archive.block_keys), 2)
        self.assertGreater(archive.total_size_bytes, 1000)

        manifest_data, downloaded_blocks = await self.repository.download_snapshot(snapshot_id)

        self.assertEqual(manifest_data["id"], snapshot_id)

    async def test_snapshot_deletion_flow(self):
        """Test snapshot deletion flow."""
        snapshot_id = "snap_delete_test"

        paginator_mock = MagicMock()
        paginator_mock.paginate.return_value = [
            {
                "Contents": [
                    {"Key": f"snapshots/te/{snapshot_id}/manifest.json"},
                    {"Key": f"blocks/te/abc123"},
                ]
            }
        ]
        self.mock_s3_client.get_paginator.return_value = paginator_mock

        result = await self.repository.delete_snapshot(snapshot_id)

        self.assertTrue(result)
        self.mock_s3_client.delete_objects.assert_called_once()


if __name__ == "__main__":
    unittest.main()

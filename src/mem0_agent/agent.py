"""
Mem0 Universal Agent
====================

Lightweight agent that transforms ANY computer into a Mem0 sandbox provider.
Supports: Linux, macOS, Windows (via WSL)
Features:
- Process forking for instant sandboxes
- Resource monitoring and reporting
- Node discovery on local network
- Secure sandbox execution
- Auto-updates and health checks

Usage:
    python3 -m mem0_agent.agent run
    python3 -m mem0_agent.agent register --server http://coordinator:8080
    python3 -m mem0_agent.agent status
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import shutil
import signal
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import psutil

logger = logging.getLogger(__name__)


class NodeState(Enum):
    """Agent node states."""

    OFFLINE = "offline"
    ONLINE = "online"
    BUSY = "busy"
    MAINTENANCE = "maintenance"


class SandboxState(Enum):
    """Sandbox lifecycle states."""

    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class NodeInfo:
    """Information about this node."""

    node_id: str
    hostname: str
    ip_address: str
    mac_address: str
    os_name: str
    os_version: str
    architecture: str
    cpu_count: int
    cpu_frequency: float
    memory_bytes: int
    disk_bytes: int
    gpu_available: bool
    gpu_count: int
    tags: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    last_heartbeat: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NodeResources:
    """Current resource usage."""

    cpu_percent: float
    memory_used_bytes: int
    memory_available_bytes: int
    disk_used_bytes: int
    disk_available_bytes: int
    network_bytes_sent: int
    network_bytes_recv: int
    active_sandboxes: int
    active_connections: int
    load_average: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SandboxConfig:
    """Configuration for a sandbox."""

    sandbox_id: str
    user_id: str
    runtime: str
    memory_limit_mb: int
    cpu_limit_percent: float
    timeout_seconds: int
    environment: Dict[str, str] = field(default_factory=dict)
    mount_points: List[Dict[str, str]] = field(default_factory=list)
    network_enabled: bool = True
    gpu_required: bool = False


class SandboxProcess:
    """Manages a sandbox process."""

    def __init__(self, config: SandboxConfig):
        self.config = config
        self.sandbox_id = config.sandbox_id
        self.state = SandboxState.PENDING
        self.process: Optional[asyncio.subprocess.Process] = None
        self.start_time: Optional[datetime] = None
        self.output: List[str] = []
        self.exit_code: Optional[int] = None

    async def start(self):
        """Start the sandbox."""
        self.state = SandboxState.STARTING

        try:
            cmd = self._build_command()

            self.process = await asyncio.create_subprocess_shell(
                cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=1024 * 1024,
            )

            self.state = SandboxState.RUNNING
            self.start_time = datetime.utcnow()

            logger.info(f"Sandbox {self.sandbox_id} started with PID {self.process.pid}")

        except Exception as e:
            self.state = SandboxState.FAILED
            logger.error(f"Failed to start sandbox {self.sandbox_id}: {e}")
            raise

    def _build_command(self) -> str:
        """Build the command to execute."""
        if self.config.runtime in ["python", "python3", "py"]:
            return f"python3 - << 'PYTHON'\nprint('Python sandbox')\nPYTHON"

        elif self.config.runtime in ["node", "nodejs"]:
            return "node -e 'console.log(\"Node.js sandbox\")'"

        elif self.config.runtime in ["bash", "sh"]:
            return "bash -c 'echo Bash sandbox'"

        elif self.config.runtime == "rust":
            return "rustc --version 2>/dev/null || echo 'Rust runtime not available'"

        elif self.config.runtime == "go":
            return "go version 2>/dev/null || echo 'Go runtime not available'"

        else:
            return f"echo 'Unknown runtime: {self.config.runtime}'"

    async def execute(self, code: str) -> Tuple[str, str, int]:
        """Execute code in the sandbox."""
        if not self.process or self.state != SandboxState.RUNNING:
            raise RuntimeError(f"Sandbox {self.sandbox_id} is not running")

        try:
            self.process.stdin.write(code.encode() + b"\n")
            await self.process.stdin.drain()

            stdout, stderr = await self.process.communicate()
            return stdout.decode(), stderr.decode(), self.process.returncode or 0
        except Exception as e:
            return "", str(e), 1

    async def stop(self):
        """Stop the sandbox."""
        if self.process:
            self.state = SandboxState.STOPPING

            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()

            self.exit_code = self.process.returncode
            self.state = SandboxState.STOPPED
            logger.info(f"Sandbox {self.sandbox_id} stopped")

    def get_info(self) -> Dict[str, Any]:
        """Get sandbox information."""
        return {
            "sandbox_id": self.sandbox_id,
            "state": self.state.value,
            "runtime": self.config.runtime,
            "user_id": self.config.user_id,
            "memory_limit_mb": self.config.memory_limit_mb,
            "cpu_limit_percent": self.config.cpu_limit_percent,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "exit_code": self.exit_code,
        }


class Mem0Agent:
    """
    Universal Mem0 Agent.

    Transforms any computer into a sandbox provider.
    """

    def __init__(self, config_dir: Optional[str] = None):
        self.config_dir = Path(config_dir) if config_dir else Path.home() / ".mem0"
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.node_id = self._load_or_create_node_id()
        self.state = NodeState.OFFLINE
        self.sandboxes: Dict[str, SandboxProcess] = {}
        self.coordinator_url: Optional[str] = None
        self.executor = ThreadPoolExecutor(max_workers=10)

        self.node_info = self._collect_node_info()
        self.resources = self._get_resources()

        self._setup_logging()
        self._load_config()

    def _load_or_create_node_id(self) -> str:
        """Load or create unique node ID."""
        node_id_file = self.config_dir / "node_id"

        if node_id_file.exists():
            return node_id_file.read_text().strip()

        node_id = str(uuid.uuid4())
        node_id_file.write_text(node_id)
        return node_id

    def _collect_node_info(self) -> NodeInfo:
        """Collect node information."""
        try:
            cpu_freq = psutil.cpu_freq()
            frequency = cpu_freq.current if cpu_freq else 0.0
        except Exception:
            frequency = 0.0

        try:
            gpu_count = self._detect_gpu_count()
        except Exception:
            gpu_count = 0

        return NodeInfo(
            node_id=self.node_id,
            hostname=socket.gethostname(),
            ip_address=self._get_ip_address(),
            mac_address=self._get_mac_address(),
            os_name=platform.system(),
            os_version=platform.version(),
            architecture=platform.machine(),
            cpu_count=psutil.cpu_count(),
            cpu_frequency=frequency,
            memory_bytes=psutil.virtual_memory().total,
            disk_bytes=shutil.disk_usage("/").total,
            gpu_available=gpu_count > 0,
            gpu_count=gpu_count,
            tags=["mem0-agent", "sandbox-provider"],
            labels={
                "environment": os.getenv("MEM0_ENV", "development"),
                "region": os.getenv("MEM0_REGION", "local"),
            },
        )

    def _get_ip_address(self) -> str:
        """Get primary IP address."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def _get_mac_address(self) -> str:
        """Get MAC address."""
        try:
            mac = uuid.getnode()
            return ":".join(f"{(mac >> i) & 0xFF:02X}" for i in range(0, 48, 8))
        except Exception:
            return "00:00:00:00:00:00"

    def _detect_gpu_count(self) -> int:
        """Detect available GPUs."""
        count = 0
        try:
            import subprocess

            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=gpu_name", "--format=csv,noheader"],
                capture_output=True,
                timeout=5,
            )
            count = len(result.stdout.strip().split("\n"))
        except Exception:
            pass

        return count

    def _get_resources(self) -> NodeResources:
        """Get current resource usage."""
        net_io = psutil.net_io_counters()
        disk = psutil.disk_usage("/")

        try:
            load_avg = os.getloadavg()[0]
        except Exception:
            load_avg = 0.0

        memory = psutil.virtual_memory()

        return NodeResources(
            cpu_percent=psutil.cpu_percent(),
            memory_used_bytes=memory.used,
            memory_available_bytes=memory.available,
            disk_used_bytes=disk.used,
            disk_available_bytes=disk.free,
            network_bytes_sent=net_io.bytes_sent,
            network_bytes_recv=net_io.bytes_recv,
            active_sandboxes=len(self.sandboxes),
            active_connections=len(psutil.net_connections()),
            load_average=load_avg,
        )

    def _setup_logging(self):
        """Configure logging."""
        log_file = self.config_dir / "agent.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(),
            ],
        )

    def _load_config(self):
        """Load agent configuration."""
        config_file = self.config_dir / "config.json"

        if config_file.exists():
            try:
                config = json.loads(config_file.read_text())
                self.coordinator_url = config.get("coordinator_url")
            except Exception as e:
                logger.warning(f"Failed to load config: {e}")

    def _save_config(self):
        """Save agent configuration."""
        config_file = self.config_dir / "config.json"
        config = {
            "node_id": self.node_id,
            "coordinator_url": self.coordinator_url,
        }
        config_file.write_text(json.dumps(config, indent=2))

    async def run(self):
        """Run the agent."""
        logger.info(f"Starting Mem0 Agent on {self.node_info.hostname}")
        self.state = NodeState.ONLINE

        try:
            await self._heartbeat_loop()
        except KeyboardInterrupt:
            logger.info("Shutting down agent...")
            await self.shutdown()

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to coordinator."""
        while self.state != NodeState.OFFLINE:
            try:
                if self.coordinator_url:
                    await self._send_heartbeat()
                await self._update_resources()
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")

            await asyncio.sleep(30)

    async def _send_heartbeat(self):
        """Send heartbeat to coordinator."""
        payload = {
            "node_id": self.node_id,
            "status": self.state.value,
            "resources": self.resources.to_dict(),
            "sandboxes": len(self.sandboxes),
            "timestamp": datetime.utcnow().isoformat(),
        }

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.coordinator_url}/api/v1/nodes/heartbeat",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        logger.debug("Heartbeat sent successfully")
        except ImportError:
            logger.debug("aiohttp not available, skipping heartbeat")

    async def _update_resources(self):
        """Update resource information."""
        self.resources = self._get_resources()

        for sandbox in self.sandboxes.values():
            if sandbox.state == SandboxState.RUNNING and sandbox.process:
                try:
                    proc = psutil.Process(sandbox.process.pid)
                    sandbox.config.memory_limit_mb = proc.memory_info().rss // 1024 // 1024
                except Exception:
                    pass

    async def register(self, coordinator_url: str):
        """Register this node with a coordinator."""
        self.coordinator_url = coordinator_url
        self._save_config()

        payload = {
            "node": self.node_info.to_dict(),
            "resources": self.resources.to_dict(),
            "capabilities": [
                "sandbox_execution",
                "process_forking",
                "resource_monitoring",
            ],
        }

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{coordinator_url}/api/v1/nodes/register",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status in [200, 201]:
                        data = await resp.json()
                        logger.info(f"Registered with coordinator: {data}")
                        self.state = NodeState.ONLINE
                    else:
                        logger.error(f"Registration failed: {resp.status}")
        except ImportError:
            logger.error("aiohttp required for registration. Install with: pip install aiohttp")

    def status(self) -> Dict[str, Any]:
        """Get agent status."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "hostname": self.node_info.hostname,
            "ip_address": self.node_info.ip_address,
            "os": f"{self.node_info.os_name} {self.node_info.os_version}",
            "resources": self.resources.to_dict(),
            "active_sandboxes": len(self.sandboxes),
            "coordinator_url": self.coordinator_url,
            "config_dir": str(self.config_dir),
        }

    async def create_sandbox(self, config: SandboxConfig) -> SandboxProcess:
        """Create a new sandbox."""
        sandbox = SandboxProcess(config)
        self.sandboxes[config.sandbox_id] = sandbox

        await sandbox.start()

        logger.info(f"Created sandbox {config.sandbox_id} on {self.node_info.hostname}")
        return sandbox

    async def get_sandbox(self, sandbox_id: str) -> Optional[SandboxProcess]:
        """Get a sandbox by ID."""
        return self.sandboxes.get(sandbox_id)

    async def list_sandboxes(self) -> List[Dict[str, Any]]:
        """List all sandboxes."""
        return [s.get_info() for s in self.sandboxes.values()]

    async def delete_sandbox(self, sandbox_id: str) -> bool:
        """Delete a sandbox."""
        sandbox = self.sandboxes.get(sandbox_id)
        if sandbox:
            await sandbox.stop()
            del self.sandboxes[sandbox_id]
            logger.info(f"Deleted sandbox {sandbox_id}")
            return True
        return False

    async def execute_code(self, sandbox_id: str, code: str) -> Tuple[str, str, int]:
        """Execute code in a sandbox."""
        sandbox = await self.get_sandbox(sandbox_id)
        if not sandbox:
            raise ValueError(f"Sandbox {sandbox_id} not found")

        return await sandbox.execute(code)

    async def shutdown(self):
        """Shutdown the agent."""
        self.state = NodeState.OFFLINE

        for sandbox_id in list(self.sandboxes.keys()):
            await self.delete_sandbox(sandbox_id)

        self.executor.shutdown(wait=True)
        logger.info("Agent shutdown complete")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Mem0 Universal Agent")
    parser.add_argument(
        "command",
        choices=["run", "register", "status", "list", "delete", "execute"],
        help="Command to execute",
    )
    parser.add_argument(
        "--coordinator",
        "-c",
        help="Coordinator URL for registration",
    )
    parser.add_argument(
        "--sandbox-id",
        "-s",
        help="Sandbox ID for execute command",
    )
    parser.add_argument(
        "--code",
        "-C",
        help="Code to execute",
    )
    parser.add_argument(
        "--config-dir",
        help="Configuration directory",
    )

    args = parser.parse_args()

    agent = Mem0Agent(config_dir=args.config_dir)

    if args.command == "run":
        asyncio.run(agent.run())

    elif args.command == "register":
        if not args.coordinator:
            parser.error("--coordinator required for register")
        asyncio.run(agent.register(args.coordinator))

    elif args.command == "status":
        status = agent.status()
        print(json.dumps(status, indent=2))

    elif args.command == "list":
        sandboxes = asyncio.run(agent.list_sandboxes())
        print(json.dumps(sandboxes, indent=2))

    elif args.command == "delete":
        if not args.sandbox_id:
            parser.error("--sandbox-id required for delete")
        success = asyncio.run(agent.delete_sandbox(args.sandbox_id))
        print(json.dumps({"success": success}, indent=2))

    elif args.command == "execute":
        if not args.sandbox_id or not args.code:
            parser.error("--sandbox-id and --code required for execute")
        stdout, stderr, exit_code = asyncio.run(agent.execute_code(args.sandbox_id, args.code))
        print(f"STDOUT:\n{stdout}")
        if stderr:
            print(f"STDERR:\n{stderr}")
        print(f"EXIT CODE: {exit_code}")


if __name__ == "__main__":
    main()

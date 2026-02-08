"""Sandbox Executor - Containerized code execution."""

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    execution_time_ms: float
    memory_used_bytes: int = 0


@dataclass
class SandboxConfig:
    max_memory_mb: int = 512
    timeout_seconds: int = 300
    network_access: bool = True
    environment: Dict[str, str] = field(default_factory=dict)


class LocalProcessExecutor:
    def __init__(self, workdir: str = "/tmp/sandbox-workdir"):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self._sandboxes: Dict[str, Dict] = {}

    async def create(self, config: Optional[SandboxConfig] = None) -> str:
        sandbox_id = f"local_{uuid.uuid4().hex[:12]}"
        sandbox_dir = self.workdir / sandbox_id
        sandbox_dir.mkdir(parents=True, exist_ok=True)

        self._sandboxes[sandbox_id] = {
            "dir": sandbox_dir,
            "config": config or SandboxConfig(),
        }
        return sandbox_id

    async def execute(
        self, sandbox_id: str, code: str, timeout_seconds: int = 300, env: Optional[Dict] = None
    ) -> ExecutionResult:
        if sandbox_id not in self._sandboxes:
            raise FileNotFoundError(sandbox_id)

        sandbox = self._sandboxes[sandbox_id]
        sandbox_dir = sandbox["dir"]
        config = sandbox["config"]

        env_copy = os.environ.copy()
        env_copy.update(config.environment)
        if env:
            env_copy.update(env)

        import time

        start = time.time()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", dir=sandbox_dir, delete=False) as f:
            f.write(code)
            script_path = f.name

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                script_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env_copy,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
            except asyncio.TimeoutExpired:
                proc.terminate()
                return ExecutionResult(
                    success=False,
                    exit_code=-1,
                    stdout="",
                    stderr="Timeout expired",
                    execution_time_ms=float(timeout_seconds) * 1000,
                )

            rc = proc.returncode
            return ExecutionResult(
                success=rc == 0 if rc is not None else False,
                exit_code=rc if rc is not None else -1,
                stdout=stdout.decode() if stdout else "",
                stderr=stderr.decode() if stderr else "",
                execution_time_ms=(time.time() - start) * 1000,
            )
        finally:
            try:
                os.unlink(script_path)
            except:
                pass

    async def delete(self, sandbox_id: str) -> bool:
        if sandbox_id not in self._sandboxes:
            return False
        sandbox = self._sandboxes[sandbox_id]
        shutil.rmtree(sandbox["dir"], ignore_errors=True)
        del self._sandboxes[sandbox_id]
        return True

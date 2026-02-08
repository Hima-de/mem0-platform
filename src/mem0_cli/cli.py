#!/usr/bin/env python3
"""
Mem0 CLI - Command Line Interface
=================================

Universal CLI for Mem0 Platform.
Turn any computer into a sandbox provider.

Usage:
    mem0 agent run              # Run the agent
    mem0 agent register         # Register with coordinator
    mem0 agent status          # Show agent status
    mem0 sandbox create        # Create a sandbox
    mem0 sandbox list          # List sandboxes
    mem0 sandbox exec          # Execute code
    mem0 sandbox delete        # Delete a sandbox
    mem0 node list             # List registered nodes
    mem0 install               # Install Mem0 on any machine

Examples:
    mem0 agent run              # Start the agent
    mem0 sandbox create --runtime python --memory 512
    mem0 sandbox exec --id abc123 --code "print('hello')"
    mem0 install --server coordinator.mem0.ai
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


class Colors:
    """ANSI color codes."""

    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"


def format_bytes(num: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024.0:
            return f"{num:.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}PB"


def format_time(iso_time: str) -> str:
    """Format ISO time to readable format."""
    try:
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_time


class Mem0CLI:
    """Mem0 Command Line Interface."""

    def __init__(self):
        self.config_dir = Path.home() / ".mem0"
        self.config_file = self.config_dir / "cli_config.json"
        self._load_config()

    def _load_config(self):
        """Load CLI configuration."""
        if self.config_file.exists():
            try:
                self.config = json.loads(self.config_file.read_text())
            except Exception:
                self.config = {}
        else:
            self.config = {}

    def _save_config(self):
        """Save CLI configuration."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_file.write_text(json.dumps(self.config, indent=2))

    def get_server(self) -> str:
        """Get configured server."""
        return self.config.get("server", "http://localhost:8000")

    def set_server(self, server: str):
        """Set server URL."""
        self.config["server"] = server
        self._save_config()

    async def agent_run(self, coordinator: Optional[str] = None):
        """Run the agent."""
        from mem0_agent.agent import Mem0Agent

        if coordinator:
            self.set_server(coordinator)

        agent = Mem0Agent()
        print(f"{Colors.GREEN}Starting Mem0 Agent...{Colors.ENDC}")
        print(f"Node ID: {agent.node_id}")
        print(f"Hostname: {agent.node_info.hostname}")
        print(f"IP: {agent.node_info.ip_address}")
        print(f"CPU: {agent.node_info.cpu_count}x @ {agent.node_info.cpu_frequency:.0f}MHz")
        print(f"Memory: {format_bytes(agent.node_info.memory_bytes)}")
        print(f"GPU: {'Yes' if agent.node_info.gpu_available else 'No'}")
        print()
        print(f"{Colors.CYAN}Press Ctrl+C to stop{Colors.ENDC}")

        await agent.run()

    async def agent_status(self) -> Dict[str, Any]:
        """Show agent status."""
        from mem0_agent.agent import Mem0Agent

        agent = Mem0Agent()
        status = agent.status()

        print(f"\n{Colors.BOLD}=== Mem0 Agent Status ==={Colors.ENDC}")
        print(f"Node ID:     {status['node_id']}")
        print(f"State:       {Colors.GREEN}{status['state']}{Colors.ENDC}")
        print(f"Hostname:    {status['hostname']}")
        print(f"IP Address:  {status['ip_address']}")
        print(f"OS:          {status['os']}")
        print()
        print(f"{Colors.BOLD}Resources:{Colors.ENDC}")
        resources = status["resources"]
        print(f"  CPU:        {resources['cpu_percent']:.1f}%")
        print(
            f"  Memory:     {format_bytes(resources['memory_used_bytes'])} / {format_bytes(resources['memory_available_bytes'])}"
        )
        print(
            f"  Disk:      {format_bytes(resources['disk_used_bytes'])} / {format_bytes(resources['disk_available_bytes'])}"
        )
        print(f"  Load:       {resources['load_average']:.2f}")
        print()
        print(f"{Colors.BOLD}Sandboxes:{Colors.ENDC}")
        print(f"  Active:     {status['active_sandboxes']}")
        print()
        print(f"Coordinator: {status.get('coordinator_url', 'Not configured')}")

        return status

    async def agent_register(self, coordinator: str):
        """Register with coordinator."""
        from mem0_agent.agent import Mem0Agent

        if not coordinator:
            coordinator = self.get_server()

        print(f"{Colors.CYAN}Registering with {coordinator}...{Colors.ENDC}")

        agent = Mem0Agent()
        await agent.register(coordinator)

        print(f"{Colors.GREEN}Registration successful!{Colors.ENDC}")
        print(f"Node ID: {agent.node_id}")

    async def sandbox_create(
        self,
        runtime: str,
        memory: int = 512,
        cpu: float = 100.0,
        timeout: int = 300,
        user_id: str = "default",
    ) -> Dict[str, Any]:
        """Create a new sandbox."""
        from mem0_agent.agent import Mem0Agent, SandboxConfig

        sandbox_id = f"sandbox_{int(time.time())}_{os.getpid() % 1000:04d}"
        config = SandboxConfig(
            sandbox_id=sandbox_id,
            user_id=user_id,
            runtime=runtime,
            memory_limit_mb=memory,
            cpu_limit_percent=cpu,
            timeout_seconds=timeout,
        )

        agent = Mem0Agent()
        sandbox = await agent.create_sandbox(config)

        print(f"\n{Colors.GREEN}Sandbox created successfully!{Colors.ENDC}")
        print(f"  ID:       {sandbox_id}")
        print(f"  Runtime:  {runtime}")
        print(f"  Memory:   {memory}MB")
        print(f"  CPU:      {cpu}%")
        print(f"  Timeout:  {timeout}s")

        return sandbox.get_info()

    async def sandbox_list(self) -> List[Dict[str, Any]]:
        """List all sandboxes."""
        from mem0_agent.agent import Mem0Agent

        agent = Mem0Agent()
        sandboxes = await agent.list_sandboxes()

        if not sandboxes:
            print(f"\n{Colors.WARNING}No active sandboxes{Colors.ENDC}")
            return []

        print(f"\n{Colors.BOLD}=== Active Sandboxes ==={Colors.ENDC}")
        print(f"{'ID':<20} {'STATE':<12} {'RUNTIME':<10} {'MEMORY':<10} {'STARTED':<20}")
        print("-" * 75)

        for s in sandboxes:
            state_color = Colors.GREEN if s["state"] == "running" else Colors.WARNING
            print(
                f"{s['sandbox_id'][:20]:<20} "
                f"{state_color}{s['state']:<12}{Colors.ENDC} "
                f"{s['runtime']:<10} "
                f"{s.get('memory_limit_mb', 'N/A')}MB "
                f"{format_time(s.get('start_time', 'N/A')):<20}"
            )

        return sandboxes

    async def sandbox_exec(self, sandbox_id: str, code: str):
        """Execute code in a sandbox."""
        from mem0_agent.agent import Mem0Agent

        agent = Mem0Agent()
        stdout, stderr, exit_code = await agent.execute_code(sandbox_id, code)

        print(f"\n{Colors.BOLD}=== Output ==={Colors.ENDC}")
        if stdout:
            print(stdout)
        if stderr:
            print(f"{Colors.WARNING}{stderr}{Colors.ENDC}")
        print(f"{Colors.CYAN}Exit Code: {exit_code}{Colors.ENDC}")

        return {"stdout": stdout, "stderr": stderr, "exit_code": exit_code}

    async def sandbox_delete(self, sandbox_id: str) -> bool:
        """Delete a sandbox."""
        from mem0_agent.agent import Mem0Agent

        agent = Mem0Agent()
        success = await agent.delete_sandbox(sandbox_id)

        if success:
            print(f"{Colors.GREEN}Sandbox {sandbox_id} deleted{Colors.ENDC}")
        else:
            print(f"{Colors.FAIL}Sandbox {sandbox_id} not found{Colors.ENDC}")

        return success

    async def sandbox_logs(self, sandbox_id: str):
        """Show sandbox logs."""
        print(f"{Colors.CYAN}Logs for {sandbox_id}:{Colors.ENDC}")
        print("(Logs not yet implemented)")

    def install(self, server: str = "http://localhost:8000"):
        """Install Mem0 on this machine."""
        print(f"\n{Colors.BOLD}=== Mem0 Installation ==={Colors.ENDC}\n")

        print("This will install Mem0 Agent on this machine.")
        print(f"Coordinator Server: {server}\n")

        print("Installing dependencies...")
        os.system("pip install -q aiohttp psutil")

        self.set_server(server)

        print(f"\n{Colors.GREEN}Installation complete!{Colors.ENDC}\n")
        print("Next steps:")
        print(f"  1. Start the agent: {Colors.CYAN}mem0 agent run{Colors.ENDC}")
        print(f"  2. Register:        {Colors.CYAN}mem0 agent register --server {server}{Colors.ENDC}")
        print(f"  3. Create sandbox:  {Colors.CYAN}mem0 sandbox create --runtime python{Colors.ENDC}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Mem0 CLI - Universal Sandbox Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    parser.add_argument(
        "--server",
        "-s",
        help="Coordinator server URL",
    )

    agent_parser = subparsers.add_parser("agent", help="Agent commands")
    agent_sub = agent_parser.add_subparsers(dest="agent_command")
    agent_sub.add_parser("run", help="Run the agent").add_argument("--coordinator", "-c", help="Coordinator URL")
    agent_sub.add_parser("status", help="Show agent status")
    agent_sub.add_parser("register", help="Register with coordinator").add_argument(
        "--coordinator", "-c", help="Coordinator URL"
    )

    sandbox_parser = subparsers.add_parser("sandbox", help="Sandbox commands")
    sandbox_sub = sandbox_parser.add_subparsers(dest="sandbox_command")

    create = sandbox_sub.add_parser("create", help="Create a sandbox")
    create.add_argument("--runtime", "-r", default="python", help="Runtime (python, node, bash, etc.)")
    create.add_argument("--memory", "-m", type=int, default=512, help="Memory limit in MB")
    create.add_argument("--cpu", "-c", type=float, default=100.0, help="CPU limit in percent")
    create.add_argument("--timeout", "-t", type=int, default=300, help="Timeout in seconds")
    create.add_argument("--user", "-u", default="default", help="User ID")

    sandbox_sub.add_parser("list", help="List sandboxes")

    exec_cmd = sandbox_sub.add_parser("exec", help="Execute code in sandbox")
    exec_cmd.add_argument("--id", required=True, help="Sandbox ID")
    exec_cmd.add_argument("--code", "-C", required=True, help="Code to execute")

    delete = sandbox_sub.add_parser("delete", help="Delete a sandbox")
    delete.add_argument("--id", required=True, help="Sandbox ID")

    sandbox_sub.add_parser("logs", help="Show sandbox logs").add_argument("--id", required=True)

    node_parser = subparsers.add_parser("node", help="Node commands")
    node_sub = node_parser.add_subparsers(dest="node_command")
    node_sub.add_parser("list", help="List registered nodes")
    node_sub.add_parser("info", help="Show node info").add_argument("--id", required=True)

    install_parser = subparsers.add_parser("install", help="Install Mem0 on this machine")
    install_parser.add_argument("--server", "-s", default="http://localhost:8000", help="Coordinator server")

    args = parser.parse_args()

    cli = Mem0CLI()

    if args.server:
        cli.set_server(args.server)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        if args.command == "agent":
            if args.agent_command == "run":
                loop.run_until_complete(cli.agent_run(args.coordinator))
            elif args.agent_command == "status":
                loop.run_until_complete(cli.agent_status())
            elif args.agent_command == "register":
                loop.run_until_complete(cli.agent_register(args.coordinator))
            else:
                agent_parser.print_help()

        elif args.command == "sandbox":
            if args.sandbox_command == "create":
                loop.run_until_complete(
                    cli.sandbox_create(
                        runtime=args.runtime,
                        memory=args.memory,
                        cpu=args.cpu,
                        timeout=args.timeout,
                        user_id=args.user,
                    )
                )
            elif args.sandbox_command == "list":
                loop.run_until_complete(cli.sandbox_list())
            elif args.sandbox_command == "exec":
                loop.run_until_complete(cli.sandbox_exec(args.id, args.code))
            elif args.sandbox_command == "delete":
                loop.run_until_complete(cli.sandbox_delete(args.id))
            elif args.sandbox_command == "logs":
                loop.run_until_complete(cli.sandbox_logs(args.id))
            else:
                sandbox_parser.print_help()

        elif args.command == "node":
            if args.node_command == "list":
                print("Node list requires coordinator connection")
            else:
                node_parser.print_help()

        elif args.command == "install":
            cli.install(args.server)

        else:
            parser.print_help()

    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        loop.close()


if __name__ == "__main__":
    main()

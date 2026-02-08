"""
Mem0 Multi-Agent Collaboration System
====================================

Enables multiple AI agents to collaborate on complex tasks.
Features:
- Agent orchestration and coordination
- Shared memory and communication
- Role-based task assignment
- Hierarchical agent teams
- Real-time collaboration
"""

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
from abc import ABC, abstractmethod
from collections import deque
import logging

logger = logging.getLogger(__name__)


class AgentRole(Enum):
    """Agent roles in a team."""

    LEADER = "leader"
    COORDINATOR = "coordinator"
    SPECIALIST = "specialist"
    WORKER = "worker"
    OBSERVER = "observer"


class MessageType(Enum):
    """Types of inter-agent messages."""

    TASK = "task"
    RESULT = "result"
    QUERY = "query"
    RESPONSE = "response"
    STATUS = "status"
    ERROR = "error"
    SYNC = "sync"
    BROADCAST = "broadcast"


@dataclass
class AgentMessage:
    """Message between agents."""

    message_id: str
    sender_id: str
    recipient_id: str
    message_type: MessageType
    content: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    priority: int = 0
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "recipient_id": self.recipient_id,
            "message_type": self.message_type.value,
            "content": self.content,
            "timestamp": self.timestamp,
            "priority": self.priority,
            "correlation_id": self.correlation_id,
        }


@dataclass
class AgentCapability:
    """Agent capability or skill."""

    name: str
    description: str
    parameters: Dict[str, Any]
    handler: Callable

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
        }


@dataclass
class AgentConfig:
    """Configuration for an agent."""

    agent_id: str
    name: str
    role: AgentRole
    capabilities: List[AgentCapability]
    model: str = "gpt-4"
    max_iterations: int = 100
    timeout_seconds: int = 300
    memory_limit_mb: int = 512
    tools: List[str] = field(default_factory=list)
    instructions: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseAgent(ABC):
    """Base class for all agents."""

    def __init__(self, config: AgentConfig):
        self.config = config
        self.agent_id = config.agent_id
        self.name = config.name
        self.role = config.role
        self.capabilities = {c.name: c for c in config.capabilities}
        self.state: Dict[str, Any] = {}
        self.memory: deque = deque(maxlen=1000)
        self._running = False

    @abstractmethod
    async def process(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task and return result."""
        pass

    async def execute_capability(self, capability_name: str, **kwargs) -> Any:
        """Execute a capability."""
        if capability_name not in self.capabilities:
            raise ValueError(f"Capability {capability_name} not found")
        return await self.capabilities[capability_name].handler(**kwargs)

    def remember(self, key: str, value: Any):
        """Store in short-term memory."""
        self.memory.append(
            {
                "key": key,
                "value": value,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    def recall(self, key: str) -> Optional[Any]:
        """Recall from short-term memory."""
        for item in reversed(list(self.memory)):
            if item["key"] == key:
                return item["value"]
        return None

    async def think(self, context: Dict[str, Any]) -> str:
        """Generate thinking/reasoning."""
        return f"Agent {self.name} thinking about: {context}"


class SimpleAgent(BaseAgent):
    """Simple task-processing agent."""

    def __init__(self, config: AgentConfig):
        super().__init__(config)
        self.task_history: List[Dict] = []

    async def process(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task."""
        self._running = True
        self.task_history.append(
            {
                "task": task,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

        result = {
            "agent_id": self.agent_id,
            "agent_name": self.name,
            "task": task,
            "result": f"Processed by {self.name}",
            "status": "completed",
            "timestamp": datetime.utcnow().isoformat(),
        }

        self._running = False
        return result


class TeamLeader:
    """Agent team leader for orchestration."""

    def __init__(self, team_id: str, name: str):
        self.team_id = team_id
        self.name = name
        self.agents: Dict[str, BaseAgent] = {}
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.shared_state: Dict[str, Any] = {}
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    def add_agent(self, agent: BaseAgent):
        """Add an agent to the team."""
        self.agents[agent.agent_id] = agent
        logger.info(f"Added agent {agent.name} to team {self.team_id}")

    async def assign_task(self, task: Dict[str, Any], agent_id: Optional[str] = None) -> str:
        """Assign a task to an agent."""
        task_id = str(uuid.uuid4())
        await self.task_queue.put((task_id, task, agent_id))
        return task_id

    async def broadcast(self, message: AgentMessage):
        """Broadcast message to all agents."""
        for agent in self.agents.values():
            await self.message_queue.put(message)

    async def run_task_loop(self):
        """Main task processing loop."""
        self._running = True

        while self._running:
            try:
                task_id, task, preferred_agent_id = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)

                agent = self._select_agent(task, preferred_agent_id)
                if agent:
                    result = await agent.process(task)
                    self.shared_state[task_id] = result
                else:
                    logger.warning(f"No suitable agent for task: {task}")

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Task loop error: {e}")

    def _select_agent(self, task: Dict[str, Any], preferred_id: Optional[str] = None) -> Optional[BaseAgent]:
        """Select best agent for a task."""
        if preferred_id and preferred_id in self.agents:
            return self.agents[preferred_id]

        task_type = task.get("type", "general")

        for agent in self.agents.values():
            if task_type in [c.name for c in agent.capabilities.values()]:
                return agent

        return list(self.agents.values())[0] if self.agents else None

    def stop(self):
        """Stop the team."""
        self._running = False


class MultiAgentTeam:
    """Multi-agent collaboration system."""

    def __init__(self, team_id: str):
        self.team_id = team_id
        self.teams: Dict[str, TeamLeader] = {}
        self.global_memory: Dict[str, Any] = {}
        self.execution_graph: Dict[str, List[str]] = {}

    def create_team(self, team_id: str, name: str) -> TeamLeader:
        """Create a new agent team."""
        team = TeamLeader(team_id, name)
        self.teams[team_id] = team
        return team

    async def orchestrate(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a multi-step workflow."""
        results = {}
        steps = workflow.get("steps", [])

        for step in steps:
            step_id = step.get("id", str(uuid.uuid4()))
            team_id = step.get("team_id")
            task = step.get("task")

            if team_id not in self.teams:
                raise ValueError(f"Team {team_id} not found")

            team = self.teams[team_id]
            task_id = await team.assign_task(task)

            await team.run_task_loop()

            results[step_id] = self.global_memory.get(task_id, {})

        return results

    async def parallel_execute(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute tasks in parallel across teams."""
        results = []

        for task in tasks:
            team_id = task.get("team_id", list(self.teams.keys())[0])
            team = self.teams.get(team_id)
            if team:
                task_id = await team.assign_task(task)
                results.append({"task_id": task_id, "status": "queued"})
            else:
                results.append({"error": f"Team {team_id} not found"})

        return results


class AgentSwarm:
    """Emergent agent swarm for complex problems."""

    def __init__(self, swarm_id: str):
        self.swarm_id = swarm_id
        self.agents: List[BaseAgent] = []
        self.emergent_behaviors: List[Callable] = []
        self.solution_space: List[Dict] = []

    def add_agent(self, agent: BaseAgent):
        """Add agent to swarm."""
        self.agents.append(agent)

    async def explore(self, problem: Dict[str, Any], num_solutions: int = 5) -> List[Dict]:
        """Explore solution space with multiple agents."""
        self.solution_space = []

        tasks = [
            {"problem": problem, "agent_index": i, "variation": i} for i in range(min(num_solutions, len(self.agents)))
        ]

        async def solve_with_agent(agent: BaseAgent, task: Dict) -> Dict:
            result = await agent.process(task)
            return {
                "agent_id": agent.agent_id,
                "solution": result,
                "variation": task.get("variation"),
            }

        solutions = await asyncio.gather(*[solve_with_agent(agent, task) for agent, task in zip(self.agents, tasks)])

        self.solution_space = solutions
        return solutions

    def select_best(self, criteria: Callable[[Dict], float]) -> Dict:
        """Select best solution based on criteria."""
        if not self.solution_space:
            return {}

        return max(self.solution_space, key=criteria)


class ConversationManager:
    """Manage agent conversations and dialogue."""

    def __init__(self, conversation_id: str):
        self.conversation_id = conversation_id
        self.participants: Dict[str, BaseAgent] = {}
        self.dialogue_history: List[Dict] = []
        self.context_window: Dict[str, Any] = {}

    def add_participant(self, agent: BaseAgent, role: str = "participant"):
        """Add agent to conversation."""
        self.participants[agent.agent_id] = {
            "agent": agent,
            "role": role,
            "joined_at": datetime.utcnow().isoformat(),
        }

    async def send_message(
        self,
        sender_id: str,
        recipient_id: str,
        content: str,
        message_type: MessageType = MessageType.MESSAGE,
    ) -> AgentMessage:
        """Send message between agents."""
        message = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender_id=sender_id,
            recipient_id=recipient_id,
            message_type=message_type,
            content={"text": content},
        )

        self.dialogue_history.append(message.to_dict())

        if recipient_id in self.participants:
            recipient = self.participants[recipient_id]["agent"]
            response = await recipient.process(
                {
                    "type": "message",
                    "message": message.to_dict(),
                }
            )
            return response

        return message

    async def broadcast(self, sender_id: str, content: str):
        """Broadcast message to all participants."""
        message = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender_id=sender_id,
            recipient_id="all",
            message_type=MessageType.BROADCAST,
            content={"text": content},
        )

        self.dialogue_history.append(message.to_dict())

    def get_context(self, max_messages: int = 10) -> Dict[str, Any]:
        """Get recent conversation context."""
        return {
            "conversation_id": self.conversation_id,
            "participants": list(self.participants.keys()),
            "history": self.dialogue_history[-max_messages:],
            "context": self.context_window,
        }


class ConsensusBuilder:
    """Build consensus among agents."""

    def __init__(self):
        self.proposals: List[Dict] = []
        self.votes: Dict[str, Dict[str, int]] = {}
        self.threshold: float = 0.67

    def propose(self, proposal_id: str, proposal: Dict[str, Any], proposer_id: str):
        """Submit a proposal."""
        self.proposals.append(
            {
                "id": proposal_id,
                "proposal": proposal,
                "proposer": proposer_id,
                "timestamp": datetime.utcnow().isoformat(),
                "votes_for": 0,
                "votes_against": 0,
                "abstentions": 0,
            }
        )

    def vote(self, agent_id: str, proposal_id: str, vote: str):
        """Cast a vote."""
        if proposal_id not in self.votes:
            self.votes[proposal_id] = {}

        self.votes[proposal_id][agent_id] = vote

        for p in self.proposals:
            if p["id"] == proposal_id:
                if vote == "for":
                    p["votes_for"] += 1
                elif vote == "against":
                    p["votes_against"] += 1
                else:
                    p["abstentions"] += 1

    def check_consensus(self, proposal_id: str) -> bool:
        """Check if consensus is reached."""
        for p in self.proposals:
            if p["id"] == proposal_id:
                total = p["votes_for"] + p["votes_against"] + p["abstentions"]
                if total == 0:
                    return False
                return (p["votes_for"] / total) >= self.threshold
        return False

    def get_result(self, proposal_id: str) -> Dict[str, Any]:
        """Get proposal result."""
        for p in self.proposals:
            if p["id"] == proposal_id:
                consensus = self.check_consensus(proposal_id)
                return {
                    "proposal": p["proposal"],
                    "votes_for": p["votes_for"],
                    "votes_against": p["votes_against"],
                    "abstentions": p["abstentions"],
                    "consensus_reached": consensus,
                }
        return {}


__all__ = [
    "AgentRole",
    "MessageType",
    "AgentMessage",
    "AgentCapability",
    "AgentConfig",
    "BaseAgent",
    "SimpleAgent",
    "TeamLeader",
    "MultiAgentTeam",
    "AgentSwarm",
    "ConversationManager",
    "ConsensusBuilder",
]

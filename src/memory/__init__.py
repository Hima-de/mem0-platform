"""Unified Memory Core - combines hierarchical + importance scoring."""

import asyncio
import hashlib
import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple, Any
from uuid import uuid4

logger = logging.getLogger(__name__)


class MemoryCategory(str, Enum):
    FACT = "fact"
    PREFERENCE = "preference"
    PROCEDURE = "procedure"
    INSIGHT = "insight"
    CONTEXT = "context"


@dataclass
class Memory:
    id: str
    content: str
    category: MemoryCategory
    importance_score: float
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    access_count: int = 0
    last_accessed: Optional[datetime] = None

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "content": self.content,
            "category": self.category.value,
            "importance_score": self.importance_score,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "access_count": self.access_count,
            "last_accessed": self.last_accessed.isoformat() if self.last_accessed else None,
        }


@dataclass
class Session:
    id: str
    user_id: Optional[str]
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Conversation:
    id: str
    session_id: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Message:
    id: str
    conversation_id: str
    role: str
    content: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class MemoryCore:
    """
    Unified memory core with hierarchical storage + importance scoring.

    Features:
    - Hierarchical: Session → Conversation → Message → Memory
    - Importance scoring (0.0-1.0) with decay
    - Full-text search (FTS5 fallback to LIKE)
    - Category-based organization
    - Temporal access patterns
    """

    def __init__(
        self,
        db_path: str = "/tmp/mem0.db",
        embedding_dim: int = 384,
    ):
        self.db_path = db_path
        self.embedding_dim = embedding_dim
        self._init_db()

    @contextmanager
    def _get_conn(self, isolation_level=None):
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=isolation_level)
        try:
            yield conn
            if isolation_level is None:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    created_at TEXT,
                    metadata TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id TEXT PRIMARY KEY,
                    session_id TEXT,
                    created_at TEXT,
                    metadata TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    conversation_id TEXT,
                    role TEXT,
                    content TEXT,
                    created_at TEXT,
                    metadata TEXT,
                    FOREIGN KEY (conversation_id) REFERENCES conversations(id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS memories (
                    id TEXT PRIMARY KEY,
                    session_id TEXT,
                    conversation_id TEXT,
                    message_id TEXT,
                    content TEXT,
                    category TEXT,
                    importance_score REAL,
                    embedding BLOB,
                    metadata TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    access_count INTEGER DEFAULT 0,
                    last_accessed TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id),
                    FOREIGN KEY (conversation_id) REFERENCES conversations(id),
                    FOREIGN KEY (message_id) REFERENCES messages(id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_memories_session ON memories(session_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_memories_category ON memories(category)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_memories_importance ON memories(importance_score)
            """)
            try:
                conn.execute("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
                        content, tokenize='porter'
                    )
                """)
            except sqlite3.OperationalError:
                pass

    async def add_memory(
        self,
        content: str,
        category: MemoryCategory = MemoryCategory.FACT,
        importance_score: float = 0.5,
        session_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        message_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> Memory:
        memory_id = f"mem_{uuid4().hex[:12]}"
        now = datetime.utcnow()

        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO memories 
                (id, session_id, conversation_id, message_id, content, category, 
                 importance_score, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    memory_id,
                    session_id,
                    conversation_id,
                    message_id,
                    content,
                    category.value,
                    importance_score,
                    json.dumps(metadata or {}),
                    now.isoformat(),
                    now.isoformat(),
                ),
            )

        logger.debug(f"Added memory: {memory_id}")
        return Memory(
            id=memory_id,
            content=content,
            category=category,
            importance_score=importance_score,
            metadata=metadata or {},
            created_at=now,
            updated_at=now,
        )

    async def search_memories(
        self,
        query: str,
        session_id: Optional[str] = None,
        category: Optional[MemoryCategory] = None,
        limit: int = 10,
    ) -> List[Memory]:
        with self._get_conn() as conn:
            if session_id:
                cursor = conn.execute(
                    """
                    SELECT * FROM memories 
                    WHERE session_id = ? AND content LIKE ?
                    ORDER BY importance_score DESC, created_at DESC
                    LIMIT ?
                """,
                    (session_id, f"%{query}%", limit),
                )
            elif category:
                cursor = conn.execute(
                    """
                    SELECT * FROM memories 
                    WHERE category = ? AND content LIKE ?
                    ORDER BY importance_score DESC, created_at DESC
                    LIMIT ?
                """,
                    (category.value, f"%{query}%", limit),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT * FROM memories 
                    WHERE content LIKE ?
                    ORDER BY importance_score DESC, created_at DESC
                    LIMIT ?
                """,
                    (f"%{query}%", limit),
                )

            rows = cursor.fetchall()

        memories = []
        for row in rows:
            memories.append(
                Memory(
                    id=row[0],
                    content=row[4],
                    category=MemoryCategory(row[5]),
                    importance_score=row[6],
                    metadata=json.loads(row[8]) if row[8] else {},
                    created_at=datetime.fromisoformat(row[9]),
                    updated_at=datetime.fromisoformat(row[10]),
                    access_count=row[11],
                    last_accessed=datetime.fromisoformat(row[12]) if row[12] else None,
                )
            )
        return memories

    async def get_memory(self, memory_id: str) -> Optional[Memory]:
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,))
            row = cursor.fetchone()

        if not row:
            return None

        return Memory(
            id=row[0],
            content=row[4],
            category=MemoryCategory(row[5]),
            importance_score=row[6],
            metadata=json.loads(row[8]) if row[8] else {},
            created_at=datetime.fromisoformat(row[9]),
            updated_at=datetime.fromisoformat(row[10]),
            access_count=row[11],
            last_accessed=datetime.fromisoformat(row[12]) if row[12] else None,
        )

    async def update_memory(
        self,
        memory_id: str,
        content: Optional[str] = None,
        importance_score: Optional[float] = None,
    ) -> bool:
        updates = []
        params = []

        if content:
            updates.append("content = ?")
            params.append(content)
        if importance_score is not None:
            updates.append("importance_score = ?")
            params.append(importance_score)

        if not updates:
            return False

        updates.append("updated_at = ?")
        params.append(datetime.utcnow().isoformat())
        params.append(memory_id)

        with self._get_conn() as conn:
            conn.execute(
                f"""
                UPDATE memories SET {", ".join(updates)} WHERE id = ?
            """,
                params,
            )
            return conn.rowcount > 0

    async def delete_memory(self, memory_id: str) -> bool:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM memories WHERE id = ?", (memory_id,))
            return conn.rowcount > 0

    async def get_context(
        self,
        session_id: str,
        limit: int = 50,
        min_importance: float = 0.3,
    ) -> str:
        memories = await self.search_memories(query="", session_id=session_id, limit=limit)
        important_memories = [m for m in memories if m.importance_score >= min_importance]
        return "\n".join(
            [f"[{m.category.value}] (importance: {m.importance_score:.2f}) {m.content}" for m in important_memories]
        )

    async def decay_importance(self, decay_factor: float = 0.95) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute(
                """
                UPDATE memories 
                SET importance_score = importance_score * ?,
                    updated_at = ?
                WHERE importance_score > 0.1
            """,
                (decay_factor, datetime.utcnow().isoformat()),
            )
            return cursor.rowcount

    def get_stats(self) -> Dict:
        with self._get_conn() as conn:
            sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            conversations = conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
            memories = conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]
            avg_importance = conn.execute("SELECT AVG(importance_score) FROM memories").fetchone()[0] or 0

        return {
            "sessions": sessions,
            "conversations": conversations,
            "memories": memories,
            "avg_importance": round(avg_importance, 3),
        }

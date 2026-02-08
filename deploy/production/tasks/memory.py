"""
Celery Tasks for Memory Operations.

Provides async processing for:
- Embedding generation
- Importance decay
- Memory categorization
"""

import logging
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name="tasks.memory.generate_embedding",
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    acks_late=True,
)
def generate_embedding(self, memory_id: str, content: str, model: str = "all-MiniLM-L6-v2"):
    """
    Generate embedding for memory content.

    Args:
        memory_id: UUID of the memory
        content: Text content to embed
        model: Sentence transformer model name

    Returns:
        Embedding vector as list
    """
    try:
        from sentence_transformers import SentenceTransformer

        logger.info(f"Generating embedding for memory {memory_id}")

        # Load model (cached globally)
        if not hasattr(generate_embedding, "model"):
            generate_embedding.model = SentenceTransformer(model)

        model = generate_embedding.model

        # Generate embedding
        embedding = model.encode(content, normalize_embeddings=True)

        # Store embedding asynchronously
        store_embedding.delay(memory_id, embedding.tolist(), model)

        return {"memory_id": memory_id, "embedding_dim": len(embedding), "model": model, "status": "success"}

    except SoftTimeLimitExceeded:
        logger.error(f"Embedding generation timeout for memory {memory_id}")
        return {"memory_id": memory_id, "status": "timeout"}
    except Exception as e:
        logger.error(f"Embedding generation failed for {memory_id}: {e}")
        raise


@shared_task(
    bind=True,
    name="tasks.memory.store_embedding",
    acks_late=True,
)
def store_embedding(self, memory_id: str, embedding: list, model: str = "all-MiniLM-L6-v2"):
    """Store embedding in database."""
    try:
        from sqlalchemy import text
        from database import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO embeddings (memory_id, model_name, embedding)
                    VALUES (:memory_id, :model, :embedding)
                    ON CONFLICT (memory_id) DO UPDATE
                    SET embedding = :embedding, model_name = :model
                """),
                {"memory_id": memory_id, "model": model, "embedding": embedding},
            )

        logger.info(f"Stored embedding for memory {memory_id}")
        return {"memory_id": memory_id, "status": "stored"}

    except Exception as e:
        logger.error(f"Failed to store embedding for {memory_id}: {e}")
        raise


@shared_task(
    bind=True,
    name="tasks.memory.decay_importance",
    time_limit=3600,
    soft_time_limit=3300,
)
def decay_importance(self, decay_factor: float = 0.95, min_importance: float = 0.1):
    """
    Decay importance scores for all memories.
    Run daily to gradually reduce importance of older memories.

    Args:
        decay_factor: Multiply importance by this factor (0.95 = 5% decay)
        min_importance: Minimum importance threshold

    Returns:
        Number of memories updated
    """
    try:
        from sqlalchemy import text
        from database import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    UPDATE memories
                    SET importance_score = GREATEST(
                        importance_score * :decay_factor,
                        :min_importance
                    ),
                        updated_at = NOW()
                    WHERE status = 'active'
                    AND importance_score > :min_importance
                    AND created_at < NOW() - INTERVAL '7 days'
                """),
                {"decay_factor": decay_factor, "min_importance": min_importance},
            )

            affected = result.rowcount

        logger.info(f"Decayed importance for {affected} memories")
        return {"affected": affected, "status": "success"}

    except Exception as e:
        logger.error(f"Importance decay failed: {e}")
        raise


@shared_task(
    bind=True,
    name="tasks.memory.categorize_memory",
    max_retries=2,
)
def categorize_memory(self, memory_id: str, content: str):
    """
    Automatically categorize memory using ML.

    Categories:
        - fact: Objective statements
        - preference: User preferences
        - procedure: How-to information
        - insight: Conclusions or realizations
        - context: Situational information
    """
    try:
        import re

        # Simple keyword-based categorization
        # In production, use ML classifier

        category_keywords = {
            "preference": ["prefer", "like", "dislike", "love", "hate", "favorite", "enjoy"],
            "procedure": ["how to", "steps", "instructions", "always", "never", "remember to"],
            "insight": ["realized", "discovered", "found out", "conclusion", "understand"],
            "context": ["when", "where", "situation", "context", "meeting", "project"],
        }

        content_lower = content.lower()

        # Check for preference
        if any(kw in content_lower for kw in category_keywords["preference"]):
            category = "preference"
        # Check for procedure
        elif any(kw in content_lower for kw in category_keywords["procedure"]):
            category = "procedure"
        # Check for insight
        elif any(kw in content_lower for kw in category_keywords["insight"]):
            category = "insight"
        # Check for context
        elif any(kw in content_lower for kw in category_keywords["context"]):
            category = "context"
        else:
            category = "fact"

        # Update memory category
        from sqlalchemy import text
        from database import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE memories
                    SET category = :category, updated_at = NOW()
                    WHERE id = :memory_id
                """),
                {"memory_id": memory_id, "category": category},
            )

        logger.info(f"Categorized memory {memory_id} as {category}")
        return {"memory_id": memory_id, "category": category, "status": "success"}

    except Exception as e:
        logger.error(f"Memory categorization failed for {memory_id}: {e}")
        raise


@shared_task(
    bind=True,
    name="tasks.memory.consolidate_memories",
    time_limit=1800,
)
def consolidate_memories(self, user_id: str, session_id: str):
    """
    Consolidate memories from a session into higher-level insights.

    Uses clustering to find related memories and create summaries.
    """
    try:
        from sqlalchemy import text
        from database import get_engine
        from collections import defaultdict

        engine = get_engine()

        # Fetch memories from session
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id, content, category, importance_score
                    FROM memories
                    WHERE user_id = :user_id
                    AND session_id = :session_id
                    AND status = 'active'
                    ORDER BY created_at
                """),
                {"user_id": user_id, "session_id": session_id},
            )

            memories = [
                {"id": row[0], "content": row[1], "category": row[2], "importance": row[3]} for row in result.fetchall()
            ]

        if len(memories) < 3:
            return {"status": "skipped", "reason": "insufficient_memories"}

        # Group by category
        category_groups = defaultdict(list)
        for memory in memories:
            category_groups[memory["category"]].append(memory)

        # Generate consolidation insights
        insights = []
        for category, group in category_groups.items():
            if len(group) >= 2:
                # Create insight from multiple memories of same category
                combined_content = " ".join(m["content"] for m in group)
                avg_importance = sum(m["importance"] for m in group) / len(group)

                # Create consolidated memory
                consolidated = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "content": f"Consolidated from {len(group)} {category} memories",
                    "category": category,
                    "importance": min(avg_importance + 0.1, 1.0),
                    "metadata": {
                        "consolidated": True,
                        "source_memory_ids": [m["id"] for m in group],
                        "combined_length": len(combined_content),
                    },
                }
                insights.append(consolidated)

        # Store consolidated memories
        if insights:
            with engine.connect() as conn:
                for insight in insights:
                    conn.execute(
                        text("""
                            INSERT INTO memories
                            (user_id, session_id, content, category, importance_score, metadata)
                            VALUES (:user_id, :session_id, :content, :category, :importance, :metadata)
                        """),
                        insight,
                    )

        logger.info(f"Consolidated {len(insights)} insights for session {session_id}")
        return {"status": "success", "consolidated": len(insights), "user_id": user_id, "session_id": session_id}

    except Exception as e:
        logger.error(f"Memory consolidation failed: {e}")
        raise


@shared_task(
    bind=True,
    name="tasks.memory.find_similar",
)
def find_similar_memories(self, memory_id: str, user_id: str, threshold: float = 0.8, limit: int = 10):
    """
    Find similar memories using vector similarity.

    Args:
        memory_id: Source memory
        user_id: User context
        threshold: Similarity threshold (0-1)
        limit: Maximum results

    Returns:
        List of similar memories with similarity scores
    """
    try:
        from sqlalchemy import text
        from database import get_engine
        from numpy.linalg import norm

        engine = get_engine()

        # Get source embedding
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT embedding FROM embeddings WHERE memory_id = :memory_id
                """),
                {"memory_id": memory_id},
            )
            row = result.fetchone()

            if not row:
                return {"status": "error", "reason": "embedding_not_found"}

            source_embedding = np.array(row[0])

        # Find similar embeddings using cosine similarity
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT e.memory_id, m.content, m.category, m.importance_score,
                           1 - (e.embedding <=> :embedding) as similarity
                    FROM embeddings e
                    JOIN memories m ON e.memory_id = m.id
                    WHERE m.user_id = :user_id
                    AND m.status = 'active'
                    AND e.memory_id != :memory_id
                    AND m.created_at > NOW() - INTERVAL '90 days'
                    ORDER BY similarity DESC
                    LIMIT :limit
                """),
                {"user_id": user_id, "memory_id": memory_id, "embedding": source_embedding.tolist(), "limit": limit},
            )

            similar = [
                {
                    "memory_id": row[0],
                    "content": row[1],
                    "category": row[2],
                    "importance": float(row[3]),
                    "similarity": float(row[4]),
                }
                for row in result.fetchall()
                if row[4] >= threshold
            ]

        logger.info(f"Found {len(similar)} similar memories for {memory_id}")
        return {"status": "success", "similar": similar}

    except Exception as e:
        logger.error(f"Similar memory search failed: {e}")
        raise

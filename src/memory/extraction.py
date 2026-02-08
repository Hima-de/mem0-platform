"""Memory Extraction - Extract memories from code execution."""

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from memory import MemoryCategory

logger = logging.getLogger(__name__)


@dataclass
class ExtractedMemory:
    content: str
    category: MemoryCategory
    importance_score: float
    source: str


PATTERNS = {
    MemoryCategory.FACT: [
        r"(?:fact|truth|actually|really|i learned|i found out)",
        r"(?:fact:|truth:|note:)",
    ],
    MemoryCategory.PREFERENCE: [
        r"(?:i prefer|i like|i love|hate|dislike|best|worst)",
        r"(?:preference:|likes:|dislikes:)",
    ],
    MemoryCategory.PROCEDURE: [
        r"(?:how to|steps to|workflow|process|remember to|always|never)",
        r"(?:procedure:|instructions:|guide:)",
    ],
    MemoryCategory.INSIGHT: [
        r"(?:insight:|realization:|discovered|figured out|noticed)",
        r"(?:interesting:|surprising:|unexpected:)",
    ],
    MemoryCategory.CONTEXT: [
        r"(?:context:|background:|situation:|scenario:)",
        r"(?:currently|i am|i'm working|i'm building)",
    ],
}


class MemoryExtractor:
    """Extract structured memories from code/text."""

    def __init__(self, default_importance: float = 0.5):
        self.default_importance = default_importance

    async def extract_from_text(self, text: str) -> List[ExtractedMemory]:
        """Extract memories from plain text."""
        memories = []

        for category, patterns in PATTERNS.items():
            for pattern in patterns:
                matches = re.findall(f".*?({pattern}.*)", text, re.IGNORECASE)
                for match in matches:
                    memories.append(
                        ExtractedMemory(
                            content=match.strip(),
                            category=category,
                            importance_score=self._calculate_importance(match, category),
                            source="text",
                        )
                    )

        return memories

    async def extract_from_code(self, code: str) -> List[ExtractedMemory]:
        """Extract memories from code comments/docstrings."""
        memories = []

        docstring_pattern = r'"""([\s\S]*?)"""'
        docstrings = re.findall(docstring_pattern, code)

        comment_pattern = r"# (.+)"
        comments = re.findall(comment_pattern, code)

        all_text = " ".join(docstrings + comments)

        memories.extend(await self.extract_from_text(all_text))

        return memories

    async def extract_from_execution(
        self,
        stdout: str,
        stderr: str,
    ) -> List[ExtractedMemory]:
        """Extract memories from execution output."""
        memories = []

        memory_pattern = r"\[MEMORY\]\s*(.+)"
        found = re.findall(memory_pattern, stdout + stderr)

        for match in found:
            try:
                data = json.loads(match)
                memories.append(
                    ExtractedMemory(
                        content=data.get("content", match),
                        category=MemoryCategory(data.get("category", "fact")),
                        importance_score=data.get("importance", self.default_importance),
                        source="execution",
                    )
                )
            except json.JSONDecodeError:
                memories.append(
                    ExtractedMemory(
                        content=match,
                        category=MemoryCategory.FACT,
                        importance_score=self.default_importance,
                        source="execution",
                    )
                )

        return memories

    def _calculate_importance(self, text: str, category: MemoryCategory) -> float:
        base = self.default_importance

        if category == MemoryCategory.PREFERENCE:
            if any(w in text.lower() for w in ["love", "hate", "best", "worst"]):
                base += 0.2
        elif category == MemoryCategory.PROCEDURE:
            if "always" in text.lower() or "never" in text.lower():
                base += 0.1
        elif category == MemoryCategory.INSIGHT:
            if "discovered" in text.lower() or "figured out" in text.lower():
                base += 0.1

        return min(1.0, max(0.0, base))

    async def categorize_text(self, text: str) -> MemoryCategory:
        """Auto-categorize text without explicit markers."""
        scores = {cat: 0 for cat in MemoryCategory}

        for category, patterns in PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    scores[category] += 1

        if max(scores.values()) > 0:
            return max(scores, key=scores.get)
        return MemoryCategory.FACT


async def create_extractor(default_importance: float = 0.5) -> MemoryExtractor:
    """Factory for memory extractor."""
    return MemoryExtractor(default_importance)

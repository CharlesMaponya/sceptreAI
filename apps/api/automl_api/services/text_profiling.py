from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from typing import Any

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9']+")

# Keep the cloud focused on terms that describe the dataset instead of grammar,
# URL fragments, and common social-media noise.
STOP_WORDS = frozenset(
    {
        "about",
        "after",
        "again",
        "against",
        "all",
        "also",
        "amp",
        "and",
        "any",
        "are",
        "because",
        "been",
        "before",
        "being",
        "but",
        "can",
        "com",
        "could",
        "did",
        "does",
        "doing",
        "don",
        "for",
        "from",
        "had",
        "has",
        "have",
        "her",
        "here",
        "hers",
        "him",
        "his",
        "how",
        "http",
        "https",
        "into",
        "its",
        "just",
        "more",
        "most",
        "not",
        "now",
        "our",
        "out",
        "over",
        "she",
        "should",
        "some",
        "than",
        "that",
        "the",
        "their",
        "them",
        "then",
        "there",
        "these",
        "they",
        "this",
        "those",
        "through",
        "too",
        "under",
        "very",
        "was",
        "were",
        "what",
        "when",
        "where",
        "which",
        "while",
        "who",
        "why",
        "will",
        "with",
        "would",
        "www",
        "you",
        "your",
    }
)


def text_word_counter(
    values: Iterable[Any],
    *,
    maximum_terms: int | None = None,
) -> Counter[str]:
    counts = Counter(
        token
        for value in values
        for token in (match.lower() for match in TOKEN_PATTERN.findall(str(value)))
        if token not in STOP_WORDS and not token.isdigit()
    )
    if maximum_terms is None or len(counts) <= maximum_terms:
        return counts
    return Counter(dict(counts.most_common(maximum_terms)))


def word_frequencies(
    values: Iterable[Any],
    *,
    limit: int = 40,
) -> list[dict[str, int | str]]:
    return [
        {"word": word, "count": count}
        for word, count in text_word_counter(values).most_common(limit)
    ]

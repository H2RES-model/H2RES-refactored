"""Iterable helpers shared across loaders."""

from __future__ import annotations

from typing import Iterable, List


def union_lists(*lists: Iterable[str]) -> List[str]:
    """Union lists while preserving first-seen order.

    Args:
        *lists: Iterables of strings to merge.

    Returns:
        A list containing unique items in first-seen order.
    """
    merged: List[str] = []
    seen = set()
    for lst in lists:
        for item in lst or []:
            if item not in seen:
                seen.add(item)
                merged.append(item)
    return merged

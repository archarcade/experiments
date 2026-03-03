#!/usr/bin/env python3
"""
Simple file-based workload cache.
"""

from __future__ import annotations

import hashlib
import pickle
from pathlib import Path

from .workload_generator import Workload


class WorkloadCache:
    def __init__(self, cache_dir: str = "./experiments/.workload_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def hash_text(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Workload | None:
        path = self.cache_dir / f"{key}.pkl"
        if not path.exists():
            return None
        with path.open("rb") as f:
            return pickle.load(f)

    def put(self, key: str, workload: Workload) -> None:
        path = self.cache_dir / f"{key}.pkl"
        with path.open("wb") as f:
            pickle.dump(workload, f)

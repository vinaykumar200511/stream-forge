"""Persistent local state store for stateful stream processing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class LocalStateStore:
    """A small, filesystem-backed dictionary used to persist worker-local state."""

    def __init__(self, path: str | Path, filename: str = "state_store.json") -> None:
        self._path = Path(path)
        if self._path.suffix:
            self._file_path = self._path
        else:
            self._path.mkdir(parents=True, exist_ok=True)
            self._file_path = self._path / filename

        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if not self._file_path.exists():
            return {}
        try:
            with self._file_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return data if isinstance(data, dict) else {}

    def _persist(self) -> None:
        try:
            with self._file_path.open("w", encoding="utf-8") as handle:
                json.dump(self._state, handle, sort_keys=True)
        except OSError:
            pass

    def get(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._state[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._state[key] = value
        self._persist()

    def __delitem__(self, key: str) -> None:
        del self._state[key]
        self._persist()

    def __contains__(self, key: str) -> bool:
        return key in self._state

    def __iter__(self):
        return iter(self._state)

    def __len__(self) -> int:
        return len(self._state)

    def __repr__(self) -> str:
        return f"LocalStateStore(path={self._file_path}, entries={len(self._state)})"

    def keys(self):
        return self._state.keys()

    def items(self):
        return self._state.items()

    def values(self):
        return self._state.values()

    def update(self, other: dict[str, Any]) -> None:
        self._state.update(other)
        self._persist()

    def clear(self) -> None:
        self._state.clear()
        self._persist()

    def current(self, key: str | None = None) -> Any:
        if key is None:
            return self._state.copy()
        return self._state.get(key)

    @property
    def state(self) -> dict[str, Any]:
        return self._state.copy()

    def snapshot(self) -> dict[str, Any]:
        return self._state.copy()


__all__ = ["LocalStateStore"]

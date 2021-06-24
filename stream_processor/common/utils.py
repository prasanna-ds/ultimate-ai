from __future__ import annotations

from typing import Any, Type, TypeVar


T = TypeVar("T", bound="Singleton")


class Singleton(type):
    _instances: dict = {}

    def __call__(cls: T, *args, **kwargs) -> T:  # type: ignore
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]  # type: ignore


class Cache(metaclass=Singleton):
    class KeyNotFoundError(Exception):
        pass

    def __init__(self) -> None:
        self._cache: dict = {}

    def set(self, key: str, value: int) -> None:
        self._cache[key] = value

    def _key_exists(self, key: str) -> bool:
        if self._cache[key]:
            return True
        return False

    def get(self, key: str) -> int:
        if not self._key_exists(key):
            raise Cache.KeyNotFoundError(f"Key {key} not found in Cache")
        return int(self._cache[key])

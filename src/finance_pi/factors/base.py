from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Protocol

import polars as pl


class FactorContext(Protocol):
    def scan(self, name: str) -> pl.LazyFrame:
        ...


class Factor(ABC):
    requires: ClassVar[list[str]] = []
    rebalance: ClassVar[str] = "monthly"

    @abstractmethod
    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """Return columns: date, security_id, score."""


class FactorRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, type[Factor]] = {}

    def register(self, name: str):
        def decorator(cls: type[Factor]) -> type[Factor]:
            if name in self._registry:
                raise KeyError(f"factor {name!r} is already registered")
            self._registry[name] = cls
            return cls

        return decorator

    def get(self, name: str) -> type[Factor]:
        try:
            return self._registry[name]
        except KeyError as exc:
            available = ", ".join(sorted(self._registry))
            raise KeyError(f"unknown factor {name!r}; available: {available}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._registry))


factor_registry = FactorRegistry()

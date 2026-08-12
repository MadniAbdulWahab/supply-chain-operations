from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_SEED = 20260812
DEFAULT_SHIPMENT_COUNT = 250_000


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def state(self) -> Path:
        return self.root / "state"

    @property
    def evidence(self) -> Path:
        return self.root / "evidence"

    @property
    def database(self) -> Path:
        return self.state / "lakehouse.duckdb"


def default_paths() -> ProjectPaths:
    return ProjectPaths(Path("data"))

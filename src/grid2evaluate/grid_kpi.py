from abc import ABC, abstractmethod
from pathlib import Path


class GridKpi(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def evaluate(self, directory: Path) -> list[float]:
        pass

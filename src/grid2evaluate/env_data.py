import json
from pathlib import Path


class EnvData:
    def __init__(self, json: dict):
        self._json = json

    @staticmethod
    def load(directory: Path) -> 'EnvData':
        with open(directory / "env.json", "r", encoding="utf-8") as f:
            return EnvData(json.load(f))

    @property
    def json(self) -> dict:
        return self._json

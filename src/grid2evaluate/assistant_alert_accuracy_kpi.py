from pathlib import Path

from grid2evaluate.grid_kpi import GridKpi


class AssistantAlertAccuracyKpi(GridKpi):
    def __init__(self):
        super().__init__("Assistant alert accuracy")

    def evaluate(self, directory: Path) -> list[float]:
        # TODO
        return [0]

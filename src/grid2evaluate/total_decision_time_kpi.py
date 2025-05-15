from pathlib import Path

from grid2evaluate.grid_kpi import GridKpi


class TotalDecisionTimeKpi(GridKpi):
    def __init__(self):
        super().__init__("Total decision time")

    def evaluate(self, directory: Path) -> list[float]:
        # TODO
        return [0]

from pathlib import Path
from statistics import mean

import pyarrow.parquet as pq

from grid2evaluate.actions import Actions
from grid2evaluate.env_data import EnvData
from grid2evaluate.grid_kpi import GridKpi


class TopologicalActionComplexityKpi(GridKpi):
    def __init__(self):
        super().__init__("Topological action complexity")

    @staticmethod
    def get_connected_buses(directory: Path) -> list[int]:
        # we can get it for unique pairs of (substation_num, local_bus_num) for both ends of lines
        line_table = pq.read_table(directory / 'line.parquet')
        line_or_bus_table = pq.read_table(directory / 'line_or_bus.parquet')
        line_ex_bus_table = pq.read_table(directory / 'line_ex_bus.parquet')
        time_col = line_or_bus_table['time']
        connected_buses = [set()] * len(time_col)
        for row in line_table.to_pandas().itertuples():
            line_or_bus_col = line_or_bus_table[row.name]
            line_ex_bus_col = line_ex_bus_table[row.name]
            for time_index, (line_or_bus, line_ex_bus) in enumerate(zip(line_or_bus_col, line_ex_bus_col)):
                if line_or_bus != -1:
                    connected_buses[time_index].add((row.line_or_to_subid, line_or_bus.as_py()))
                if line_ex_bus != -1:
                    connected_buses[time_index].add((row.line_ex_to_subid, line_ex_bus.as_py()))
        return [len(connected_buses) for connected_buses in connected_buses]

    def evaluate(self, directory: Path) -> list[float]:
        # step 1 and 2: get topo actions for each step
        action_table = pq.read_table(directory / 'actions.parquet')
        topo_actions = Actions.load(action_table).filter_topo_actions()
        n_topo = [len(acts) for acts in topo_actions]

        # step 3
        min_topo = min(count for count in n_topo)

        # step 4
        max_topo = max(count for count in n_topo)

        # step 5
        avg_topo = mean(count for count in n_topo)

        # step 6
        n_connected_buses = self.get_connected_buses(directory)

        # step 7
        delta_connected_bus = [0] + [n_connected_buses[i] - n_connected_buses[i - 1]
                                              for i in range(1, len(n_connected_buses))]

        # step 8
        env_data = EnvData.load(directory)
        n_max_bus = env_data.json["n_sub"]  * env_data.json["n_busbar_per_sub"]

        # step 9
        min_bus = min(delta * 100 / n_max_bus for delta in delta_connected_bus)

        # step 10
        max_bus = max(delta * 100 / n_max_bus for delta in delta_connected_bus)

        # step 11
        avg_bus = mean(delta * 100 / n_max_bus for delta in delta_connected_bus)

        # step 12
        return [min_topo, max_topo, avg_topo, min_bus, max_bus, avg_bus]

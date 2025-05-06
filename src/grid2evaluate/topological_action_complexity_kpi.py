import json
from pathlib import Path
from statistics import mean

import pyarrow.parquet as pq

from grid2evaluate.grid_kpi import GridKpi


class TopologicalActionComplexityKpi(GridKpi):
    def __init__(self):
        super().__init__("Topological action complexity")

    @staticmethod
    def filter_action(actions: dict) -> dict:
        only_topo_actions = {}
        for action_type in ['set_bus',
                            'change_bus']:
            if action_type in actions:
                if 'lines_or_id' in actions[action_type] or 'lines_ex_id' in actions[action_type]:
                    only_topo_actions[action_type] = actions[action_type]
        for action_type in ['line_or_set_bus',
                            'line_ex_set_bus',
                            'line_or_change_bus',
                            'line_ex_change_bus',
                            'line_set_status',
                            'line_change_status']:
            if action_type in actions:
                only_topo_actions[action_type] = actions[action_type]
        return only_topo_actions

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
        action_table = pq.read_table(directory / 'actions.parquet')
        time_col = action_table['time']

        # get topo actions for each step
        n_topo = [0] * len(time_col)
        for (i, row) in enumerate(action_table.to_pandas().itertuples()):
            actions = json.loads(row.action)
            only_topo_actions = self.filter_action(actions)
            n_topo[i] = len(only_topo_actions)

        min_topo = min(count for count in n_topo)
        max_topo = max(count for count in n_topo)
        avg_topo = mean(count for count in n_topo)

        n_connected_buses = self.get_connected_buses(directory)

        delta_connected_bus = [0] + [n_connected_buses[i] - n_connected_buses[i - 1]
                                              for i in range(1, len(n_connected_buses))]

        with open(directory / "env.json", "r", encoding="utf-8") as f:
            env_data = json.load(f)
        n_max_bus = env_data["n_sub"]  * env_data["n_busbar_per_sub"]

        min_bus = min(delta * 100 / n_max_bus for delta in delta_connected_bus)
        max_bus = max(delta * 100 / n_max_bus for delta in delta_connected_bus)
        avg_bus = mean(delta * 100 / n_max_bus for delta in delta_connected_bus)

        return [min_topo, max_topo, avg_topo, min_bus, max_bus, avg_bus]

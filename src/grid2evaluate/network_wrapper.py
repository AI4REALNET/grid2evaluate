# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

import glob
from pathlib import Path

import pandapower as pdp
import pandas as pd
import pypowsybl as pp


class NetworkWrapper:
    def __init__(self, network: pp.network.Network):
        self._network = network

    @property
    def network(self) -> pp.network.Network:
        return self._network

    @staticmethod
    def _convert_to_bus_breaker_topo(network: pp.network.Network):
        voltage_levels = network.get_voltage_levels(attributes=['topology_kind'])
        network.update_voltage_levels(id=voltage_levels.index.tolist(),
                                      topology_kind=['BUS_BREAKER'] * len(voltage_levels))

    @staticmethod
    def _get_numbered_buses(network: pp.network.Network):
        buses = network.get_bus_breaker_view_buses(attributes=['voltage_level_id'])
        voltage_levels = network.get_voltage_levels(attributes=[])
        numbered_buses = buses.merge(voltage_levels.rename(columns=lambda x: x + '_voltage_level'),
                                     left_on='voltage_level_id', right_index=True, how='outer')
        numbered_buses['local_num'] = numbered_buses.groupby('voltage_level_id').cumcount()
        return numbered_buses

    @classmethod
    def _create_extra_buses(cls, network: pp.network.Network, n_busbar_per_sub: int):
        buses = cls._get_numbered_buses(network)
        bus_count_by_voltage_level = buses.groupby('voltage_level_id')['local_num'].max().reset_index()
        for _, row in bus_count_by_voltage_level.iterrows():
            voltage_level_id = row['voltage_level_id']
            bus_count = row['local_num'] + 1
            bus_nums_to_create = range(bus_count, n_busbar_per_sub)
            bus_ids = [f"{voltage_level_id}_extra_busbar_{i}" for i in bus_nums_to_create]
            voltage_level_ids = [voltage_level_id] * len(bus_nums_to_create)
            network.create_buses(id=bus_ids, voltage_level_id=voltage_level_ids)

    @classmethod
    def load(cls, directory: Path, n_busbar_per_sub: int) -> 'NetworkWrapper':
        grid_paths = glob.glob(str(directory / "grid.*"))
        grid_path = grid_paths[0]
        if grid_path.endswith('.json'):
            n_pdp = pdp.from_json(grid_path)
            network = pp.network.convert_from_pandapower(n_pdp)
        else:
            network = pp.network.load(grid_path)

        # we need to convert to bus breaker topo to apply Grid2op style topology
        cls._convert_to_bus_breaker_topo(network)

        # also, to apply Grid2op topology, we need to reach the n_busbar_per_sub buses for each voltage level
        cls._create_extra_buses(network, n_busbar_per_sub)

        return NetworkWrapper(network)

    @classmethod
    def _fill_bus_id_and_connected(cls, name: str, bus_local_num: int, bus_id: list[str], connected: list[bool],
                                   elements: pd.DataFrame, buses: pd.DataFrame, voltage_level_id_attr: str):
        if bus_local_num == -1:
            bus_id.append("")
            connected.append(False)
        else:
            voltage_level_id = cls._get_voltage_level_id_from_name(elements, name, voltage_level_id_attr)
            local_bus_id = buses[(buses['voltage_level_id'] == voltage_level_id) & (buses['local_num'] == bus_local_num - 1)].iloc[0].name
            bus_id.append(local_bus_id)
            connected.append(True)

    @staticmethod
    def get_id_from_name(elements: pd.DataFrame, name: str) -> str:
        found_element = elements[(elements['name'] == name)]
        if found_element.empty:
            filtered = elements[elements.index == name]
            if filtered.empty:
                return ""
            else:
                return name  
        return found_element.iloc[0].name

    @staticmethod
    def _get_voltage_level_id_from_name(elements: pd.DataFrame, name: str, voltage_level_id_attr: str) -> str:
        return elements[(elements['name'] == name)].iloc[0][voltage_level_id_attr]

    def _update_loads(self, load_table, load_p, load_q, load_bus, time_index: int, loads: pd.DataFrame, buses: pd.DataFrame):
        id = []
        p0 = []
        q0 = []
        bus_id = []
        connected = []
        for name, p_col, q_col, bus_col in zip(load_table['name'], load_p.columns[1:], load_q.columns[1:], load_bus.columns[1:]):
            id.append(self.get_id_from_name(loads, name.as_py()))
            p0.append(p_col[time_index].as_py())
            q0.append(q_col[time_index].as_py())
            self._fill_bus_id_and_connected(name=name.as_py(), bus_local_num=bus_col[time_index].as_py(), bus_id=bus_id,
                                            connected=connected, elements=loads, buses=buses, voltage_level_id_attr='voltage_level_id')
        self._network.update_loads(id=id, p0=p0, q0=q0, bus_breaker_bus_id=bus_id, connected=connected)

    def _update_generators(self, gen_table, gen_p, gen_v, gen_bus, time_index: int, generators: pd.DataFrame, buses: pd.DataFrame):
        id = []
        target_p = []
        target_v = []
        voltage_regulator_on = []
        bus_id = []
        connected = []
        for name, p_col, v_col, bus_col in zip(gen_table['name'], gen_p.columns[1:], gen_v.columns[1:], gen_bus.columns[1:]):
            id.append(self.get_id_from_name(generators, name.as_py()))
            target_p.append(p_col[time_index].as_py())
            v = v_col[time_index].as_py()
            voltage_regulator_on.append(False if v <= 0 else True)
            target_v.append(v)
            self._fill_bus_id_and_connected(name=name.as_py(), bus_local_num=bus_col[time_index].as_py(), bus_id=bus_id,
                                            connected=connected, elements=generators, buses=buses, voltage_level_id_attr='voltage_level_id')
        self._network.update_generators(id=id, target_p=target_p, voltage_regulator_on=voltage_regulator_on, target_v=target_v, bus_breaker_bus_id=bus_id, connected=connected)

    def _update_batteries(self, storage_table, storage_power, storage_bus, time_index: int, batteries: pd.DataFrame, buses: pd.DataFrame):
        id = []
        target_p = []
        target_q = []
        bus_id = []
        connected = []
        for name, p_col, bus_col in zip(storage_table['name'], storage_power.columns[1:], storage_bus.columns[1:]):
            id.append(self.get_id_from_name(batteries, name.as_py()))
            target_p.append(p_col[time_index].as_py())
            target_q.append(0.0)
            self._fill_bus_id_and_connected(name=name.as_py(), bus_local_num=bus_col[time_index].as_py(), bus_id=bus_id,
                                            connected=connected, elements=batteries, buses=buses, voltage_level_id_attr='voltage_level_id')
        self._network.update_batteries(id=id, target_p=target_p, target_q=target_q, bus_breaker_bus_id=bus_id, connected=connected)

    def _update_lines(self, line_table, line_or_bus, line_ex_bus, time_index: int, branches: pd.DataFrame, buses: pd.DataFrame):
        id = []
        bus1_id = []
        connected1 = []
        bus2_id = []
        connected2 = []
        for name, bus1_col, bus2_col in zip(line_table['name'], line_or_bus.columns[1:], line_ex_bus.columns[1:]):
            id.append(self.get_id_from_name(branches, name.as_py()))
            self._fill_bus_id_and_connected(name=name.as_py(), bus_local_num=bus1_col[time_index].as_py(), bus_id=bus1_id,
                                            connected=connected1, elements=branches, buses=buses, voltage_level_id_attr='voltage_level1_id')
            self._fill_bus_id_and_connected(name=name.as_py(), bus_local_num=bus2_col[time_index].as_py(), bus_id=bus2_id,
                                            connected=connected2, elements=branches, buses=buses, voltage_level_id_attr='voltage_level2_id')
        self._network.update_branches(id=id, bus_breaker_bus1_id=bus1_id, connected1=connected1, bus_breaker_bus2_id=bus2_id, connected2=connected2)

    def get_branches(self, attributes: list[str]) -> pd.DataFrame:
        # TODO waiting to a fix on pypowsybl to be able to get name attribute with network.get_branches(attributes=['name', 'voltage_level1_id', 'voltage_level2_id'])
        lines = self._network.get_lines(attributes=attributes)
        transfos = self._network.get_2_windings_transformers(attributes=attributes)
        return pd.concat([lines, transfos])

    def update_network(self,
                       load_table, load_p, load_q, load_bus,
                       gen_table, gen_p, gen_q, gen_bus,
                       storage_table, storage_power, storage_bus,
                       line_table, line_or_bus, line_ex_bus,
                       time_index: int):
        loads = self._network.get_loads(attributes=['name', 'voltage_level_id'])
        generators = self._network.get_generators(attributes=['name', 'voltage_level_id'])
        batteries = self._network.get_batteries(attributes=['name', 'voltage_level_id'])
        branches = self.get_branches(attributes=['name', 'voltage_level1_id', 'voltage_level2_id'])
        buses = self._get_numbered_buses(self._network)
        if (len(loads) > 0):
            self._update_loads(load_table, load_p, load_q, load_bus, time_index, loads, buses)
        if (len(generators) > 0):
            self._update_generators(gen_table, gen_p, gen_q, gen_bus, time_index, generators, buses)
        if (len(batteries) > 0):
            self._update_batteries(storage_table, storage_power, storage_bus, time_index, batteries, buses)
        if (len(branches) > 0):
            self._update_lines(line_table, line_or_bus, line_ex_bus, time_index, branches, buses)

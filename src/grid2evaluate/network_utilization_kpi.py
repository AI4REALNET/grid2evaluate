import logging
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pypowsybl as pp

from grid2evaluate.env_data import EnvData
from grid2evaluate.grid_kpi import GridKpi
from grid2evaluate.network_wrapper import NetworkWrapper

logger = logging.getLogger(__name__)

class NetworkUtilizationKpi(GridKpi):
    def __init__(self):
        super().__init__("Network utilization")

    @staticmethod
    def calculate_rho(line_rho) -> np.ndarray:
        rho = np.zeros((len(line_rho[0]), len(line_rho.columns) - 1))
        for branch_index, rho_col in enumerate(line_rho.columns[1:]):
            for time_index, rho_value in enumerate(rho_col):
                rho[time_index, branch_index] = rho_value.as_py()
        return rho

    @staticmethod
    def run_security_analysis(network_wrapper: NetworkWrapper,
                              contingency_ids: list[str],
                              monitored_element_ids: list[str],
                              time_col, done_col,
                              load_table, load_p, load_q, load_bus,
                              gen_table, gen_p, gen_v, gen_bus,
                              storage_table, storage_power, storage_bus,
                              line_table, line_or_bus, line_ex_bus) -> tuple[list[dict], int, int]:
        parameters = pp.loadflow.Parameters(voltage_init_mode=pp.loadflow.VoltageInitMode.DC_VALUES)
        analysis = pp.security.create_analysis()
        analysis.add_single_element_contingencies(contingency_ids)
        analysis.add_monitored_elements(branch_ids=monitored_element_ids)
        flows = [{} for _ in range(len(time_col))]
        n_div = 0
        n1_div = 0
        for time_index in range(len(time_col)):
            if done_col[time_index]:
                continue

            network_wrapper.update_network(load_table, load_p, load_q, load_bus,
                                           gen_table, gen_p, gen_v, gen_bus,
                                           storage_table, storage_power, storage_bus,
                                           line_table, line_or_bus, line_ex_bus,
                                           time_index)

            result = analysis.run_ac(network_wrapper.network, parameters)

            # TODO what should be done in case of divergence on N and N-1 states ?
            if result.pre_contingency_result.status == pp.loadflow.ComponentStatus.CONVERGED:
                for contingency_id, post_contingency_result in result.post_contingency_results.items():
                    if post_contingency_result.status != pp.security.ComputationStatus.CONVERGED:
                        n1_div += 1
                        logger.warning(
                            f"Calculation failed at time {time_index} and contingency '{contingency_id}' with {post_contingency_result.status}")
            else:
                logger.warning(f"Calculation failed at time {time_index} with {result.pre_contingency_result.status}")
                n_div += 1

            for (contingency_id, _, branch_id), row in result.branch_results.iterrows():
                flows[time_index][(contingency_id, branch_id)] = (row.i1, row.i2)
        return flows, n_div, n1_div

    @staticmethod
    def compute_rho_n1(network_wrapper: NetworkWrapper,
                       contingency_ids: list[str],
                       monitored_element_ids: list[str],
                       security_analysis_flows: list[dict],
                       time_col, line_table, line_thermal_limit) -> np.ndarray:
        rho = np.zeros((len(time_col), len(contingency_ids), len(monitored_element_ids)))
        branches = network_wrapper.get_branches(attributes=['name'])
        for branch_index, (branch_name, thermal_limit_col) in enumerate(
                zip(line_table['name'], line_thermal_limit.columns[1:])):
            branch_id = NetworkWrapper.get_id_from_name(branches, branch_name.as_py())
            for time_index in range(len(time_col)):
                for contingency_index, contingency_id in enumerate(contingency_ids):
                    flow = security_analysis_flows[time_index].get((contingency_id, branch_id))
                    i1, i2 = flow if flow else (0, 0)
                    rho1 = i1 / thermal_limit_col[time_index].as_py()
                    rho2 = i2 / thermal_limit_col[time_index].as_py()
                    rho[time_index][contingency_index][branch_index] = max(rho1, rho2)
        return rho

    def evaluate(self, directory: Path) -> list[float]:
        action_table = pq.read_table(directory / 'actions.parquet')
        done_col = action_table['done']

        gen_table = pq.read_table(directory / 'gen.parquet')
        load_table = pq.read_table(directory / 'load.parquet')
        storage_table = pq.read_table(directory / 'storage.parquet')
        line_table = pq.read_table(directory / 'line.parquet')

        gen_p = pq.read_table(directory / 'gen_p.parquet')
        gen_v = pq.read_table(directory / 'gen_v.parquet')
        gen_bus = pq.read_table(directory / 'gen_bus.parquet')
        load_p = pq.read_table(directory / 'load_p.parquet')
        load_q = pq.read_table(directory / 'load_q.parquet')
        load_bus = pq.read_table(directory / 'load_bus.parquet')
        storage_power = pq.read_table(directory / 'storage_power.parquet')
        storage_bus = pq.read_table(directory / 'storage_bus.parquet')
        line_or_bus = pq.read_table(directory / 'line_or_bus.parquet')
        line_ex_bus = pq.read_table(directory / 'line_ex_bus.parquet')
        line_rho = pq.read_table(directory / 'line_rho.parquet')
        line_thermal_limit = pq.read_table(directory / 'line_thermal_limit.parquet')

        # step 1
        rho_n = self.calculate_rho(line_rho)

        # step 2
        rho_n_max = np.max(rho_n)

        env = EnvData.load(directory)
        n_busbar_per_sub = env.json["n_busbar_per_sub"]

        network_wrapper = NetworkWrapper.load(Path(env.json['path']), n_busbar_per_sub)

        time_col = gen_p['time']

        # run a security analysis on all N-1 branch contingency and monitoring all branches
        all_branches_ids = network_wrapper.network.get_branches(attributes=[]).index.tolist()
        contingency_ids = all_branches_ids
        monitored_element_ids = all_branches_ids
        flows, n_div, n1_div = self.run_security_analysis(network_wrapper,
                                                          contingency_ids, monitored_element_ids,
                                                          time_col, done_col,
                                                          load_table, load_p, load_q, load_bus,
                                                          gen_table, gen_p, gen_v, gen_bus,
                                                          storage_table, storage_power, storage_bus,
                                                          line_table, line_or_bus, line_ex_bus)

        # step 3
        rho_n1 = self.compute_rho_n1(network_wrapper,
                                     contingency_ids, monitored_element_ids, flows,
                                     time_col, line_table, line_thermal_limit)

        # step 4
        rho_n1_max = np.max(rho_n1)

        # step 5
        rho_n_avg = np.mean(rho_n)

        # step 6
        rho_n1_avg = np.mean(rho_n1)

        # step 7
        overload_n = np.sum(rho_n > 1) * 100.0 / np.size(rho_n)

        # step 8
        overload_n1 = np.sum(rho_n1 > 1) * 100.0 / np.size(rho_n1)

        # step 9
        return [rho_n_max, rho_n1_max, rho_n_avg, rho_n1_avg, overload_n, overload_n1, n_div, n1_div]

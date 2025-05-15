from pathlib import Path

import pyarrow.parquet as pq

from grid2evaluate.actions import Actions
from grid2evaluate.energy_util import calculate_dispatched_energy_by_generator, \
    calculate_curtailment_energy_by_generator, calculate_lost_energy_by_generator, \
    calculate_balancing_energy_by_generator, calculate_blackout_energy
from grid2evaluate.grid_kpi import GridKpi


class OperationScoreKpi(GridKpi):
    def __init__(self):
        super().__init__("Operation score")

    def evaluate(self, directory: Path) -> list[float]:
        action_table = pq.read_table(directory / 'actions.parquet')
        actions = Actions.load(action_table)

        # step 1
        topo_actions = actions.filter_topo_actions()
        n_topo = [len(acts) for acts in topo_actions]

        # step 2
        n_topo_sum = sum(n_topo)

        # step 3
        redispatch_actions = actions.filter_redispatch_actions()

        # step 4
        n_redispatch = [len(acts) for acts in redispatch_actions]

        # step 5
        n_redispatch_sum = sum(n_redispatch)

        # step 6
        gen_table = pq.read_table(directory / 'gen.parquet')
        gen_actual_dispatch_table = pq.read_table(directory / 'gen_actual_dispatch.parquet')
        e_redispatch = sum(calculate_dispatched_energy_by_generator(gen_table, gen_actual_dispatch_table))

        # step 7
        gen_target_dispatch_table = pq.read_table(directory / 'gen_target_dispatch.parquet')
        e_balancing = sum(calculate_balancing_energy_by_generator(gen_table, gen_actual_dispatch_table, gen_target_dispatch_table))

        # step 8
        curtail_actions = actions.filter_curtail_actions()

        # step 9
        n_curtail = [len(acts) for acts in curtail_actions]

        # step 10
        n_curtail_sum = sum(n_curtail)

        # step 11
        gen_p_before_curtail_table = pq.read_table(directory / 'gen_p_before_curtail.parquet')
        gen_p_table = pq.read_table(directory / 'gen_p.parquet')
        e_curtailment = sum(calculate_curtailment_energy_by_generator(gen_table, gen_p_before_curtail_table, gen_p_table))

        # step 12
        load_table = pq.read_table(directory / 'load.parquet')
        load_p_table = pq.read_table(directory / 'load_p.parquet')
        e_lost = calculate_lost_energy_by_generator(gen_table, gen_p_table, load_table, load_p_table)

        # step 13
        e_blackout = calculate_blackout_energy(action_table, load_table, load_p_table)

        return [n_topo_sum, n_redispatch_sum, e_redispatch, e_balancing, n_curtail_sum, e_curtailment, e_lost, e_blackout]

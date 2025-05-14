from pathlib import Path

import pyarrow.parquet as pq

from grid2evaluate.grid_kpi import GridKpi


class CarbonIntensityKpi(GridKpi):
    def __init__(self):
        super().__init__("Carbon Intensity")

    def evaluate(self, directory: Path) -> list[float]:
        # step 1
        gen_table = pq.read_table(directory / 'gen.parquet')

        # step 2
        gen_p_before_curtail_table = pq.read_table(directory / 'gen_p_before_curtail.parquet')

        # step 3
        gen_p_table = pq.read_table(directory / 'gen_p.parquet')

        # step 4
        e_curtailment = [0] * len(gen_table)
        time_col = gen_p_before_curtail_table['time']
        for (gen_index, gen_name) in enumerate(gen_table['name']):
            gen_p_before_curtail_col = gen_p_before_curtail_table[str(gen_name)]
            gen_p_col = gen_p_table[str(gen_name)]
            for time_index, (time, gen_p_before_curtail, gen_p) in enumerate(zip(time_col,
                                                                                 gen_p_before_curtail_col,
                                                                                 gen_p_col)):
                duration_step = (time.as_py() - time_col[time_index - 1].as_py()) / 3600 if time_index > 0 else 0
                e_curtailment[gen_index] += (gen_p.as_py() - gen_p_before_curtail.as_py()) * duration_step

        # step 5: TODO
        e_redispatch = [0] * len(gen_table)

        # step 6:
        energy = e_curtailment + e_redispatch

        # step 7 + 8
        energy_by_gen_type = {str(gen_type): 0 for gen_type in set(gen_table['type'])}
        for gen_type, energy in zip(gen_table['type'], energy):
            energy_by_gen_type[str(gen_type)] += energy

        # step 9
        emission_factor = {'hydro': 24, 'thermal': 655, 'solar': 45, 'nuclear': 12, 'wind': 11}

        # step 10
        weighted_energy_sum = 0
        for gen_type, energy in energy_by_gen_type.items():
            weighted_energy_sum += energy * emission_factor[gen_type]
        sum_energy_by_gen_type = sum(energy_by_gen_type.values())
        return [weighted_energy_sum / sum_energy_by_gen_type] if sum_energy_by_gen_type > 0 else [0]

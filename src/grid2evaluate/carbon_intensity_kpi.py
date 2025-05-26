# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from pathlib import Path

import pyarrow.parquet as pq

from grid2evaluate.energy_util import calculate_dispatched_energy_by_generator, \
    calculate_curtailment_energy_by_generator
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
        e_curtailment = calculate_curtailment_energy_by_generator(gen_table, gen_p_before_curtail_table, gen_p_table)

        # step 5
        gen_actual_dispatch_table = pq.read_table(directory / 'gen_actual_dispatch.parquet')
        e_redispatch = calculate_dispatched_energy_by_generator(gen_table, gen_actual_dispatch_table)

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

# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from pyarrow import Table, ChunkedArray


def calculate_duration_step(time_col: ChunkedArray, time_index: int):
    return (time_col[time_index].as_py() - time_col[time_index - 1].as_py()) / 3600 if time_index > 0 else 0


def calculate_curtailment_energy_by_generator(gen_table: Table, gen_p_before_curtail_table: Table, gen_p_table: Table) -> list[float]:
    e_curtailment = [0] * len(gen_table)
    time_col = gen_p_before_curtail_table['time']
    for (gen_index, gen_name) in enumerate(gen_table['name']):
        gen_p_before_curtail_col = gen_p_before_curtail_table[str(gen_name)]
        gen_p_col = gen_p_table[str(gen_name)]
        for time_index, (gen_p_before_curtail, gen_p) in enumerate(zip(gen_p_before_curtail_col, gen_p_col)):
            duration_step = calculate_duration_step(time_col, time_index)
            e_curtailment[gen_index] += (gen_p.as_py() - gen_p_before_curtail.as_py()) * duration_step
    return e_curtailment


def calculate_dispatched_energy_by_generator(gen_table: Table, gen_actual_dispatch_table: Table) -> list[float]:
    e_redispatch = [0] * len(gen_table)
    time_col = gen_actual_dispatch_table['time']
    for (gen_index, gen_name) in enumerate(gen_table['name']):
        gen_actual_dispatch_col = gen_actual_dispatch_table[str(gen_name)]
        for time_index, gen_actual_dispatch in enumerate(gen_actual_dispatch_col):
            duration_step = calculate_duration_step(time_col, time_index)
            e_redispatch[gen_index] += gen_actual_dispatch.as_py() * duration_step
    return e_redispatch


def calculate_balancing_energy_by_generator(gen_table: Table, gen_actual_dispatch_table: Table, gen_target_dispatch_table: Table) -> list[float]:
    e_balancing = [0] * len(gen_table)
    time_col = gen_target_dispatch_table['time']
    for (gen_index, gen_name) in enumerate(gen_table['name']):
        gen_actual_dispatch_col = gen_actual_dispatch_table[str(gen_name)]
        gen_target_dispatch_col = gen_target_dispatch_table[str(gen_name)]
        for time_index, (gen_actual_dispatch, gen_target_dispatch) in enumerate(zip(gen_actual_dispatch_col,
                                                                                    gen_target_dispatch_col)):
            duration_step = calculate_duration_step(time_col, time_index)
            e_balancing[gen_index] += (gen_actual_dispatch.as_py() - gen_target_dispatch.as_py()) * duration_step
    return e_balancing


def calculate_lost_energy_by_generator(gen_table: Table, gen_p_table: Table, load_table: Table, load_p_table: Table) -> float:
    e_lost = 0.0
    time_col = gen_p_table['time']
    for gen_name in gen_table['name']:
        gen_p_col = gen_p_table[str(gen_name)]
        for time_index, gen_p in enumerate(gen_p_col):
            duration_step = calculate_duration_step(time_col, time_index)
            e_lost += gen_p.as_py() * duration_step
    for load_name in load_table['name']:
        load_p_col = load_p_table[str(load_name)]
        for time_index, load_p in enumerate(load_p_col):
            duration_step = calculate_duration_step(time_col, time_index)
            e_lost -= load_p.as_py() * duration_step
    return e_lost


def calculate_blackout_energy(action_table: Table, load_table: Table, load_p_table: Table) -> float:
    e_blackout = 0.0
    done_col = action_table['done'].to_pandas()
    if done_col.any():
        blackout_time_index = done_col.idxmax()
        time_col = load_p_table['time']
        duration_step = calculate_duration_step(time_col, blackout_time_index)
        for (load_index, load_name) in enumerate(load_table['name']):
            load_p = load_p_table[str(load_name)]
            e_blackout += load_p[blackout_time_index - 1].as_py() * duration_step
    return e_blackout

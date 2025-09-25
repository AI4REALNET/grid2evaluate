# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from pathlib import Path
import time
import argparse

from grid2evaluate.agent_runnner import AgentRunner
from grid2evaluate.assistant_alert_accuracy_kpi import AssistantAlertAccuracyKpi
from grid2evaluate.carbon_intensity_kpi import CarbonIntensityKpi
from grid2evaluate.network_utilization_kpi import NetworkUtilizationKpi
from grid2evaluate.operation_score_kpi import OperationScoreKpi
from grid2evaluate.topological_action_complexity_kpi import TopologicalActionComplexityKpi


def main():
    parser = argparse.ArgumentParser(description="Run grid2evaluate KPIs.")
    parser.add_argument("--input_directory", type=str, required=True, help="Path to input directory")
    parser.add_argument("--record_directory", type=str, required=True, help="Path to enviromenet record directory")
    args = parser.parse_args()

    input_directory = Path(args.input_directory)
    record_directory = Path(args.record_directory)

    agent_runner = AgentRunner()
    agent_runner.run(input_directory, record_directory)

    kpis = [
        CarbonIntensityKpi(),
        TopologicalActionComplexityKpi(),
        NetworkUtilizationKpi(),
        OperationScoreKpi()
    ]
    for kpi in kpis:
        
        print(f"Start {kpi.name}")
        start_time = time.time()
        value = kpi.evaluate(record_directory)
        print(f"{kpi.name}={value} in {time.time() - start_time:.2f} seconds")

    assistant_alert_kpi = AssistantAlertAccuracyKpi()
    print(f"Start {assistant_alert_kpi.name}")
    start_time = time.time()
    value = assistant_alert_kpi.evaluate(input_directory)
    print(f"{assistant_alert_kpi.name}={value} in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()

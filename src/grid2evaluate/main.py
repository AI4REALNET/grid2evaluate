# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from pathlib import Path

from grid2evaluate.assistant_alert_accuracy_kpi import AssistantAlertAccuracyKpi
from grid2evaluate.carbon_intensity_kpi import CarbonIntensityKpi
from grid2evaluate.network_utilization_kpi import NetworkUtilizationKpi
from grid2evaluate.operation_score_kpi import OperationScoreKpi
from grid2evaluate.topological_action_complexity_kpi import TopologicalActionComplexityKpi
from grid2evaluate.total_decision_time_kpi import TotalDecisionTimeKpi


def main():
    directory = Path('/tmp/rec')
    kpis = [
        CarbonIntensityKpi(),
        TopologicalActionComplexityKpi(),
        NetworkUtilizationKpi(),
        OperationScoreKpi(),
        AssistantAlertAccuracyKpi(),
        TotalDecisionTimeKpi()
    ]
    for kpi in kpis:
        value = kpi.evaluate(directory)
        print(f"{kpi.name}={value}")


if __name__ == "__main__":
    main()

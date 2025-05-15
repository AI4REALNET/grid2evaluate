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

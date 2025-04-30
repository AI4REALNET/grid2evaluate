from pathlib import Path

from grid2evaluate.carbon_intensity_kpi import CarbonIntensityKpi


def main():
    directory = Path('/tmp/rec')
    kpis = [CarbonIntensityKpi()]
    for kpi in kpis:
        value = kpi.evaluate(directory)
        print(f"{kpi.name}={value}")


if __name__ == "__main__":
    main()

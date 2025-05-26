# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from pathlib import Path

from grid2evaluate.grid_kpi import GridKpi


class AssistantAlertAccuracyKpi(GridKpi):
    def __init__(self):
        super().__init__("Assistant alert accuracy")

    def evaluate(self, directory: Path) -> list[float]:
        # TODO
        return [0]

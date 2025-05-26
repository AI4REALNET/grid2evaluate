# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from abc import ABC, abstractmethod
from pathlib import Path


class GridKpi(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def evaluate(self, directory: Path) -> list[float]:
        pass

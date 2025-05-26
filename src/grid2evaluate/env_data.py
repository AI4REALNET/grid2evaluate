# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

import json
from pathlib import Path


class EnvData:
    def __init__(self, json: dict):
        self._json = json

    @staticmethod
    def load(directory: Path) -> 'EnvData':
        with open(directory / "env.json", "r", encoding="utf-8") as f:
            return EnvData(json.load(f))

    @property
    def json(self) -> dict:
        return self._json

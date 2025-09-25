# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0
from pathlib import Path
from lightsim2grid import LightSimBackend

import grid2op
from grid2op.Agent import RandomAgent
from grid2op.Agent.alertAgent import AlertAgent

from grid2op.Environment.EnvRecorder import EnvRecorder

class AgentRunner():

    def run(self, directory: Path, output_directory: Path):
        env = grid2op.make(
            directory,
            test=True,
            backend=LightSimBackend(),
            _add_to_name="test"
        )
        with EnvRecorder(env, output_directory) as env_rec:
            obs = env_rec.reset()
            do_nothing = env.action_space()
            reward = 0
            done = False
            total_reward = 0
            agent = RandomAgent(env.action_space)
            while not done:
                action = agent.act(obs, reward, done)
                obs, reward, done, info = env_rec.step(action)
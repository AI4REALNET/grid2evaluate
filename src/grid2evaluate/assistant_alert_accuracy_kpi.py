# Copyright (c) 2025, RTE (http://www.rte-france.com)
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# SPDX-License-Identifier: MPL-2.0

from pathlib import Path

from grid2evaluate.grid_kpi import GridKpi
from pathlib import Path
from lightsim2grid import LightSimBackend

import grid2op
from grid2op.Action.playableAction import PlayableAction
from grid2op.Agent import RandomAgent
from grid2op.Agent.alertAgent import AlertAgent
from grid2op.Environment.EnvRecorder import EnvRecorder


from grid2op.Reward.alertReward import AlertReward
from grid2op.tests._aux_opponent_for_test_alerts import TestOpponent

DEFAULT_ALERT_REWARD_PARAMS = dict(reward_min_no_blackout=-1.0,  #FP
                                   reward_min_blackout=-10.0,    #FN
                                   reward_max_no_blackout=1.0,   #TN
                                   reward_max_blackout=2.0,      #TP
                                   reward_end_episode_bonus=42.0)


class AssistantAlertAccuracyKpi(GridKpi):
    def __init__(self):
        super().__init__("Assistant alert accuracy")

    @staticmethod
    def get_confusion_matrix_row(alert_reward):
        if alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_max_no_blackout']:
            return [0, 1, 0, 0] #TN true negative
        elif alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_min_no_blackout']:
            return [0, 0, 1, 0] #FP false positive
        elif alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_max_blackout']:
            return [1, 0, 0, 0] #TP true positive
        elif alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_min_blackout']:
            return [0, 0, 0, 1] #FN false negative
        else:
            return []

    @staticmethod
    def get_alert_reward(directory, attacked_line, no_action=False):

        kwargs_opponent = dict(lines_attacked=[attacked_line],
                                duration=3, 
                                steps_attack=[1])
        env = grid2op.make(
            directory,
            test=True,
            backend=LightSimBackend(),
            difficulty="1", 
            opponent_attack_cooldown=0, 
            opponent_attack_duration=99999, 
            opponent_budget_per_ts=1000, 
            opponent_init_budget=10000., 
            opponent_action_class=PlayableAction, 
            opponent_class=TestOpponent, 
            kwargs_opponent=kwargs_opponent,
            reward_class=AlertReward(**DEFAULT_ALERT_REWARD_PARAMS),
            _add_to_name="test"
        )

        env.seed(0)
        obs = env.reset()
        do_nothing = env.action_space()
        agent = AlertAgent(env.action_space)
        reward = 0
        done = False
        action = do_nothing
        for i in range(env.parameters.ALERT_TIME_WINDOW +1):
            
            if (not no_action):
                action = agent.act(obs, reward, done)
            obs, reward, done, info = env.step(action)
        return reward

    def evaluate(self, directory: Path) -> list[float]:
        env = grid2op.make(
            directory,
            test=True,
            backend=LightSimBackend(),
            _add_to_name="test"
        )
        obs = env.reset()
        alert_rewards = []
        for line_id in env.alertable_line_ids:
            attacked_line_name = obs.name_line[line_id]
            alert_reward = self.get_alert_reward(directory, attacked_line_name, False)
            alert_rewards.append(self.get_confusion_matrix_row(alert_reward))

        return alert_rewards

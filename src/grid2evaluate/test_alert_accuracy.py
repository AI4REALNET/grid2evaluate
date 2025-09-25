from pathlib import Path
import shutil
from lightsim2grid import LightSimBackend
from l2rpn_baselines.ExpertAgent import ExpertAgent
from alphaDeesp.expert_operator import expert_operator
from alphaDeesp.core.grid2op.Grid2opSimulation import Grid2opSimulation

import grid2op
from grid2op.Action.playableAction import PlayableAction
from grid2op.Agent import RandomAgent
from grid2op.Agent.alertAgent import AlertAgent
from grid2op.Environment.EnvRecorder import EnvRecorder
import copy
import os

from grid2op.Reward.alertReward import AlertReward
from grid2op.tests._aux_opponent_for_test_alerts import TestOpponent

DEFAULT_ALERT_REWARD_PARAMS = dict(reward_min_no_blackout=-1.0,  #FP
                                   reward_min_blackout=-10.0,    #FN
                                   reward_max_no_blackout=1.0,   #TN
                                   reward_max_blackout=2.0,      #TP
                                   reward_end_episode_bonus=42.0)



def get_confusion_matrix_row(alert_reward):
    # TP,TN,FP,FN
    if alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_max_no_blackout']:
        return [0, 1, 0, 0] #TN
    elif alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_min_no_blackout']:
        return [0, 0, 1, 0] #FP
    elif alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_max_blackout']:
        return [1, 0, 0, 0] #"TP"
    elif alert_reward == DEFAULT_ALERT_REWARD_PARAMS['reward_min_blackout']:
        return [0, 0, 0, 1] #"FN"
    else:
        return []


def get_alert_reward(attacked_line, no_action=False):
    print(f"attacked_line {attacked_line}")

    kwargs_opponent = dict(lines_attacked=[attacked_line],
                               duration=3, 
                               steps_attack=[1])
    env = grid2op.make(
        "/home/opfab/develop/Grid2Op/grid2op/data/l2rpn_idf_2023",
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
        #print(f"Step {i+1}/{env.parameters.ALERT_TIME_WINDOW} rewar = {reward}")
    return reward

def get_alert_reward_no_attack(no_action=False):

    env = grid2op.make(
        "/home/opfab/develop/Grid2Op/grid2op/data/l2rpn_idf_2023",
        test=True,
        backend=LightSimBackend(),
        difficulty="1",
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
    for i in range(env.max_episode_duration()):
        
        if (not no_action):
            action = agent.act(obs, reward, done)
        obs, reward, done, info = env.step(action)
        #print(f"Step {i+1}/{env.parameters.ALERT_TIME_WINDOW} rewar = {reward}")
    return reward

if __name__ == "__main__":
    env = grid2op.make(
        "/home/opfab/develop/Grid2Op/grid2op/data/l2rpn_idf_2023",
        test=True,
        backend=LightSimBackend(),
        _add_to_name="test"
    )
    obs = env.reset()
    #alert_reward = get_alert_reward_no_attack()
    #print(f"Alert reward no disconnction = {alert_reward}")
    
    print(f"alertable_line_ids {env.alertable_line_ids}")
    #for line_id in env.alertable_line_ids:
    #  alert_reward = get_alert_reward(obs.name_line[line_id], False)
    #  print(f"Alert reward for disconnction on line {line_id} = {alert_reward}")
    
    # ...existing code...
    print(f"alertable_line_ids {env.alertable_line_ids}")
    alert_rewards = []  # Collect rewards here
    for line_id in env.alertable_line_ids:
        alert_reward = get_alert_reward(obs.name_line[line_id], False)
        alert_rewards.append(get_confusion_matrix_row(alert_reward))
        print(f"Alert reward for disconnction on line {line_id} = {alert_reward}")
    print("All alert rewards:", alert_rewards)
    # ...existing code...

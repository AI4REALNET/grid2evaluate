# grid2evaluate

To generate parquet tables from a Grid2op environment:

```python
from pathlib import Path

from lightsim2grid import LightSimBackend

import grid2op
from grid2op.Agent import RandomAgent
from grid2op.Environment.EnvRecorder import EnvRecorder

if __name__ == "__main__":
    env = grid2op.make(
        "<PATH TO ENV>",
        test=True,
        backend=LightSimBackend(),
        _add_to_name="test"
    )
    with EnvRecorder(env, Path('<PATH TO RECORDED DATA>')) as env_rec:
        obs = env_rec.reset()
        do_nothing = env.action_space()
        reward = 0
        done = False
        total_reward = 0
        agent = RandomAgent(env.action_space)
        while not done:
            action = agent.act(obs, reward, done)
            obs, reward, done, info = env_rec.step(action)
```

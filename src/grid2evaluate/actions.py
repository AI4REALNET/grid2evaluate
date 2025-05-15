import json

from pyarrow import Table


class Actions:
    def __init__(self, actions: list):
        self.actions = actions

    def __len__(self):
        return len(self.actions)

    def __iter__(self):
        return iter(self.actions)

    @staticmethod
    def load(action_table: Table) -> 'Actions':
        actions = []
        for row in action_table.to_pandas().itertuples():
            actions.append(json.loads(row.action))
        return Actions(actions)

    @staticmethod
    def _filter_step_actions(actions: dict, action_types: list[str]) -> dict:
        filtered_actions = {}
        for action_type in action_types:
            if action_type in actions:
                filtered_actions[action_type] = actions[action_type]
        return filtered_actions

    @staticmethod
    def _filter_step_topo_actions(actions: dict) -> dict:
        only_topo_actions = {}
        for action_type in ['set_bus', 'change_bus']:
            if action_type in actions:
                if 'lines_or_id' in actions[action_type] or 'lines_ex_id' in actions[action_type]:
                    only_topo_actions[action_type] = actions[action_type]
        only_topo_actions.update(Actions._filter_step_actions(actions,['line_or_set_bus',
                                                                                  'line_ex_set_bus',
                                                                                  'line_or_change_bus',
                                                                                  'line_ex_change_bus',
                                                                                  'line_set_status',
                                                                                  'line_change_status']))
        return only_topo_actions

    @staticmethod
    def _filter_step_redispatch_actions(actions: dict) -> dict:
        return Actions._filter_step_actions(actions, ['redispatch', 'storage_p'])

    @staticmethod
    def _filter_step_curtail_actions(actions: dict) -> dict:
        return Actions._filter_step_actions(actions, ['curtail'])

    def filter_topo_actions(self) -> 'Actions':
        return Actions([self._filter_step_topo_actions(action) for action in self.actions])

    def filter_redispatch_actions(self) -> 'Actions':
        return Actions([self._filter_step_redispatch_actions(action) for action in self.actions])

    def filter_curtail_actions(self) -> 'Actions':
        return Actions([self._filter_step_curtail_actions(action) for action in self.actions])

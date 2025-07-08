from tables.subscription import Subscription


class SubscriptionTable(object):
    """
    Class representing a table with all the subscriptions to context information
    """

    def __init__(self):
        self._table = {}

    def add_entry(self, sub: Subscription):
        if sub is not None:
            self._table[sub.get_object_id()] = sub

    def remove_entry(self, entry_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        if entry_id is not None and entry_id in self._table:
            del self._table[entry_id]

    def search_entry(self, entity_id: str, entity_type: str, scope: str) -> list:
        """searches for any matching subscription from the table, if one or more exist, returns a list of subscriptions
                 if not entry exist, then returns an empty list"""
        result_list = []
        if entity_id is not None and entity_type is not None and scope is not None:
            for sub in self._table.values():
                if sub.get_entity_id() == entity_id and sub.get_entity_type() == entity_type and sub.get_scope() == scope:
                    result_list.append(sub)
        return result_list

    def print(self) -> str:
        result = ""
        for sub in self._table.values():
            result += '\n' + sub.print() + " ||"
        return result

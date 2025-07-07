import time

from tables.context_info import ContextInfo

class ContextInfoTable(object):
    """
    Class representing a table with all the gathered context information
    """

    def __init__(self):
        self._table = {}

    def add_entry(self, context_info_object: ContextInfo):
        if context_info_object is not None:
            self._table[context_info_object.get_object_id()] = context_info_object

    def remove_entry(self, entry_id):
        if entry_id is not None and entry_id in self._table:
            del self._table[entry_id]
        return None

    def search_entry(self, entity_id, entity_type, scope):
        if entity_id is not None and entity_type is not None and scope is not None:
            key_identifier = str(hash(entity_id + entity_type + scope))
            if key_identifier in self._table:

                begin_tag = int(self._table[key_identifier].get_validity_begin())
                end_tag = int(self._table[key_identifier].get_validity_end())
                current_time = int(round(time.time() * 1000))

                currently_valid = begin_tag <= current_time <= end_tag
                if currently_valid:
                    return self._table[key_identifier]
        return None

    def print(self):
        result = ""
        for entry in self._table.values():
            result += entry.print() + " || " + '\n'
        return result

import threading

from global_data import *
from utils.misc_utils import is_string_valid_int_bigger_than_zero


class ContextInfo(object):
    """Class representing an entry in the context info table"""

    def __init__(self):
        self._provider_id = ""
        self._entity_id = ""
        self._entity_type = ""
        self._scope = ""
        self._data = ""
        "timestamps for the beginning and the end of the time period when the context info is valid"
        "time expressed as milliseconds UNIX time"
        self._validity_begin = 0
        self._validity_end = 0

        self._timer_thread = None

    def get_object_id(self) -> str:
        """unique identifier for this object"""
        return str(hash(self._entity_id + self._entity_type + self._scope))

    def get_provider_id(self) -> str:
        return self._provider_id

    def set_provider_id(self, provider_id: str):
        if provider_id is not None:
            self._provider_id = provider_id

    def get_entity_id(self) -> str:
        return self._entity_id

    def set_entity_id(self, entity_id: str):
        if entity_id is not None:
            self._entity_id = entity_id

    def get_entity_type(self) -> str:
        return self._entity_type

    def set_entity_type(self, entity_type: str):
        if entity_type is not None:
            self._entity_type = entity_type

    def get_scope(self) -> str:
        return self._scope

    def set_scope(self, scope: str):
        if scope is not None:
            self._scope = scope

    def get_data(self) -> str:
        return self._data

    def set_data(self, data: str):
        if data is not None:
            self._data = data

    def get_validity_begin(self) -> int:
        """value for the beginning of the time period when the context info is valid, time expressed as milliseconds UNIX time """
        return self._validity_begin

    def set_validity_begin(self, validity_begin: str):
        """timestamp for the beginning of the time period when the context info is valid, time expressed as milliseconds UNIX time """
        if validity_begin is not None and is_string_valid_int_bigger_than_zero(validity_begin):
            self._validity_begin = int(validity_begin)

    def get_validity_end(self) -> int:
        """value for the end of the time period when the context info is valid, time expressed as milliseconds UNIX time """
        return self._validity_end

    def set_validity_end(self, validity_end: str):
        """timestamp for the end of the time period when the context info is valid, time expressed as milliseconds UNIX time """
        if validity_end is not None and is_string_valid_int_bigger_than_zero(validity_end):
            self._validity_end = int(validity_end)

    def print(self) -> str:
        contents = {"CONTEXT_INFO_ID": self.get_object_id(), "PROVIDER_ID": self.get_provider_id(),
                    "ENTITY_ID": self.get_entity_id(), "ENTITY_TYPE": self.get_entity_type(), "SCOPE": self.get_scope(),
                    "DATA": self.get_data(), "VALIDITY_BEGIN": self.get_validity_begin(),
                    "VALIDITY_END": self.get_validity_end()}

        return str(contents)


class ContextInfoTable(object):
    """
    Class representing a table with all the gathered context information
    """

    def __init__(self):
        self._table = {}
        self._table_lock = threading.Lock()
        # each entry has an associated timer
        # when the info is no longer valid, it will call this thread and delete the entry
        self._entry_timers = {}

    def add_entry(self, context_info_object: ContextInfo):
        if context_info_object is not None:
            self._table_lock.acquire()
            self._table[context_info_object.get_object_id()] = context_info_object
            valid_interval_time = (context_info_object.get_validity_end() - broker_clock.current_broker_time()) / 1000
            self._entry_timers[context_info_object.get_object_id()] = threading.Timer(valid_interval_time,
                                                                                      ctx_timer_thread,
                                                                                      [context_info_object.get_object_id(),
                                                                                       self])
            self._entry_timers[context_info_object.get_object_id()].start()
            self._table_lock.release()

    def remove_entry(self, entry_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        self._table_lock.acquire()
        if entry_id is not None and entry_id in self._table:
            self._entry_timers[entry_id].cancel()
            del self._table[entry_id]
        self._table_lock.release()

    def search_entry(self, entity_id: str, entity_type: str, scope: str) -> ContextInfo | None:
        """searches for an entry from the table, if entry exist, returns the entry
         if entry doesn't exist, then returns None"""
        result = None
        if entity_id is not None and entity_type is not None and scope is not None:
            key_identifier = str(hash(entity_id + entity_type + scope))
            self._table_lock.acquire()
            if key_identifier in self._table:

                begin_tag = int(self._table[key_identifier].get_validity_begin())
                end_tag = int(self._table[key_identifier].get_validity_end())
                current_time = broker_clock.current_broker_time()

                currently_valid = begin_tag <= current_time <= end_tag
                if currently_valid:
                    result = self._table[key_identifier]
            self._table_lock.release()
        return result

    def print(self) -> str:
        result = ""
        self._table_lock.acquire()
        for entry in self._table.values():
            result += '\n' + entry.print() + " ||"
        self._table_lock.release()
        return result


def ctx_timer_thread(entry_id: str, ctx_tab: ContextInfoTable):
    """timer thread used to delete context table entries after their validity ends"""
    ctx_tab.remove_entry(entry_id)
    broker_logger.info("ctx_table : " + ctx_tab.print())

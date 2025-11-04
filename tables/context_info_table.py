import threading

from global_data import *
from utils.parsed_message import ParsedMessage


class ContextInfo(object):
    """Class representing an entry in the context info table"""

    def __init__(self, rcv_msg: ParsedMessage):
        self._timer_thread = None
        self._provider_id = rcv_msg.get_token("PROVIDER_ID")
        self._entity_id = rcv_msg.get_token("ENTITY_ID")
        self._entity_type = rcv_msg.get_token("ENTITY_TYPE")
        self._scope = rcv_msg.get_token("SCOPE")
        self._data = rcv_msg.get_token("DATA")
        "timestamp for the end of the time period when the context info is valid"
        "time expressed as milliseconds UNIX time"
        self._validity_end = rcv_msg.get_token("VALIDITY_END")

    def get_object_id(self) -> str:
        """unique identifier for this object"""
        return str(hash(self._entity_id + self._entity_type + self._scope))

    def get_provider_id(self) -> str:
        return self._provider_id

    def get_entity_id(self) -> str:
        return self._entity_id

    def get_entity_type(self) -> str:
        return self._entity_type

    def get_scope(self) -> str:
        return self._scope

    def get_data(self) -> str:
        return self._data

    def get_validity_end(self) -> int:
        """value for the end of the time period when the context info is valid, time expressed as milliseconds UNIX time """
        return self._validity_end

    def print(self) -> str:
        contents = {"CONTEXT_INFO_ID": self.get_object_id(), "PROVIDER_ID": self.get_provider_id(),
                    "ENTITY_ID": self.get_entity_id(), "ENTITY_TYPE": self.get_entity_type(), "SCOPE": self.get_scope(),
                    "DATA": self.get_data(), "VALIDITY_END": self.get_validity_end()}

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
                if broker_clock.current_broker_time() <= self._table[key_identifier].get_validity_end():
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
    broker_logger.debug("ctx_table : " + ctx_tab.print())

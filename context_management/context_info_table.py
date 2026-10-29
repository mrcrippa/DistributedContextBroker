import json
import threading
from dataclasses import dataclass, field

from global_data import *


@dataclass
class ContextInfo(object):
    """Class representing an entry in the context info table"""

    provider_id: str
    provider_address : str
    provider_port: int
    entity_id: str
    entity_type: str
    scope: str
    data: str
    # timestamp for the end of the time period when the context info is valid
    # time expressed as milliseconds UNIX time
    validity_end: int
    # unique identifier for this object
    object_id: str = field(init=False)

    def __post_init__(self):
        self.object_id = str(hash(self.entity_id + self.entity_type + self.scope))

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


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
            self._table[context_info_object.object_id] = context_info_object
            valid_interval_time = (context_info_object.validity_end - broker_clock.current_broker_time()) / 1000
            self._entry_timers[context_info_object.object_id] = threading.Timer(valid_interval_time,
                                                                                      ctx_timer_thread,
                                                                                      [context_info_object.object_id,
                                                                                       self])
            self._entry_timers[context_info_object.object_id].start()
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
                if broker_clock.current_broker_time() <= self._table[key_identifier].validity_end:
                    result = self._table[key_identifier]
            self._table_lock.release()
        return result

    def print(self) -> str:
        result = ""
        self._table_lock.acquire()
        for entry in self._table.values():
            result += '\n' + entry + " ||"
        self._table_lock.release()
        return result


def ctx_timer_thread(entry_id: str, ctx_tab: ContextInfoTable):
    """timer thread used to delete context table entries after their validity ends"""
    ctx_tab.remove_entry(entry_id)
    broker_logger.debug("ctx_table : " + ctx_tab.print())

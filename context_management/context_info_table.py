"""
This module contains classes defining the context info table, which holds all the context information received by this
broker, and a dataclass defining an entry in the table

Classes
--------
ContextInfo
ContextInfoTable
"""

import threading
from dataclasses import dataclass, field

from global_data import *


@dataclass
class ContextInfo(object):
    """
            Class that defines contextual information and represents an entry in the context info table

            A unique object identifier is generated after an instance is created. This identifier is used to index
            the context info table

            Attributes
            ----------
            provider_id : str
                unique identifier for the context provider that sent this data
            provider_address : str
                address for the provider
            provider_port : int
                port for the provider
            entity_id : str
                unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
            entity_type : str
                specifies the type of entity (for instance, "city" or "person")
            scope : str
                specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")
            data : str
                string with the value for the indicated scope (for instance, "temperature_celsius=21.0" or "height=1.8m")
            validity_end : int
                timestamp for the end of the time period when the context info is still valid,
                time expressed as milliseconds UNIX time, should come after the current internal broker clock
            object_id : str
                a unique identifier for an instance of this dataclass. It's initialized automatically after instantiation
        """

    provider_id: str
    provider_address: str
    provider_port: int
    entity_id: str
    entity_type: str
    scope: str
    data: str
    validity_end: int
    # unique identifier for this object
    object_id: str = field(init=False)

    def __post_init__(self):
        """Sets the unique identifier for an instance of this dataclass based on entity_id, entity_type, and scope"""
        self.object_id = str(hash(self.entity_id + self.entity_type + self.scope))


class ContextInfoTable(object):
    """
        Class representing a table with all the gathered context information

        Attributes
        ----------
        _table : dict
            dict with all the ContextInfo objects, representing all the context information gathered. The keys come from the
            object_id attribute from ContextInfo
        _table_lock : Lock
            mutex Lock used to manage simultaneous access from multiple threads
        _entry_timers : dict
            since each ContextInfo has an expiration time (validity_end), each entry has an associated Timer thread that will
            delete the entry when it's expired
    """

    def __init__(self):
        self._table = {}
        self._table_lock = threading.Lock()
        self._entry_timers = {}

    def _add_entry(self, context_info_object: ContextInfo):
        """
            Internal version of add_entry, for thread safety

            Parameters
            ----------
            context_info_object: ContextInfo
                the context info object to be added
        """

        self._table[context_info_object.object_id] = context_info_object
        # calculates the validity interval and creates the associated Timer thread for deletion when the validity expires
        valid_interval_time = (context_info_object.validity_end - broker_internal_clock.current_broker_time()) / 1000
        self._entry_timers[context_info_object.object_id] = threading.Timer(valid_interval_time,
                                                                            ctx_timer_thread,
                                                                            [context_info_object.object_id,
                                                                             self])
        self._entry_timers[context_info_object.object_id].start()

    def add_entry(self, context_info_object: ContextInfo):
        """
            Adds an entry to the context info table

            Parameters
            ----------
            context_info_object: ContextInfo
                the context info object to be added
        """
        if context_info_object is not None:
            with self._table_lock:
                self._add_entry(context_info_object)

    def _remove_entry(self, entry_id: str):
        """
            Internal version of remove_entry, for thread safety

            Parameters
            ----------
            entry_id: str
                object id of the entry to be removed
        """
        if entry_id is not None and entry_id in self._table:
            # cancels the Timer thread associated with this entry
            self._entry_timers[entry_id].cancel()
            del self._table[entry_id]

    def remove_entry(self, entry_id: str):
        """
            Deletes an entry from the table

            If an entry with the given id doesn't exist, then nothing happens

            Parameters
            ----------
            entry_id: str
                object id of the entry to be removed
        """
        with self._table_lock:
            self._remove_entry(entry_id)

    def _search_entry(self, key_identifier: str) -> ContextInfo | None:
        """
            Internal version of search_entry, for thread safety

            Parameters
            ----------
            key_identifier : str
                object id of the desired entry

            Returns
            -------
            ContextInfo | None
                the entry as a ContextInfo object, if the entry exists in the table or
                None, if the entry is not in the table
        """
        result = None
        if key_identifier in self._table:
            if broker_internal_clock.current_broker_time() <= self._table[key_identifier].validity_end:
                result = self._table[key_identifier]
        return result

    def search_entry(self, entity_id: str, entity_type: str, scope: str) -> ContextInfo | None:

        """
            Searches for an entry in the table.

            Parameters
            ----------
            entity_id : str
                unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
            entity_type : str
                specifies the type of entity (for instance, "city" or "person")
            scope : str
                specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")

            Returns
            -------
            ContextInfo | None
                the entry as a ContextInfo object, if the entry exists in the table or
                None, if the entry is not in the table
        """
        result = None
        if entity_id is not None and entity_type is not None and scope is not None:
            with self._table_lock:
                result = self._search_entry(str(hash(entity_id + entity_type + scope)))
        return result

    def _print(self) -> str:
        """
        Internal version of print, for thread safety

        Returns
        -------
        str
            string showing the contents of the table
        """
        result = ""
        for entry in self._table.values():
            result += '\n' + str(entry) + " ||"
        return result

    def print(self) -> str:
        """
            Utility function, inserts the contents of the context info table into a string

            Helpful for debugging

            Returns
            -------
            str
                string showing the contents of the table
        """
        result = ""
        with self._table_lock:
            result = self._print()
        return result


def ctx_timer_thread(entry_id: str, ctx_tab: ContextInfoTable):
    """
        Function called by a Timer thread to delete context table entries after their validity ends

        Parameters
        ----------
        entry_id: str
            object id of the entry to be deleted
        ctx_tab: ContextInfoTable
            reference to the context information table
    """
    ctx_tab.remove_entry(entry_id)
    # broker_logger.debug("ctx_table : " + ctx_tab.print())

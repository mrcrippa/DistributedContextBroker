"""
This module contains classes defining the broker neighbor table, which holds information on all the neighbors of this
broker in the network

Classes
--------
BrokerNeighborInfo
BrokerNeighborTable
"""

import random
import threading
from dataclasses import dataclass

from broker_messages.broker_message_classes import BrokerNeighborsRequestMessage
from global_data import broker_internal_clock, this_broker_info
from message_exchange.message_sender import send_message


@dataclass
class BrokerNeighborInfo(object):
    """
        Class representing one of this broker's neighbors in the network, and an entry in the broker neighbors table

        Attributes
        ----------
        broker_neighbor_id: str
            unique identifier for this neighbor
        broker_neighbor_address: str
            address of this neighbor
        broker_neighbor_port: int
            port of this neighbor
        broker_neighbor_last_online_timestamp
            timestamp of the last contact with this neighbor
            its value is 0 for a neighbor considered offline, != 0 for neighbor considered online

    """

    broker_neighbor_id: str
    broker_neighbor_address: str
    broker_neighbor_port: int
    broker_neighbor_last_online_timestamp: int


class BrokerNeighborTable(object):
    """
        Class representing a table with all information about this broker's neighbors

        Attributes
        ----------
        _table : dict
            dict with all the BrokerNeighborInfo objects, representing all the neighbors of this
            broker in the network. The keys come from the broker_neighbor_id attribute from BrokerNeighborInfo
        _table_lock : Lock
            mutex Lock used to manage simultaneous access from multiple threads
    """

    def __init__(self):
        self._table = {}
        self._table_lock = threading.Lock()

    def _set_entry_online(self, broker_neighbor_id: str):
        """
            Internal version of set_entry_online, for thread safety

            Parameters
            ----------
            broker_neighbor_id: str
                unique identifier for the neighbor now considered online
        """
        if broker_neighbor_id is not None and broker_neighbor_id in self._table:
            # last_online_timestamp is set to the current broker internal clock value
            self._table[
                broker_neighbor_id].broker_neighbor_last_online_timestamp = broker_internal_clock.current_broker_time()

    def set_entry_online(self, broker_neighbor_id: str):
        """
            Searches for and marks a neighbor as online, if the neighbor does not exist, nothing happens

            Parameters
            ----------
            broker_neighbor_id: str
                unique identifier for the neighbor now considered online
        """
        with self._table_lock:
            self._set_entry_online(broker_neighbor_id)

    def _add_entry(self, neighbor: BrokerNeighborInfo):
        """
            Internal version of add_entry, for thread safety

            Parameters
             ----------
            neighbor: BrokerNeighborInfo
                a BrokerNeighborInfo object with the neighbor's information
        """
        self._table[neighbor.broker_neighbor_id] = neighbor

    def add_entry(self, neighbor: BrokerNeighborInfo):
        """
            Adds an entry to the broker neighbor table

            Parameters
             ----------
            neighbor: BrokerNeighborInfo
                a BrokerNeighborInfo object with the neighbor's information
        """
        if neighbor is not None:
            with self._table_lock:
                self._add_entry(neighbor)

    def _remove_entry(self, broker_neighbor_id: str):
        """
            Internal version of remove_entry, for thread safety

            Parameters
             ----------
            broker_neighbor_id: str
                 unique identifier for the neighbor to be removed
        """
        if broker_neighbor_id is not None and broker_neighbor_id in self._table:
            del self._table[broker_neighbor_id]

    def remove_entry(self, broker_neighbor_id: str):
        """
            Removes an entry from the table, if the entry doesn't exist, then nothing happens

            Parameters
            ----------
            broker_neighbor_id: str
                unique identifier for the neighbor to be removed
        """
        with self._table_lock:
            self._remove_entry(broker_neighbor_id)

    def _search_entry(self, broker_neighbor_id: str) -> BrokerNeighborInfo | None:
        """
           Internal version of search_entry, for thread safety

            Parameters
            ----------
            broker_neighbor_id: str
                unique identifier for the neighbor to be searched

            Returns
            -------
            BrokerNeighborInfo | None
                the entry as a BrokerNeighborInfo object, if the entry exists in the table or
                None, if the entry is not in the table
        """
        result = None
        if broker_neighbor_id is not None and broker_neighbor_id in self._table:
            result = self._table[broker_neighbor_id]
        return result

    def search_entry(self, broker_neighbor_id: str) -> BrokerNeighborInfo | None:
        """
            Searches for an entry in the table.

            Parameters
            ----------
            broker_neighbor_id: str
                unique identifier for the neighbor to be searched

            Returns
            -------
            BrokerNeighborInfo | None
                the entry as a BrokerNeighborInfo object, if the entry exists in the table or
                None, if the entry is not in the table
        """
        result = None
        with self._table_lock:
            result = self._search_entry(broker_neighbor_id)
        return result

    def _check_table_for_potentially_offline_brokers(self):
        """ Internal version of check_table_for_potentially_offline_brokers, for thread safety"""

        neighbor_request_msg = BrokerNeighborsRequestMessage(broker_id=this_broker_info["id"],
                                                             broker_address=this_broker_info["address"],
                                                             broker_port=this_broker_info["port"])

        for entry in self._table.values():
            # time elapsed since the last contact with this neighbor
            time_elapsed = broker_internal_clock.current_broker_time() - entry.broker_neighbor_last_online_timestamp
            # if time_elapsed less than 15s, neighbor is considered online
            # between 15s and 30s, it's potentially offline, and it's contacted for still alive checking
            # more than 30s, it's considered offline
            if 15000 < time_elapsed < 30000:
                send_message(neighbor_request_msg.as_json(), this_broker_info["address"],
                             (entry.broker_neighbor_address, entry.broker_neighbor_port))
            elif time_elapsed > 30000:
                entry.broker_neighbor_last_online_timestamp = 0

    def check_table_for_potentially_offline_brokers(self):
        """Checks which neighbors might be potentially offline,
         and either sends them a broker_neighbors_request if the last contact was between 15s and 30s ago,
         or marks them as offline if last contact was >30s ago"""
        with self._table_lock:
            self._check_table_for_potentially_offline_brokers()

    def _get_shallow_local_flooding_list(self, checked_brokers: list) -> list:
        """
            Internal version of get_shallow_local_flooding_list, for thread safety

            Parameters
            ----------
            checked_brokers: list
                a list of broker ids, each broker was already queried for the desired subscription, so they should not be
                asked again

            Returns
            -------
            list
                a list of BrokerNeighborInfo entries from the table. The first on the list should be the next broker on
                the random walk, the others will be asked only once if they can fulfill the subscription, and not forward the request
        """
        result = []
        to_be_verified = list(self._table.keys())

        # eliminates broker ids of broker that were already asked about the subscription
        for entry in checked_brokers:
            if entry in to_be_verified:
                to_be_verified.remove(entry)

        # creates a list of up to 5 broker ids as the shallow flooding list
        # the choice is random
        # the neighbor must be considered online
        while to_be_verified and len(result) <= 5:
            selected = random.choice(to_be_verified)
            to_be_verified.remove(selected)
            if self._table[selected].broker_neighbor_last_online_timestamp:
                result.append(self._table[selected])

        return result

    def get_shallow_local_flooding_list(self, checked_brokers: list) -> list:
        """
            This function is used during the subscription search between brokers (check subscriptions.subscribe_service.subscription_broker_network_search)

            Parameters
            ----------
            checked_brokers: list
                a list of broker ids, each broker was already queried for the desired subscription, so they should not be
                asked again

            Returns
            -------
            list
                a list of BrokerNeighborInfo entries from the table. The first on the list should be the next broker on
                the random walk, the others will be asked only once if they can fulfill the subscription, and not forward the request
        """
        result = []
        with self._table_lock:
            result = self._get_shallow_local_flooding_list(checked_brokers)
        return result

    def _get_table_as_list_of_dict(self) -> list:
        """
            Internal version of get_table_as_list_of_dict, for thread safety

            Returns
            -------
            list
                a list of dict with the entries in the table. Each dict has the following keys: "broker_id", "broker_address", "broker_port";
                values for id and address are strings, port is an integer
        """
        temp_list = []
        for entry in self._table.values():
            if entry.broker_neighbor_last_online_timestamp:
                temp_dict = {"broker_id": entry.broker_neighbor_id,
                             "broker_address": entry.broker_neighbor_address,
                             "broker_port": entry.broker_neighbor_port}
                temp_list.append(temp_dict)
        return temp_list

    def get_table_as_list_of_dict(self) -> list:
        """
            Returns a list of dictionaries representing the broker neighbor table

            Only neighbors considered online are listed.

            Returns
            -------
            list
                a list of dict with the entries in the table. Each dict has the following keys: "broker_id", "broker_address", "broker_port";
                values for id and address are strings, port is an integer
        """
        temp_list = []
        with self._table_lock:
            temp_list = self._get_table_as_list_of_dict()
        return temp_list

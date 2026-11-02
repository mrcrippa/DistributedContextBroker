import random
import threading
from dataclasses import dataclass

from broker_messages.broker_message_classes import BrokerNeighborsRequestMessage
from global_data import broker_internal_clock, broker_logger, this_broker_info
from message_exchange.message_sender import send_message


@dataclass
class BrokerNeighborInfo(object):
    """
        Class representing an entry in the broker neighbors table
    """

    broker_neighbor_id: str
    broker_neighbor_address: str
    broker_neighbor_port: int
    # timestamp of the last contact with this neighbor
    # use 0 for neighbor considered offline, != 0 for neighbor considered online
    broker_neighbor_last_online_timestamp: int


class BrokerNeighborTable(object):
    """
    Class representing a table with all information about this broker's neighbors
    """

    def __init__(self):
        self._table = {}
        self._table_lock = threading.Lock()

    def set_entry_online(self, broker_neighbor_id: str):
        """ searches for and marks a neighbor as online, if the neighbor does not exist, nothing happens """
        self._table_lock.acquire()
        if broker_neighbor_id is not None and broker_neighbor_id in self._table:
            self._table[
                broker_neighbor_id].broker_neighbor_last_online_timestamp = broker_internal_clock.current_broker_time()
        self._table_lock.release()

    def add_entry(self, neighbor: BrokerNeighborInfo):
        if neighbor is not None:
            self._table_lock.acquire()
            if len(self._table) <= 20:
                self._table[neighbor.broker_neighbor_id] = neighbor
            self._table_lock.release()

    def remove_entry(self, broker_neighbor_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        self._table_lock.acquire()
        if broker_neighbor_id is not None and broker_neighbor_id in self._table:
            del self._table[broker_neighbor_id]
        self._table_lock.release()

    def search_entry(self, broker_neighbor_id: str) -> BrokerNeighborInfo | None:
        """searches for any matching neighbor from the table"""
        self._table_lock.acquire()
        result = None
        if broker_neighbor_id is not None and broker_neighbor_id in self._table:
            result = self._table[broker_neighbor_id]
        self._table_lock.release()
        return result

    def check_table_for_potentially_offline_brokers(self):
        """checks which neighbors might be potentially offline, and sends them a neighbors_request"""
        self._table_lock.acquire()

        neighbor_request_msg = BrokerNeighborsRequestMessage(broker_id=this_broker_info["id"],
                                                             broker_address=this_broker_info["address"],
                                                             broker_port=this_broker_info["port"])

        for entry in self._table.values():

            # time elapsed since the last contact with this neighbor
            time_elapsed = broker_internal_clock.current_broker_time() - entry.broker_neighbor_last_online_timestamp
            # if time_elapsed less than 15s, neighbor is considered online
            # between 15s and 30s, it's potentially offline
            # more than 30s, it's considered offline
            if 15000 < time_elapsed < 30000:
                send_message(neighbor_request_msg.as_json(), this_broker_info["address"],
                             (entry.broker_neighbor_address, entry.broker_neighbor_port))
            elif time_elapsed > 30000:
                self.remove_entry(entry.broker_neighbor_id)

        self._table_lock.release()

    def get_small_flooding_list(self, checked_brokers: list) -> list:
        """returns a list of neighbors to be used for the broker subscribe procedure
           the first neighbor on the list is to be used for the random walk, the others should not forward their requests
        """
        result = []

        broker_logger.debug(str(self.get_table_as_list_of_dict()))

        self._table_lock.acquire()
        to_be_verified = list(self._table.keys())

        for entry in checked_brokers:
            if entry in to_be_verified:
                to_be_verified.remove(entry)

        while to_be_verified and len(result) <= 5:
            selected = random.choice(to_be_verified)
            to_be_verified.remove(selected)
            if self._table[selected].broker_neighbor_last_online_timestamp:
                result.append(self._table[selected])
        self._table_lock.release()

        return result

    def get_table_as_list_of_dict(self) -> list:
        """returns the table as a list, only neighbors that are online"""
        temp_list = []
        self._table_lock.acquire()
        for entry in self._table.values():
            if entry.broker_neighbor_last_online_timestamp:
                temp_dict = {"broker_id": entry.broker_neighbor_id,
                             "broker_address": entry.broker_neighbor_address,
                             "broker_port": entry.broker_neighbor_port}
                temp_list.append(temp_dict)
        self._table_lock.release()

        return temp_list

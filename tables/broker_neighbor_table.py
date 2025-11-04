import json
import random
import threading

from global_data import broker_clock, broker_logger
from utils.message_sender import send_message


class BrokerNeighborInfo(object):
    """
        Class representing an entry in the broker neighbors table
    """

    def __init__(self, neighbour_id: str, neighbour_address: str, neighbour_port: int):
        self._broker_neighbour_id = neighbour_id
        self._broker_neighbour_address = neighbour_address
        self._broker_neighbour_port = neighbour_port
        self._broker_neighbour_online = False
        self._broker_neighbour_last_online_timestamp = 0

    def set_broker_neighbour_online(self, status: bool):
        self._broker_neighbour_online = status
        if status:
            self._broker_neighbour_last_online_timestamp = broker_clock.current_broker_time()
        else:
            self._broker_neighbour_last_online_timestamp = 0

    def is_broker_neighbour_potentially_offline(self) -> bool:
        if self._broker_neighbour_online:
            time_elapsed = broker_clock.current_broker_time() - self._broker_neighbour_last_online_timestamp
            if 15000 < time_elapsed < 30000:
                return True
            else:
                if time_elapsed > 30000:
                    self.set_broker_neighbour_online(False)
                return False
        else:
            return False

    def is_broker_neighbour_online(self) -> bool:
        return self._broker_neighbour_online

    def get_broker_neighbour_id(self) -> str:
        return self._broker_neighbour_id

    def get_broker_neighbour_address(self) -> str:
        return self._broker_neighbour_address

    def get_broker_neighbour_port(self) -> int:
        return self._broker_neighbour_port

    def print_entry(self) -> str:
        result = "[" + self._broker_neighbour_id + ", " + self._broker_neighbour_address + ", " + str(
            self._broker_neighbour_port) + ", " + str(self._broker_neighbour_online) + ", " + str(
            self._broker_neighbour_last_online_timestamp) + "]"
        return result


class BrokerNeighborTable(object):
    """
    Class representing a table with all information about this broker neighbors
    """

    def __init__(self):
        self._table = {}
        self._table_lock = threading.Lock()

    def set_entry_online(self, broker_neighbour_id: str):
        """ searches for and marks a neighbour as online, if the neighbour does not exist, nothing happens """
        self._table_lock.acquire()
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            self._table[broker_neighbour_id].set_broker_neighbour_online(True)
        self._table_lock.release()

    def add_entry(self, neighbor: BrokerNeighborInfo):
        if neighbor is not None:
            self._table_lock.acquire()
            self._table[neighbor.get_broker_neighbour_id()] = neighbor
            self._table_lock.release()

    def remove_entry(self, broker_neighbour_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        self._table_lock.acquire()
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            del self._table[broker_neighbour_id]
        self._table_lock.release()

    def search_entry(self, broker_neighbour_id: str) -> BrokerNeighborInfo | None:
        """searches for any matching neighbor from the table"""
        self._table_lock.acquire()
        result = None
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            result = self._table[broker_neighbour_id]
        self._table_lock.release()
        return result

    def check_table_for_potentially_offline_brokers(self, this_broker_info: dict):
        """checks which neighbours might be potentially offline, and sends them a neighbours_request"""
        self._table_lock.acquire()
        msg_string = json.dumps(
            {"TYPE": "BROKER_NEIGHBORS_REQUEST", "BROKER_ID": this_broker_info["id"],
             "BROKER_ADDRESS": this_broker_info["address"],
             "BROKER_PORT": this_broker_info["port"]})
        for entry in self._table.values():
            if entry.is_broker_neighbour_potentially_offline():
                send_message(msg_string, this_broker_info["address"],
                             (entry.get_broker_neighbour_address(), entry.get_broker_neighbour_port()))
        self._table_lock.release()

    def get_small_flooding_list(self, checked_brokers: list) -> list:
        """returns a list of neighbours to be used for the broker subscribe procedure
           the first neighbours on the list is to be used for the random walk, the others should not forward their requests
        """
        result = []

        broker_logger.debug(self.get_table())

        self._table_lock.acquire()
        to_be_verified = list(self._table.keys())

        for entry in checked_brokers:
            if entry in to_be_verified:
                to_be_verified.remove(entry)

        while to_be_verified:
            selected = random.choice(to_be_verified)
            to_be_verified.remove(selected)
            if self._table[selected].is_broker_neighbour_online():
                result.append(self._table[selected])
        self._table_lock.release()

        return result

    def get_table(self) -> list:
        """returns the table as a list"""
        temp_list = []
        self._table_lock.acquire()
        for entry in self._table.values():
            if entry.is_broker_neighbour_online():
                temp_dict = {"BROKER_ID": entry.get_broker_neighbour_id(),
                             "BROKER_ADDRESS": entry.get_broker_neighbour_address(),
                             "BROKER_PORT": entry.get_broker_neighbour_port()}
                temp_list.append(temp_dict)
        self._table_lock.release()

        return temp_list

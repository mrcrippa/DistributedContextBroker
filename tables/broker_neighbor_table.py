import json
import random
import threading

from global_data import broker_clock
from utils.misc_utils import is_string_valid_int_bigger_than_zero


class BrokerNeighborInfo(object):
    """
        Class representing an entry in the broker neighbors table
    """

    def __init__(self):
        self._broker_neighbour_id = ""
        self._broker_neighbour_address = ""
        self._broker_neighbour_port = 0
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

    def set_broker_neighbour_id(self, broker_id: str):
        if broker_id is not None:
            self._broker_neighbour_id = broker_id

    def get_broker_neighbour_address(self) -> str:
        return self._broker_neighbour_address

    def set_broker_neighbour_address(self, broker_address: str):
        if broker_address is not None:
            self._broker_neighbour_address = broker_address

    def get_broker_neighbour_port(self) -> int:
        return self._broker_neighbour_port

    def set_broker_neighbour_port(self, broker_port: str):
        if broker_port is not None and is_string_valid_int_bigger_than_zero(broker_port):
            self._broker_neighbour_port = int(broker_port)

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
            self._table[broker_neighbour_id].set_broker_neighbour_online()
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
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            return self._table[broker_neighbour_id]
        self._table_lock.release()
        return None

    def check_table_for_potentially_offline_brokers(self) -> list:
        """returns a list with all the entries in the table"""
        result = []
        self._table_lock.acquire()
        for entry in self._table.values():
            if entry.is_broker_neighbour_potentially_offline():
                result.append((entry.get_broker_neighbour_address(), entry.get_broker_neighbour_port()))
        self._table_lock.release()
        return result

    def get_small_flooding_list(self, checked_brokers: list) -> list:
        """returns a list of neighbours to be used for the broker subscribe procedure
           the first neighbours on the list is to be used for the random walk, the others should not forward their requests
        """
        result = []

        to_be_verified = list(self._table.keys())

        for entry in checked_brokers:
            to_be_verified.remove(entry)

        self._table_lock.acquire()
        while to_be_verified:
            selected = random.choice(to_be_verified)
            to_be_verified.remove(selected)
            if self._table[selected].is_broker_neighbour_online():
                result.append(self._table[selected])
        self._table_lock.release()

        return result

    def print_table(self) -> str:
        """returns the table as a JSON string"""
        temp_list = []
        self._table_lock.acquire()
        for entry in self._table.values():
            if entry.is_broker_neighbour_potentially_offline():
                continue
            if entry.is_broker_neighbour_online():
                temp_dict = {"broker_id": entry.get_broker_neighbour_id(),
                             "broker_address": entry.get_broker_neighbour_address(),
                             "broker_port": str(entry.get_broker_neighbour_port())}
                temp_list.append(temp_dict)
        self._table_lock.release()

        return json.dumps(temp_list)

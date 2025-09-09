import json

from tables.broker_neighbor_info import BrokerNeighborInfo


class BrokerNeighborTable(object):
    """
    Class representing a table with all information about this broker neighbors
    """

    def __init__(self):
        self._table = {}

    def set_entry_online(self, broker_neighbour_id: str):
        """ searches for and marks a neighbour as online, if the neighbour does not exist, nothing happens """
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            self._table[broker_neighbour_id].set_broker_neighbour_online()

    def add_entry(self, neighbor: BrokerNeighborInfo):
        if neighbor is not None:
            self._table[neighbor.get_broker_neighbour_id()] = neighbor

    def remove_entry(self, broker_neighbour_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            del self._table[broker_neighbour_id]

    def search_entry(self, broker_neighbour_id: str) -> BrokerNeighborInfo | None:
        """searches for any matching neighbor from the table"""
        if broker_neighbour_id is not None and broker_neighbour_id in self._table:
            return self._table[broker_neighbour_id]
        return None

    def get_table(self) -> list:
        """returns a list with all the entries in the table"""
        result = []
        for entry in self._table.values():
            result.append(entry)
        return result


    def print_table(self) -> str:
        """returns the table as a JSON string"""
        temp_list = []
        for entry in self._table.values():
            if entry.is_broker_neighbour_potentially_offline():
                continue
            if entry.is_broker_neighbour_online():
                temp_dict = {"broker_id": entry.get_broker_neighbour_id(), "broker_address": entry.get_broker_neighbour_address(),
                         "broker_port": str(entry.get_broker_neighbour_port())}
                temp_list.append(temp_dict)

        return json.dumps(temp_list)


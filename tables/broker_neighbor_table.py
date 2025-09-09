import json

from tables.broker_neighbor_info import BrokerNeighborInfo


class BrokerNeighborTable(object):
    """
    Class representing a table with all information about this broker neighbors
    """

    def __init__(self):
        self._table = {}

    def add_entry(self, neighbor: BrokerNeighborInfo):
        if neighbor is not None:
            self._table[neighbor.get_broker_id()] = neighbor

    def remove_entry(self, broker_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        if broker_id is not None and broker_id in self._table:
            del self._table[broker_id]

    def search_entry(self, broker_id: str) -> BrokerNeighborInfo | None:
        """searches for any matching neighbor from the table"""
        if broker_id is not None and broker_id in self._table:
            return self._table[broker_id]
        return None

    def get_table(self) -> list:
        result = []
        for entry in self._table.values():
            result.append(entry)
        return result


    def print_table(self) -> str:
        """returns the table as a JSON string"""
        temp_list = []
        for entry in self._table.values():
            temp_dict = {"broker_id": entry.get_broker_id(), "broker_address": entry.get_broker_address(),
                         "broker_port": str(entry.get_broker_port())}
            temp_list.append(temp_dict)

        return json.dumps(temp_list)


import time


def check_if_string_is_valid_integer_bigger_than_zero(value: str) -> bool:
    """check if a string is a valid integer bigger than zero"""
    result = False
    try:
        s = int(value)
        if s > 0:
            result = True
    except ValueError:
        result = False
    return result


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
            self._broker_neighbour_last_online_timestamp = int(round(time.time() * 1000))
        else:
            self._broker_neighbour_last_online_timestamp = 0

    def is_broker_neighbour_potentially_offline(self) -> bool:
        if self._broker_neighbour_online:
            current_timestamp = int(round(time.time() * 1000))
            time_elapsed = current_timestamp - self._broker_neighbour_last_online_timestamp
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
        if broker_port is not None and check_if_string_is_valid_integer_bigger_than_zero(broker_port):
            self._broker_neighbour_port = int(broker_port)

    def print_entry(self) -> str:
        result = "[" + self._broker_neighbour_id + ", " + self._broker_neighbour_address + ", " + str(
            self._broker_neighbour_port) + ", " + str(self._broker_neighbour_online) + ", " + str(
            self._broker_neighbour_last_online_timestamp) + "]"
        return result


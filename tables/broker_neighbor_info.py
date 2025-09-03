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
        self._broker_id = ""
        self._broker_address = ""
        self._broker_port = 0

    def get_broker_id(self) -> str:
        return self._broker_id

    def set_broker_id(self, broker_id: str):
        if broker_id is not None:
            self._broker_id = broker_id

    def get_broker_address(self) -> str:
        return self._broker_address

    def set_broker_address(self, broker_address: str):
        if broker_address is not None:
            self._broker_address = broker_address

    def get_broker_port(self) -> int:
        return self._broker_port

    def set_broker_port(self, broker_port: str):
        if broker_port is not None and check_if_string_is_valid_integer_bigger_than_zero(broker_port):
            self._broker_port = int(broker_port)

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


class Subscription(object):
    """
        Class representing an entry in the subscription table
    """

    def __init__(self):
        self._entity_id = ""
        self._entity_type = ""
        self._scope = ""
        self._subscriber_address = ""
        self._subscriber_port = 0
        "for how many milliseconds should this subscription be valid"
        self._duration = 0

    def get_object_id(self) -> str:
        """unique identifier for this object"""
        return str(hash(
            self._entity_id + self._entity_type + self._scope + self._subscriber_address + str(self._subscriber_port)))

    def get_entity_id(self) -> str:
        return self._entity_id

    def set_entity_id(self, entity_id: str):
        if entity_id is not None:
            self._entity_id = entity_id

    def get_entity_type(self) -> str:
        return self._entity_type

    def set_entity_type(self, entity_type: str):
        if entity_type is not None:
            self._entity_type = entity_type

    def get_scope(self) -> str:
        return self._scope

    def set_scope(self, scope: str):
        if scope is not None:
            self._scope = scope

    def get_subscriber_address(self) -> str:
        return self._subscriber_address

    def set_subscriber_address(self, subscriber_address: str):
        if subscriber_address is not None:
            self._subscriber_address = subscriber_address

    def get_subscriber_port(self) -> int:
        return self._subscriber_port

    def set_subscriber_port(self, subscriber_port: str):
        if subscriber_port is not None and check_if_string_is_valid_integer_bigger_than_zero(subscriber_port):
            self._subscriber_port = int(subscriber_port)

    def get_duration(self) -> int:
        """for how many milliseconds should this subscription be valid"""
        return self._duration

    def set_duration(self, duration: str):
        """for how many milliseconds should this subscription be valid"""
        if duration is not None and check_if_string_is_valid_integer_bigger_than_zero(duration):
            self._duration = int(duration)

    def print(self) -> str:
        contents = {"SUBSCRIPTION_ID": self.get_object_id(),
                    "ENTITY_ID": self.get_entity_id(), "ENTITY_TYPE": self.get_entity_type(), "SCOPE": self.get_scope(),
                    "SUBSCRIBER_ADDRESS": self.get_subscriber_address(),
                    "SUBSCRIBER_PORT": str(self.get_subscriber_port()),
                    "DURATION": str(self.get_duration())}

        return str(contents)

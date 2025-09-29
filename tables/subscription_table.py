from utils.misc_utils import is_string_valid_int_bigger_than_zero


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
        "for how many seconds should this subscription be valid"
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
        if subscriber_port is not None and is_string_valid_int_bigger_than_zero(subscriber_port):
            self._subscriber_port = int(subscriber_port)

    def get_duration(self) -> int:
        """for how many seconds should this subscription be valid"""
        return self._duration

    def set_duration(self, duration: str):
        """for how many seconds should this subscription be valid"""
        if duration is not None and is_string_valid_int_bigger_than_zero(duration):
            self._duration = int(duration)

    def print(self) -> str:
        contents = {"SUBSCRIPTION_ID": self.get_object_id(),
                    "ENTITY_ID": self.get_entity_id(), "ENTITY_TYPE": self.get_entity_type(), "SCOPE": self.get_scope(),
                    "SUBSCRIBER_ADDRESS": self.get_subscriber_address(),
                    "SUBSCRIBER_PORT": str(self.get_subscriber_port()),
                    "DURATION": str(self.get_duration())}

        return str(contents)


class SubscriptionTable(object):
    """
    Class representing a table with all the subscriptions to context information
    """

    def __init__(self):
        self._table = {}

    def add_entry(self, sub: Subscription):
        if sub is not None:
            self._table[sub.get_object_id()] = sub

    def remove_entry(self, entry_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        if entry_id is not None and entry_id in self._table:
            del self._table[entry_id]

    def search_entry(self, entity_id: str, entity_type: str, scope: str) -> list:
        """searches for any matching subscription from the table, if one or more exist, returns a list of subscriptions
                 if not entry exist, then returns an empty list"""
        result_list = []
        if entity_id is not None and entity_type is not None and scope is not None:
            for sub in self._table.values():
                if sub.get_entity_id() == entity_id and sub.get_entity_type() == entity_type and sub.get_scope() == scope:
                    result_list.append(sub)
        return result_list

    def print(self) -> str:
        result = ""
        for sub in self._table.values():
            result += '\n' + sub.print() + " ||"
        return result

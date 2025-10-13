import threading

from global_data import broker_logger
from utils.parsed_message import ParsedMessage


class Subscription(object):
    """
        Class representing an entry in the subscription table
    """

    def __init__(self, rcv_msg: ParsedMessage):
        self._entity_id = rcv_msg.get_token("ENTITY_ID")
        self._entity_type = rcv_msg.get_token("ENTITY_TYPE")
        self._scope = rcv_msg.get_token("SCOPE")
        self._subscriber_address = rcv_msg.get_token("SUBSCRIBER_ADDRESS")
        self._subscriber_port = int(rcv_msg.get_token("SUBSCRIBER_PORT"))
        "for how many seconds should this subscription be valid"
        self._duration = int(rcv_msg.get_token("DURATION"))

    def get_object_id(self) -> str:
        """unique identifier for this object"""
        return str(hash(
            self._entity_id + self._entity_type + self._scope + self._subscriber_address + str(self._subscriber_port)))

    def get_entity_id(self) -> str:
        return self._entity_id

    def get_entity_type(self) -> str:
        return self._entity_type

    def get_scope(self) -> str:
        return self._scope

    def get_subscriber_address(self) -> str:
        return self._subscriber_address

    def get_subscriber_port(self) -> int:
        return self._subscriber_port

    def get_duration(self) -> int:
        """for how many seconds should this subscription be valid"""
        return self._duration

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
        self._table_lock = threading.Lock()
        # timer threads associated with each subscription, when the subscription is no longer valid, it will call this thread and delete the subscription
        self._sub_timers = {}

    def add_entry(self, sub: Subscription):
        if sub is not None:
            self._table_lock.acquire()
            self._table[sub.get_object_id()] = sub
            self._sub_timers[sub.get_object_id()] = threading.Timer(float(sub.get_duration()), sub_timer_thread,
                                                                    [sub.get_object_id(), self])
            self._sub_timers[sub.get_object_id()].start()
            self._table_lock.release()

    def remove_entry(self, entry_id: str):
        """removes an entry from the table, if the entry doesn't exist, then nothing happens"""
        self._table_lock.acquire()
        if entry_id is not None and entry_id in self._table:
            self._sub_timers[entry_id].cancel()
            del self._table[entry_id]
        self._table_lock.release()

    def search_entry(self, entity_id: str, entity_type: str, scope: str) -> list:
        """searches for any matching subscription from the table, if one or more exist, returns a list of subscriptions
                 if not entry exist, then returns an empty list"""
        result_list = []
        if entity_id is not None and entity_type is not None and scope is not None:
            self._table_lock.acquire()
            for sub in self._table.values():
                if sub.get_entity_id() == entity_id and sub.get_entity_type() == entity_type and sub.get_scope() == scope:
                    result_list.append(sub)
            self._table_lock.release()
        return result_list

    def print(self) -> str:
        result = ""
        self._table_lock.acquire()
        for sub in self._table.values():
            result += '\n' + sub.print() + " ||"
        self._table_lock.release()
        return result


def sub_timer_thread(entry_id: str, s_table: SubscriptionTable):
    """timer thread used to delete subscriptions after their duration"""
    s_table.remove_entry(entry_id)
    broker_logger.info("sub_table : " + s_table.print())

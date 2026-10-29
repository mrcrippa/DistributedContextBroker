import json
import threading
from dataclasses import field, dataclass

from broker_messages.context_response_message import ContextResponseMessage
from context_management.context_info_table import ContextInfo
from global_data import broker_logger
from message_exchange.message_sender import send_message


@dataclass
class Subscription(object):
    """
        Class representing an entry in the subscription table
    """

    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    # for how many seconds should this subscription be valid - integer > 0
    duration: int
    object_id: str = field(init=False)

    def __post_init__(self):
        self.object_id = str(hash(
            self.entity_id + self.entity_type + self.scope + self.subscriber_address + str(self.subscriber_port)))

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


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
            self._table[sub.object_id] = sub
            self._sub_timers[sub.object_id] = threading.Timer(float(sub.duration), sub_timer_thread,
                                                              [sub.object_id, self])
            self._sub_timers[sub.object_id].start()
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
                if sub.entity_id == entity_id and sub.entity_type == entity_type and sub.scope == scope:
                    result_list.append(sub)
            self._table_lock.release()
        return result_list

    def notify_subscribers(self, this_broker_info: dict, ctx_data: ContextInfo):
        # if there are any subscriptions to this data, then notify all subscribers
        self._table_lock.acquire()
        for sub in self._table.values():
            if sub.entity_id == ctx_data.entity_id and sub.entity_type == ctx_data.entity_type and sub.scope == ctx_data.scope:
                # broker_logger.debug(
                #  "sending data to : " + str((sub.get_subscriber_address(), sub.get_subscriber_port())))
                context_response_msg = ContextResponseMessage(broker_id=this_broker_info["id"],
                                                              broker_address=this_broker_info["address"],
                                                              broker_port=this_broker_info["port"],
                                                              entity_id=ctx_data.entity_id,
                                                              entity_type=ctx_data.entity_type,
                                                              provider_id=ctx_data.provider_id,
                                                              provider_address=ctx_data.provider_address,
                                                              provider_port=ctx_data.provider_port,
                                                              scope=ctx_data.scope,
                                                              data=ctx_data.data,
                                                              validity_end=ctx_data.validity_end,
                                                              well_formed=True)

                send_message(context_response_msg.as_json(), this_broker_info["address"],
                             (sub.subscriber_address, sub.subscriber_port))

        self._table_lock.release()

    def print(self) -> str:
        result = ""
        self._table_lock.acquire()
        for sub in self._table.values():
            result += '\n' + sub + " ||"
        self._table_lock.release()
        return result


def sub_timer_thread(entry_id: str, s_table: SubscriptionTable):
    """timer thread used to delete subscriptions after their duration"""
    s_table.remove_entry(entry_id)
    broker_logger.info("sub_table : " + s_table.print())

"""
This module contains classes defining the subscription table, and a dataclass defining an entry in the table

Classes
--------
Subscription
SubscriptionTable
"""

import threading
from dataclasses import field, dataclass

from broker_messages.broker_message_classes import ContextInfoMessage
from context_management.context_info_table import ContextInfo
from global_data import broker_logger, this_broker_info
from message_exchange.message_sender import send_message


@dataclass
class Subscription(object):
    """
            Class that defines a subscription to context information stored in this broker
            and represents an entry in the subscription table

            A unique object identifier is generated after an instance is created. This identifier is used to index
            the subscription table

            Attributes
            ----------
            entity_id : str
                unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
            entity_type : str
                    specifies the type of entity (for instance, "city" or "person")
            scope : str
                    specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")
            subscriber_address : str
                address for the subscriber
            subscriber_port : int
                port for the subscriber
            duration : int
                for how many seconds should this subscription active - should be an integer > 0
            object_id : str
                a unique identifier for an instance of this dataclass. It's initialized automatically after instantiation
        """

    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    duration: int
    # unique identifier for this object
    object_id: str = field(init=False)

    def __post_init__(self):
        """Sets the unique identifier for an instance of this dataclass based on entity_id, entity_type,scope, subscriber_address and subscriber_port"""
        self.object_id = str(hash(
            self.entity_id + self.entity_type + self.scope + self.subscriber_address + str(self.subscriber_port)))


class SubscriptionTable(object):
    """
        Class representing a table with all the subscriptions to context information

        Attributes
        ----------
        _table : dict
            dict with all the Subscription objects, representing all the subscriptions. The keys come from the
            object_id attribute from Subscription
        _table_lock : Lock
            mutex Lock used to manage simultaneous access from multiple threads
        _sub_timers : dict
            since each Subscription has an expiration time (duration), each entry has an associated Timer thread that will
            delete the entry when it's expired
        """

    def __init__(self):
        self._table = {}
        self._table_lock = threading.Lock()
        self._sub_timers = {}

    def _add_entry(self, sub: Subscription):
        """
            Internal version of add_entry, for thread safety

            Parameters
            ----------
            sub: Subscription
                the subscription object to be added
        """
        self._table[sub.object_id] = sub
        self._sub_timers[sub.object_id] = threading.Timer(float(sub.duration), sub_timer_thread,
                                                          [sub.object_id, self])
        self._sub_timers[sub.object_id].start()

    def add_entry(self, sub: Subscription):
        """
            Adds an entry to the subscription table

            Parameters
            ----------
            sub: Subscription
                the subscription object to be added
        """

        if sub is not None:
            with self._table_lock:
                self._add_entry(sub)

    def _remove_entry(self, entry_id: str):
        """
            Internal version of remove_entry, for thread safety

            Parameters
            ----------
            entry_id: str
                object id of the entry to be removed
        """
        if entry_id is not None and entry_id in self._table:
            # cancels the Timer thread associated with this entry
            self._sub_timers[entry_id].cancel()
            del self._table[entry_id]

    def remove_entry(self, entry_id: str):
        """
            Deletes an entry from the table

            If an entry with the given id doesn't exist, then nothing happens

            Parameters
            ----------
            entry_id: str
                object id of the entry to be removed
        """
        with self._table_lock:
            self._remove_entry(entry_id)

    def _notify_subscribers(self, ctx_data: ContextInfo):
        """
           Internal version of notify_subscribers, for thread safety

            Parameters
            ----------
            ctx_data: ContextInfo
                the new context information received by the broker
        """
        for sub in self._table.values():
            if sub.entity_id == ctx_data.entity_id and sub.entity_type == ctx_data.entity_type and sub.scope == ctx_data.scope:
                context_info_msg = ContextInfoMessage(broker_id=this_broker_info["id"],
                                                      broker_address=this_broker_info["address"],
                                                      broker_port=this_broker_info["port"],
                                                      entity_id=ctx_data.entity_id,
                                                      entity_type=ctx_data.entity_type,
                                                      provider_id=ctx_data.provider_id,
                                                      provider_address=ctx_data.provider_address,
                                                      provider_port=ctx_data.provider_port,
                                                      scope=ctx_data.scope,
                                                      data=ctx_data.data,
                                                      validity_end=ctx_data.validity_end)

                send_message(context_info_msg.as_json(), this_broker_info["address"],
                             (sub.subscriber_address, sub.subscriber_port))

    def notify_subscribers(self, ctx_data: ContextInfo):
        """
        A new update was sent to the broker, and this function searches for and notifies all context consumers
        subscribed to this information

        More info about ContextInfo can be found in context_management.context_info_table.ContextInfo

        Parameters
        ----------
        ctx_data: ContextInfo
           the new context information received by the broker
        """

        with self._table_lock:
            self._notify_subscribers(ctx_data)

    def _print(self) -> str:
        """
        Internal version of print, for thread safety

        Returns
        -------
        str
            string showing the contents of the table
        """
        result = ""
        for entry in self._table.values():
            result += '\n' + str(entry) + " ||"
        return result

    def print(self) -> str:
        """
            Utility function, inserts the contents of the subscription table into a string

            Helpful for debugging

            Returns
            -------
            str
                string showing the contents of the table
        """
        result = ""
        with self._table_lock:
            result = self._print()
        return result


def sub_timer_thread(entry_id: str, s_table: SubscriptionTable):
    """
        Function called by a Timer thread to delete subscriptions after their duration

        Parameters
        ----------
        entry_id: str
            object id of the entry to be deleted
        s_table: SubscriptionTable
            reference to the subscription table
    """
    s_table.remove_entry(entry_id)
    broker_logger.info("sub_table : " + s_table.print())

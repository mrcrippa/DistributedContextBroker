import threading

from global_data import sub_table_lock, sub_table, sub_timer_threads_lock, sub_timer_threads, logger
from tables.subscription import Subscription
from utils.received_message import ReceivedMessage


def sub_timer_thread(entry_id: str):
    """timer thread used to delete subscriptions after their duration"""
    # delete entry in the subscription table
    sub_table_lock.acquire()
    sub_table.remove_entry(entry_id)
    sub_table_lock.release()

    # delete the timer thread associated with that subscription
    sub_timer_threads_lock.acquire()
    if entry_id is not None and entry_id in sub_timer_threads:
        del sub_timer_threads[entry_id]
    sub_timer_threads_lock.release()

    logger.info("sub_table : " + sub_table.print())
    logger.info("sub_timer_threads : " + str(sub_timer_threads))


def add_subscription(rcv_msg: ReceivedMessage) -> Subscription:
    """adds a subscription to the subscription table, received message was already verified and it's well-formed"""

    # create subscription object
    sub = Subscription()
    sub.set_entity_id(rcv_msg.get_message_token("ENTITY_ID"))
    sub.set_entity_type(rcv_msg.get_message_token("ENTITY_TYPE"))
    sub.set_scope(rcv_msg.get_message_token("SCOPE"))
    sub.set_subscriber_address(rcv_msg.get_message_token("SUBSCRIBER_ADDRESS"))
    sub.set_subscriber_port(rcv_msg.get_message_token("SUBSCRIBER_PORT"))
    sub.set_duration(rcv_msg.get_message_token("DURATION"))

    sub_table_lock.acquire()
    # remove any subscription already there, since this will replace it
    # if no subscription exists, then nothing happens
    sub_table.remove_entry(sub.get_object_id())
    # add new subscription
    sub_table.add_entry(sub)
    sub_table_lock.release()

    # remove any associated timer thread
    sub_timer_threads_lock.acquire()
    if sub.get_object_id() in sub_timer_threads:
        sub_timer_threads[sub.get_object_id()].cancel()
        del sub_timer_threads[sub.get_object_id()]

    # create new timer thread
    sub_timer_threads[sub.get_object_id()] = threading.Timer(sub.get_duration() / 1000, sub_timer_thread, [sub.get_object_id()])
    sub_timer_threads[sub.get_object_id()].start()
    sub_timer_threads_lock.release()

    return sub

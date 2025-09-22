import socket
import threading

from global_data import sub_table_lock, sub_table, sub_timer_threads_lock, sub_timer_threads, logger, ctx_table, \
    ctx_table_lock, broker_neighbors_lock, broker_neighbors
from tables.subscription_table import Subscription
from utils.message_creator import create_ack_message_string, create_context_response_message_string, \
    create_broker_subscribe_message_string
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


def process_subscription(rcv_msg: ReceivedMessage, sender_addr: tuple, this_broker_info: dict,
                         broker_sock: socket.socket, broker_sock_lock: threading.Lock):
    """adds a subscription to the subscription table, received message was already verified and it's well-formed"""

    # check if there is already data in the context info table matching the subscription
    ctx_table_lock.acquire()
    current_ctx_info = ctx_table.search_entry(rcv_msg.get_token("ENTITY_ID"),
                                              rcv_msg.get_token("ENTITY_TYPE"),
                                              rcv_msg.get_token("SCOPE"))
    ctx_table_lock.release()

    # create subscription object
    sub = Subscription()
    sub.set_entity_id(rcv_msg.get_token("ENTITY_ID"))
    sub.set_entity_type(rcv_msg.get_token("ENTITY_TYPE"))
    sub.set_scope(rcv_msg.get_token("SCOPE"))
    sub.set_subscriber_address(rcv_msg.get_token("SUBSCRIBER_ADDRESS"))
    sub.set_subscriber_port(rcv_msg.get_token("SUBSCRIBER_PORT"))
    sub.set_duration(rcv_msg.get_token("DURATION"))

    if current_ctx_info is not None:

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
        sub_timer_threads[sub.get_object_id()] = threading.Timer(sub.get_duration() / 1000, sub_timer_thread,
                                                                 [sub.get_object_id()])
        sub_timer_threads[sub.get_object_id()].start()
        sub_timer_threads_lock.release()

        logger.info("sub_table : " + sub_table.print())
        logger.info("sub_timer_threads : " + str(sub_timer_threads))

        broker_sock_lock.acquire()
        # notify the subscriber that the subscription was successfully done, sending an ACK message
        broker_sock.sendto(str.encode(create_ack_message_string(this_broker_info, "SUBSCRIBE", str(sender_addr),
                                                                "Consumer subscribed to [ " + sub.get_entity_id() + " , " + sub.get_entity_type() + " , " + sub.get_scope() + " ]")),
                           (sub.get_subscriber_address(), sub.get_subscriber_port()))

        # if there is data, then send it to the subscriber
        broker_sock.sendto(str.encode(create_context_response_message_string(this_broker_info, current_ctx_info)),
                           (sub.get_subscriber_address(), sub.get_subscriber_port()))
        broker_sock_lock.release()
    else:

        checked_brokers = [this_broker_info["id"]]
        max_hops = 2

        if rcv_msg.get_token("MAX_HOPS") is not None and rcv_msg.get_token("CHECKED_BROKERS") is not None:
            checked_brokers.append(rcv_msg.get_token("CHECKED_BROKERS"))
            max_hops = int(rcv_msg.get_token("MAX_HOPS")) - 1

        if max_hops > 0:
            broker_neighbors_lock.acquire()
            small_flood = broker_neighbors.get_small_flooding_list(checked_brokers)
            broker_neighbors_lock.release()

            broker_sock_lock.acquire()
            if small_flood:
                broker_sock.sendto(str.encode(
                    create_broker_subscribe_message_string(this_broker_info,
                                                           sub, str(max_hops),
                                                           checked_brokers)),
                    (small_flood[0].get_broker_neighbour_address(),
                     int(small_flood[0].get_broker_neighbour_port())))
            small_flood.pop(0)

            while small_flood:
                broker_sock.sendto(str.encode(
                    create_broker_subscribe_message_string(this_broker_info, sub,
                                                           "0", checked_brokers)),
                    ((small_flood[0].get_broker_neighbour_address(),
                      int(small_flood[0].get_broker_neighbour_port()))))
                small_flood.pop(0)

            broker_sock_lock.release()

import socket
import threading
import time

from global_data import *
from tables.context_info import ContextInfo
from utils.message_creator import create_context_response_message_string, create_ack_message_string
from utils.received_message import ReceivedMessage


def ctx_timer_thread(entry_id: str):
    """timer thread used to delete context table entries after their validity ends"""
    # delete entry in the context info table
    ctx_table_lock.acquire()
    ctx_table.remove_entry(entry_id)
    ctx_table_lock.release()

    # delete the timer thread associated with that entry
    ctx_timer_threads_lock.acquire()
    if entry_id is not None and entry_id in ctx_timer_threads:
        del ctx_timer_threads[entry_id]
    ctx_timer_threads_lock.release()

    logger.info("ctx_table : " + ctx_table.print())
    logger.info("ctx_timer_threads : " + str(ctx_timer_threads))


def process_update_message(rcv_msg: ReceivedMessage, sender_addr: tuple, this_broker_id: str, this_broker_address: str,
                           this_broker_port: str, broker_sock: socket.socket, broker_sock_lock: threading.Lock):
    """adds an entry to the context info table, received message was already verified and it's well-formed"""

    # create ContextInfo object
    ctx_data = ContextInfo()
    ctx_data.set_provider_id(rcv_msg.get_token("PROVIDER_ID"))
    ctx_data.set_entity_id(rcv_msg.get_token("ENTITY_ID"))
    ctx_data.set_entity_type(rcv_msg.get_token("ENTITY_TYPE"))
    ctx_data.set_scope(rcv_msg.get_token("SCOPE"))
    ctx_data.set_validity_begin(rcv_msg.get_token("VALIDITY_BEGIN"))
    ctx_data.set_validity_end(rcv_msg.get_token("VALIDITY_END"))
    ctx_data.set_data(rcv_msg.get_token("DATA"))

    ctx_table_lock.acquire()
    # remove any data already there, since this will replace it
    # if no data exists, then nothing happens
    ctx_table.remove_entry(ctx_data.get_object_id())
    # add new data
    ctx_table.add_entry(ctx_data)
    ctx_table_lock.release()

    # remove any associated timer thread
    ctx_timer_threads_lock.acquire()
    if ctx_data.get_object_id() in ctx_timer_threads:
        ctx_timer_threads[ctx_data.get_object_id()].cancel()
        del ctx_timer_threads[ctx_data.get_object_id()]

    # create new timer thread
    interval_timer = (ctx_data.get_validity_end() - int(round(time.time() * 1000))) / 1000
    ctx_timer_threads[ctx_data.get_object_id()] = threading.Timer(interval_timer, ctx_timer_thread,
                                                                  [ctx_data.get_object_id()])
    ctx_timer_threads[ctx_data.get_object_id()].start()
    ctx_timer_threads_lock.release()

    logger.info("ctx_table : " + ctx_table.print())
    logger.info("ctx_timer_threads : " + str(ctx_timer_threads))

    # send an ACK message confirming the update
    broker_sock_lock.acquire()
    broker_sock.sendto(str.encode(create_ack_message_string(this_broker_id, this_broker_address,
                                                            this_broker_port, "UPDATE", str(sender_addr),
                                                            "Update to [ " + ctx_data.get_entity_id() + " , " + ctx_data.get_entity_type() + " , " + ctx_data.get_scope() + " ]")),
                       sender_addr)

    # if there are any subscriptions to this data, then notify all subscribers
    for sub in sub_table.search_entry(ctx_data.get_entity_id(), ctx_data.get_entity_type(),
                                      ctx_data.get_scope()):
        broker_sock.sendto(str.encode(create_context_response_message_string(ctx_data)),
                           (sub.get_subscriber_address(), sub.get_subscriber_port()))
    broker_sock_lock.release()

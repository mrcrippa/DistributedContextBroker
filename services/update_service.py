import threading

from global_data import *
from tables.context_info_table import ContextInfo
from utils.message_creator import create_context_response_message_string, create_ack_message_string
from utils.message_sender import send_message
from utils.parsed_message import ParsedMessage


def ctx_timer_thread(entry_id: str):
    """timer thread used to delete context table entries after their validity ends"""
    # delete entry in the context info table
    ctx_table.remove_entry(entry_id)

    # delete the timer thread associated with that entry
    ctx_timer_threads_lock.acquire()
    if entry_id is not None and entry_id in ctx_timer_threads:
        del ctx_timer_threads[entry_id]
    ctx_timer_threads_lock.release()

    broker_logger.info("ctx_table : " + ctx_table.print())
    broker_logger.info("ctx_timer_threads : " + str(ctx_timer_threads))


def process_update_message(rcv_msg: ParsedMessage, sender_address: tuple, this_broker_info: dict):
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

    # remove any data already there, since this will replace it
    # if no data exists, then nothing happens
    ctx_table.remove_entry(ctx_data.get_object_id())
    # add new data
    ctx_table.add_entry(ctx_data)

    # remove any associated timer thread
    ctx_timer_threads_lock.acquire()
    if ctx_data.get_object_id() in ctx_timer_threads:
        ctx_timer_threads[ctx_data.get_object_id()].cancel()
        del ctx_timer_threads[ctx_data.get_object_id()]

    # create new timer thread
    interval_timer = (ctx_data.get_validity_end() - broker_clock.current_broker_time()) / 1000
    ctx_timer_threads[ctx_data.get_object_id()] = threading.Timer(interval_timer, ctx_timer_thread,
                                                                  [ctx_data.get_object_id()])
    ctx_timer_threads[ctx_data.get_object_id()].start()
    ctx_timer_threads_lock.release()

    broker_logger.info("ctx_table : " + ctx_table.print())
    broker_logger.info("ctx_timer_threads : " + str(ctx_timer_threads))

    # send an ACK message confirming the update
    send_message(create_ack_message_string(this_broker_info, "UPDATE", str(sender_address),
                                           "Successful update to [ " + ctx_data.get_entity_id() + " , " + ctx_data.get_entity_type() + " , " + ctx_data.get_scope() + " ]"),
                 this_broker_info["address"], sender_address)

    # if there are any subscriptions to this data, then notify all subscribers
    for sub in sub_table.search_entry(ctx_data.get_entity_id(), ctx_data.get_entity_type(),
                                      ctx_data.get_scope()):
        send_message(create_context_response_message_string(this_broker_info, ctx_data), this_broker_info["address"],
                     (sub.get_subscriber_address(), sub.get_subscriber_port()))

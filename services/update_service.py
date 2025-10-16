from global_data import *
from tables.context_info_table import ContextInfo
from utils.message_sender import send_ack

from utils.parsed_message import ParsedMessage


def process_update_message(rcv_msg: ParsedMessage, sender_address: tuple, this_broker_info: dict):
    """adds an entry to the context info table, received message was already verified and it's well-formed"""

    # create ContextInfo object
    ctx_data = ContextInfo(rcv_msg)

    # remove any data already there, since this will replace it
    # if no data exists, then nothing happens
    ctx_table.remove_entry(ctx_data.get_object_id())
    # add new data
    ctx_table.add_entry(ctx_data)
    broker_logger.debug("ctx_table : " + ctx_table.print())
    broker_logger.debug("sub_table : " + sub_table.print())

    # send an ACK message confirming the update
    send_ack(this_broker_info, "UPDATE", str(sender_address),
             "[" + ctx_data.get_entity_id() + "," + ctx_data.get_entity_type() + "," + ctx_data.get_scope() + "]",
             sender_address)

    # if there are any subscriptions to this data, then notify all subscribers
    sub_table.notify_subscribers(this_broker_info, ctx_data)

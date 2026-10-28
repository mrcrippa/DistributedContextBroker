from global_data import *
from context_management.context_info_table import ContextInfo
from message_exchange.ack_message import ACKMessage
from message_exchange.message_sender import send_ack, send_message

from message_exchange.broker_messages import BrokerMessage
from message_exchange.update_message import UpdateMessage


def process_update_message(rcv_msg: UpdateMessage, sender_address: tuple):
    """adds an entry to the context info table, received message was already verified and it's well-formed"""

    if rcv_msg is None  or rcv_msg is not UpdateMessage:
        return

    # create ContextInfo object
    ctx_data = ContextInfo(rcv_msg)

    # remove any data already there, since this will replace it
    # if no data exists, then nothing happens
    ctx_table.remove_entry(ctx_data.get_object_id())
    # add new data
    ctx_table.add_entry(ctx_data)
    # broker_logger.debug("ctx_table : " + ctx_table.print())
    # broker_logger.debug("sub_table : " + sub_table.print())

    # send an ACK message confirming the update

    ack_msg = ACKMessage(
        broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
        broker_port=this_broker_info["port"], requested_operation="UPDATE",
        requester_address=sender_address[0], requester_port=sender_address[1],
        information=f"[{ctx_data.get_entity_id()},{ctx_data.get_entity_type()},{ctx_data.get_scope()}]",
        well_formed=True
    )

    send_message(ack_msg.as_json(), this_broker_info["address"], (ack_msg.requester_address, ack_msg.requester_port))

    # if there are any subscriptions to this data, then notify all subscribers
    sub_table.notify_subscribers(this_broker_info, ctx_data)

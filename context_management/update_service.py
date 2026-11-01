from broker_messages.ack_message import AckMessage
from broker_messages.update_message import UpdateMessage
from context_management.context_info_table import ContextInfo
from global_data import *
from message_exchange.message_sender import send_message


def process_update_message(rcv_msg: UpdateMessage):
    """Adds an entry to the context info table, update message was already verified and it's well-formed"""

    if rcv_msg is None or rcv_msg is not isinstance(rcv_msg, UpdateMessage):
        return

    # create ContextInfo object
    ctx_data = ContextInfo(provider_id=rcv_msg.provider_id, entity_id=rcv_msg.entity_id,
                           entity_type=rcv_msg.entity_type, scope=rcv_msg.scope, data=rcv_msg.data,
                           validity_end=rcv_msg.validity_end, provider_port=rcv_msg.provider_port,
                           provider_address=rcv_msg.provider_address)

    # remove any data already there, since this will replace it
    # if no data exists, then nothing happens
    ctx_table.remove_entry(ctx_data.object_id)

    # add new data
    ctx_table.add_entry(ctx_data)

    # broker_logger.debug("ctx_table : " + ctx_table.print())
    # broker_logger.debug("sub_table : " + sub_table.print())

    # send an ACK message confirming the update
    ack_msg = AckMessage(
        broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
        broker_port=this_broker_info["port"], requested_operation="update",
        requester_address=ctx_data.provider_address, requester_port=ctx_data.provider_port,
        information=f"[{ctx_data.entity_id},{ctx_data.entity_type},{ctx_data.scope}]"
    )

    send_message(ack_msg.as_json(), this_broker_info["address"], (ack_msg.requester_address, ack_msg.requester_port))

    # if there are any subscriptions to this data, then notify all subscribers
    sub_table.notify_subscribers(this_broker_info, ctx_data)

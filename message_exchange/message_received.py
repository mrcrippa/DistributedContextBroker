import json

from global_data import broker_logger, broker_clock, this_broker_info
from message_exchange.ack_message import create_ACK_msg
from message_exchange.nack_message import NACKMessage, create_NACK_msg
from message_exchange.ping_message import create_PING_msg, PINGMessage
from message_exchange.update_message import create_UPDATE_msg
from neighbor_management.neighbour_service import process_broker_neighbors_request, process_broker_neighbors_response
from subscriptions.subscribe_service import process_subscription
from context_management.update_service import process_update_message
from message_exchange.message_sender import send_message
from message_exchange.broker_messages import BrokerMessage


def message_received_service_task(received_raw_data: str, sender_address: tuple):
    """
    When the Broker receives a message, this function is called in a separate thread from main_broker

    It checks if the message is well-formed, then calls the appropriate function for further processing.

    The accepted raw messages are valid JSON strings that will be converted into Python dicts.

    One of the keys of this dict must be "TYPE", which has the following accepted values:
    "ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE", "CONTEXT_RESPONSE",
    "BROKER_NEIGHBORS_REQUEST", "BROKER_NEIGHBORS_RESPONSE", "BROKER_SUBSCRIBE"

    :param received_raw_data: the raw string received from the broker through the UDP socket in main_broker
    :param sender_address: the address of the sender of this message, in format (host,port)
    """

    # check if the raw data is valid JSON string
    verified_msg = None

    try:
        message_tokens = json.loads(received_raw_data)
        msg_type = message_tokens.get("TYPE")

        if msg_type is not None:
            # verify the message further based on the type
            match msg_type:
                case "ACK":
                    verified_msg = create_ACK_msg(message_tokens)
                case "NACK":
                    verified_msg = create_NACK_msg(message_tokens)
                case "PING":
                    verified_msg = create_PING_msg(message_tokens)
                case "SUBSCRIBE":

                case "UPDATE":
                    verified_msg = create_UPDATE_msg(message_tokens)
                case "BROKER_NEIGHBORS_REQUEST":

                case "BROKER_NEIGHBORS_RESPONSE":

                case "BROKER_SUBSCRIBE":

    except json.JSONDecodeError:
        broker_logger.error("Message decode error")

    if verified_msg is not None and verified_msg.well_formed:

        match verified_msg.type:
            case "PING":
                # simple ping message, can be used to check the local time at the broker
                ## broker_logger.debug("PING message received from " + str(sender_address))

                ping_msg = PINGMessage(
                    well_formed=True, broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
                    broker_port=this_broker_info["port"], timestamp=broker_clock.current_broker_time(),
                    requester_address=sender_address[0], requester_port=sender_address[1]
                )

                send_message(ping_msg.as_json(), this_broker_info["address"],
                             (ping_msg.requester_address, ping_msg.requester_port))

            case "SUBSCRIBE" | "BROKER_SUBSCRIBE":
                # a subscription request received from a context consumer or from another broker
                ##broker_logger.debug("SUBSCRIBE or BROKER_SUBSCRIBE message received from " + str(sender_address))
                ##broker_logger.debug("raw data : " + str(received_raw_data))
                process_subscription(verified_msg, this_broker_info)
            case "UPDATE":
                # a context provider wants to update/add context information to this broker
                ## broker_logger.debug("UPDATE message received from " + str(sender_address))
                process_update_message(verified_msg, sender_address)
            case "BROKER_NEIGHBORS_REQUEST":
                # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
                ## broker_logger.debug("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_address))
                process_broker_neighbors_request(verified_msg, this_broker_info)
            case "BROKER_NEIGHBORS_RESPONSE":
                # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
                # broker_logger.debug("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_address))
                process_broker_neighbors_response(verified_msg, this_broker_info)
    else:
        # if the message is not well-formed, send a NACK message
        broker_logger.debug(
            "received message not well formed from" + str(sender_address) + "  raw data : " + str(received_raw_data))

        nack_msg = NACKMessage(
            broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
            broker_port=this_broker_info["port"], requested_operation="MESSAGE_RECEIVED",
            requester_address=sender_address[0], requester_port=sender_address[1],
            information="Received message is not well formed", well_formed=True
        )

        send_message(nack_msg.as_json(), this_broker_info["address"],
                     (nack_msg.requester_address, nack_msg.requester_port))

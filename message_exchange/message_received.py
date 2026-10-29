import json

from broker_messages.ack_message import process_message_tokens_as_ack_msg
from broker_messages.broker_neighbors_request_message import process_message_tokens_as_broker_neighbors_request_msg
from broker_messages.broker_neighbors_response_message import process_message_tokens_as_broker_neighbors_response_msg
from broker_messages.broker_subscribe_message import process_message_tokens_as_broker_subscribe_msg
from broker_messages.nack_message import NackMessage, process_message_tokens_as_nack_msg
from broker_messages.ping_message import process_message_tokens_as_ping_msg, PingMessage
from broker_messages.subscribe_message import process_message_tokens_as_subscribe_msg
from broker_messages.update_message import process_message_tokens_as_update_msg
from context_management.update_service import process_update_message
from global_data import broker_logger, broker_clock, this_broker_info
from message_exchange.message_sender import send_message
from neighbor_management.neighbour_service import process_broker_neighbors_request, process_broker_neighbors_response
from subscriptions.subscribe_service import process_subscription


def message_received(received_raw_data: str, sender_address: tuple):
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
                    verified_msg = process_message_tokens_as_ack_msg(message_tokens)
                case "NACK":
                    verified_msg = process_message_tokens_as_nack_msg(message_tokens)
                case "PING":
                    verified_msg = process_message_tokens_as_ping_msg(message_tokens)
                case "SUBSCRIBE":
                    verified_msg = process_message_tokens_as_subscribe_msg(message_tokens)
                case "UPDATE":
                    verified_msg = process_message_tokens_as_update_msg(message_tokens)
                case "BROKER_NEIGHBORS_REQUEST":
                    verified_msg = process_message_tokens_as_broker_neighbors_request_msg(message_tokens)
                case "BROKER_NEIGHBORS_RESPONSE":
                    verified_msg = process_message_tokens_as_broker_neighbors_response_msg(message_tokens)
                case "BROKER_SUBSCRIBE":
                    verified_msg = process_message_tokens_as_broker_subscribe_msg(message_tokens)

    except json.JSONDecodeError:
        broker_logger.error("Message decode error")

    if verified_msg is not None and verified_msg.well_formed:

        match verified_msg.type:
            case "PING":
                # simple ping message, can be used to check the local time at the broker
                ## broker_logger.debug("PING message received from " + str(sender_address))

                ping_msg = PingMessage(
                    well_formed=True, broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
                    broker_port=this_broker_info["port"], timestamp=broker_clock.current_broker_time(),
                    requester_address=sender_address[0], requester_port=sender_address[1]
                )

                send_message(ping_msg.as_json(), this_broker_info["address"],
                             (ping_msg.requester_address, ping_msg.requester_port))

            case "SUBSCRIBE":
                # a subscription request received from a context consumer
                ##broker_logger.debug("SUBSCRIBE message received from " + str(sender_address))
                ##broker_logger.debug("raw data : " + str(received_raw_data))
                process_subscription(verified_msg)
            case "BROKER_SUBSCRIBE":
                # a broker subscription request received from another broker
                ##broker_logger.debug("BROKER_SUBSCRIBE message received from " + str(sender_address))
                ##broker_logger.debug("raw data : " + str(received_raw_data))
                process_subscription(verified_msg)
            case "UPDATE":
                # a context provider wants to update/add context information to this broker
                ## broker_logger.debug("UPDATE message received from " + str(sender_address))
                process_update_message(verified_msg)
            case "BROKER_NEIGHBORS_REQUEST":
                # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
                ## broker_logger.debug("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_address))
                process_broker_neighbors_request(verified_msg)
            case "BROKER_NEIGHBORS_RESPONSE":
                # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
                # broker_logger.debug("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_address))
                process_broker_neighbors_response(verified_msg)
    else:
        # if the message is not well-formed, send a NACK message
        broker_logger.debug(
            "received message not well formed from" + str(sender_address) + "  raw data : " + str(received_raw_data))

        nack_msg = NackMessage(
            broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
            broker_port=this_broker_info["port"], requested_operation="MESSAGE_RECEIVED",
            requester_address=sender_address[0], requester_port=sender_address[1],
            information="Received message is not well formed", well_formed=True
        )

        send_message(nack_msg.as_json(), this_broker_info["address"],
                     (nack_msg.requester_address, nack_msg.requester_port))

"""
This module contains a function that processes all messages received by this broker

Classes
--------
process_received_message
"""
import json

from broker_messages.broker_message_classes import PingMessage, SubscribeMessage, UpdateMessage, \
    BrokerNeighborsRequestMessage, BrokerNeighborsResponseMessage, BrokerSubscribeMessage, NackMessage
from broker_messages.broker_message_verification import verify_ack_msg, verify_update_msg, verify_subscribe_msg, \
    verify_nack_msg, verify_ping_msg, verify_broker_neighbors_request_msg, verify_broker_neighbors_response_msg, verify_broker_subscribe_msg
from context_management.update_service import process_update_message
from global_data import broker_logger, broker_internal_clock, this_broker_info
from message_exchange.message_sender import send_message
from neighbor_management.broker_neighbor_service import process_broker_neighbors_request, \
    process_broker_neighbors_response
from subscriptions.subscribe_service import process_broker_subscribe_msg, process_subscribe_msg


def process_received_message(received_raw_data: str, sender_address: tuple):
    """
    When the Broker receives a message, this function is called in a separate thread from main_broker

    It checks if the message is properly formed, then calls the appropriate function for further processing.

    The accepted raw messages are valid JSON strings.

    One of the keys of this dict must be "type", which has the following accepted values:
    "ack", "nack", "ping", "subscribe", "update", "context_info",
    "broker_neighbors_request", "broker_neighbors_response", "broker_subscribe"

    Parameters
    ----------
    received_raw_data: str
        the raw string received from the broker through the UDP socket in main_broker
    sender_address : tuple
        the address of the sender of this message, in format (host,port)
    """


    received_msg_is_valid = False

    try:
        # check if the raw data is valid JSON string
        message_fields = json.loads(received_raw_data)
        msg_type = message_fields.get("type")

        broker_logger.debug(message_fields)

        # if there is a message filed called type
        if msg_type is not None:
            # verify the message further based on the type
            match msg_type:
                case "ack":
                    # as of right now, there is no situation where a Broker would receive an ack
                    received_msg_is_valid = verify_ack_msg(message_fields)

                    # ack_msg = AckMessage(
                    #     broker_id=message_fields.get("broker_id"), broker_address=message_fields.get("broker_address"),
                    #     broker_port=message_fields.get("broker_port"),
                    #     requested_operation=message_fields.get("requested_operation"),
                    #     requester_address=message_fields.get("requester_address"),
                    #     requester_port=message_fields.get("requester_port"),
                    #     information=message_fields.get("information")
                    # )
                case "nack":
                    # as of right now, there is no situation where a Broker would receive a nack
                    received_msg_is_valid = verify_nack_msg(message_fields)

                    # NackMessage(
                    #   broker_id=message_fields.get("broker_id"), broker_address=message_fields.get("broker_address"),
                    #  broker_port=message_fields.get("broker_port"),
                    # requested_operation=message_fields.get("requested_operation"),
                    # requester_address=message_fields.get("requester_address"),
                    # requester_port=message_fields.get("requester_port"),
                    # information=message_fields.get("information")
                    # )

                case "ping":
                    received_msg_is_valid = verify_ping_msg(message_fields)

                    if received_msg_is_valid:

                        ping_msg = PingMessage(
                            broker_id=this_broker_info["id"],
                            broker_address=this_broker_info["address"],
                            broker_port=this_broker_info["port"], timestamp=broker_internal_clock.current_broker_time(),
                            requester_address=message_fields["requester_address"],
                            requester_port=message_fields["requester_port"]
                        )

                        send_message(ping_msg.as_json(), this_broker_info["address"],
                                     (ping_msg.requester_address, ping_msg.requester_port))
                case "subscribe":
                    received_msg_is_valid = verify_subscribe_msg(message_fields)

                    if received_msg_is_valid:
                        # a subscription request received from a context consumer
                        process_subscribe_msg(
                            SubscribeMessage(
                                entity_id=message_fields.get("entity_id"),
                                entity_type=message_fields.get("entity_type"),
                                duration=message_fields.get("duration"), scope=message_fields.get("scope"),
                                subscriber_address=message_fields.get("subscriber_address"),
                                subscriber_port=message_fields.get("subscriber_port")
                            ))
                case "update":
                    received_msg_is_valid = verify_update_msg(message_fields)

                    if received_msg_is_valid:
                        # a context provider wants to update/add context information to this broker
                        upd_msg = UpdateMessage(
                                entity_id=message_fields.get("entity_id"),
                                entity_type=message_fields.get("entity_type"),
                                provider_id=message_fields.get("provider_id"), scope=message_fields.get("scope"),
                                data=message_fields.get("data"), validity_end=message_fields.get("validity_end"),
                                provider_address=message_fields.get("provider_address"),
                                provider_port=message_fields.get("provider_port")
                            )
                        process_update_message(upd_msg)
                case "broker_neighbors_request":
                    received_msg_is_valid = verify_broker_neighbors_request_msg(message_fields)

                    if received_msg_is_valid:
                        # broker_neighbors_request messages get the list of broker neighbors in return
                        process_broker_neighbors_request(BrokerNeighborsRequestMessage(
                            broker_id=message_fields.get("broker_id"),
                            broker_address=message_fields.get("broker_address"),
                            broker_port=message_fields.get("broker_port")))
                case "broker_neighbors_response":
                    received_msg_is_valid = verify_broker_neighbors_response_msg(message_fields)

                    if received_msg_is_valid:
                        # broker_neighbors_response has a list of broker neighbors from another broker
                        process_broker_neighbors_response(BrokerNeighborsResponseMessage(
                            broker_id=message_fields.get("broker_id"),
                            broker_address=message_fields.get("broker_address"),
                            broker_port=message_fields.get("broker_port"),
                            broker_neighbors=message_fields.get("broker_neighbors", [])
                        ))
                case "broker_subscribe":
                    received_msg_is_valid = verify_broker_subscribe_msg(message_fields)

                    if received_msg_is_valid:
                        # a broker subscription request received from another broker
                        process_broker_subscribe_msg(BrokerSubscribeMessage(
                            broker_id=message_fields.get("broker_id"),
                            broker_address=message_fields.get("broker_address"),
                            broker_port=message_fields.get("broker_port"),
                            entity_id=message_fields.get("entity_id"), entity_type=message_fields.get("entity_type"),
                            duration=message_fields.get("duration"), scope=message_fields.get("scope"),
                            subscriber_address=message_fields.get("subscriber_address"),
                            subscriber_port=message_fields.get("subscriber_port"),
                            checked_brokers=message_fields.get("checked_brokers"),
                            max_hops=message_fields.get("max_hops"),
                        ))

    except json.JSONDecodeError:
        #if the json is invalid
        broker_logger.error("Message decode error")

    if not received_msg_is_valid:
        # if the message is not well-formed, send a nack message
        broker_logger.debug(
            "received message not well formed from" + str(sender_address) + "  raw data : " + str(received_raw_data))

        nack_msg = NackMessage(
            broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
            broker_port=this_broker_info["port"], requested_operation="message_received",
            requester_address=sender_address[0], requester_port=sender_address[1],
            information="Received message is not well formed"
        )

        send_message(nack_msg.as_json(), this_broker_info["address"],
                     (nack_msg.requester_address, nack_msg.requester_port))

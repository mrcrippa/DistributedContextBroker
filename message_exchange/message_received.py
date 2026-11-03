import json

from broker_messages.broker_message_classes import PingMessage, SubscribeMessage, UpdateMessage, \
    BrokerNeighborsRequestMessage, BrokerNeighborsResponseMessage, BrokerSubscribeMessage, NackMessage
from broker_messages.broker_message_verification import verify_ack_msg, verify_update_msg, verify_subscribe_msg, \
    verify_nack_msg, verify_ping_msg, verify_broker_neighbors_request_msg, verify_broker_neighbors_response_msg, verify_as_broker_subscribe_msg
from context_management.update_service import process_update_message
from global_data import broker_logger, broker_internal_clock, this_broker_info
from message_exchange.message_sender import send_message
from neighbor_management.broker_neighbor_service import process_broker_neighbors_request, \
    process_broker_neighbors_response
from subscriptions.subscribe_service import process_broker_subscribe_msg, process_subscribe_msg


def message_received(received_raw_data: str, sender_address: tuple):
    """
    When the Broker receives a message, this function is called in a separate thread from main_broker

    It checks if the message is well-formed, then calls the appropriate function for further processing.

    The accepted raw messages are valid JSON strings that will be converted into Python dicts.

    One of the keys of this dict must be "type", which has the following accepted values:
    "ack", "nack", "ping", "subscribe", "update", "context_response",
    "broker_neighbors_request", "broker_neighbors_response", "broker_subscribe"

    :param received_raw_data: the raw string received from the broker through the UDP socket in main_broker
    :param sender_address: the address of the sender of this message, in format (host,port)
    """

    # check if the raw data is valid JSON string
    received_msg_is_valid = False

    try:
        message_tokens = json.loads(received_raw_data)
        msg_type = message_tokens.get("type")

        broker_logger.debug(message_tokens)

        if msg_type is not None:
            # verify the message further based on the type
            match msg_type:
                case "ack":
                    received_msg_is_valid = verify_ack_msg(message_tokens)
                    # as of right now, there is no situation where a Broker would receive an ACK
                    # ack_msg = AckMessage(
                    #     broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
                    #     broker_port=message_tokens.get("broker_port"),
                    #     requested_operation=message_tokens.get("requested_operation"),
                    #     requester_address=message_tokens.get("requester_address"),
                    #     requester_port=message_tokens.get("requester_port"),
                    #     information=message_tokens.get("information")
                    # )
                case "nack":
                    received_msg_is_valid = verify_nack_msg(message_tokens)
                    # as of right now, there is no situation where a Broker would receive an ACK
                    # NackMessage(
                    #   broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
                    #  broker_port=message_tokens.get("broker_port"),
                    # requested_operation=message_tokens.get("requested_operation"),
                    # requester_address=message_tokens.get("requester_address"),
                    # requester_port=message_tokens.get("requester_port"),
                    # information=message_tokens.get("information")
                    # )

                case "ping":
                    received_msg_is_valid = verify_ping_msg(message_tokens)

                    if received_msg_is_valid:
                        # simple ping message, can be used to check the local time at the broker

                        ping_msg = PingMessage(
                            broker_id=this_broker_info["id"],
                            broker_address=this_broker_info["address"],
                            broker_port=this_broker_info["port"], timestamp=broker_internal_clock.current_broker_time(),
                            requester_address=message_tokens["requester_address"],
                            requester_port=message_tokens["requester_port"]
                        )

                        send_message(ping_msg.as_json(), this_broker_info["address"],
                                     (ping_msg.requester_address, ping_msg.requester_port))
                case "subscribe":
                    received_msg_is_valid = verify_subscribe_msg(message_tokens)

                    if received_msg_is_valid:
                        # a subscription request received from a context consumer
                        process_subscribe_msg(
                            SubscribeMessage(
                                entity_id=message_tokens.get("entity_id"),
                                entity_type=message_tokens.get("entity_type"),
                                duration=message_tokens.get("duration"), scope=message_tokens.get("scope"),
                                subscriber_address=message_tokens.get("subscriber_address"),
                                subscriber_port=message_tokens.get("subscriber_port")
                            ))
                case "update":
                    received_msg_is_valid = verify_update_msg(message_tokens)

                    if received_msg_is_valid:
                        # a context provider wants to update/add context information to this broker
                        upd_msg = UpdateMessage(
                                entity_id=message_tokens.get("entity_id"),
                                entity_type=message_tokens.get("entity_type"),
                                provider_id=message_tokens.get("provider_id"), scope=message_tokens.get("scope"),
                                data=message_tokens.get("data"), validity_end=message_tokens.get("validity_end"),
                                provider_address=message_tokens.get("provider_address"),
                                provider_port=message_tokens.get("provider_port")
                            )
                        process_update_message(upd_msg)
                case "broker_neighbors_request":
                    received_msg_is_valid = verify_broker_neighbors_request_msg(message_tokens)

                    if received_msg_is_valid:
                        # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
                        process_broker_neighbors_request(BrokerNeighborsRequestMessage(
                            broker_id=message_tokens.get("broker_id"),
                            broker_address=message_tokens.get("broker_address"),
                            broker_port=message_tokens.get("broker_port")))
                case "broker_neighbors_response":
                    received_msg_is_valid = verify_broker_neighbors_response_msg(message_tokens)

                    if received_msg_is_valid:
                        # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
                        process_broker_neighbors_response(BrokerNeighborsResponseMessage(
                            broker_id=message_tokens.get("broker_id"),
                            broker_address=message_tokens.get("broker_address"),
                            broker_port=message_tokens.get("broker_port"),
                            broker_neighbors=message_tokens.get("broker_neighbors", [])
                        ))
                case "broker_subscribe":
                    received_msg_is_valid = verify_as_broker_subscribe_msg(message_tokens)

                    if received_msg_is_valid:
                        # a broker subscription request received from another broker
                        process_broker_subscribe_msg(BrokerSubscribeMessage(
                            broker_id=message_tokens.get("broker_id"),
                            broker_address=message_tokens.get("broker_address"),
                            broker_port=message_tokens.get("broker_port"),
                            entity_id=message_tokens.get("entity_id"), entity_type=message_tokens.get("entity_type"),
                            duration=message_tokens.get("duration"), scope=message_tokens.get("scope"),
                            subscriber_address=message_tokens.get("subscriber_address"),
                            subscriber_port=message_tokens.get("subscriber_port"),
                            checked_brokers=message_tokens.get("checked_brokers"),
                            max_hops=message_tokens.get("max_hops"),
                        ))

    except json.JSONDecodeError:
        broker_logger.error("Message decode error")

    if not received_msg_is_valid:
        # if the message is not well-formed, send a NACK message
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

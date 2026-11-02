from dataclasses import fields, is_dataclass
from typing import Any

from broker_messages.broker_message_classes import AckMessage, PingMessage, UpdateMessage, NackMessage, \
    SubscribeMessage, BrokerNeighborsRequestMessage, BrokerSubscribeMessage, BrokerNeighborsResponseMessage
from global_data import broker_internal_clock


def basic_broker_messages_verification(msg_type: str, msg_cls_def: Any, message_tokens=None) -> bool:
    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if msg_cls_def is None or not is_dataclass(msg_cls_def):
        return False

    if message_tokens.keys() != {f.name for f in fields(msg_cls_def)}:
        return False

    if message_tokens["type"] != msg_type:
        return False

    for str_field in [f.name for f in fields(msg_cls_def) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    return True


def verify_ack_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed ack message
     if yes, returns True
     if not, returns False
     """

    if not basic_broker_messages_verification("ack", AckMessage, message_tokens):
        return False

    try:
        b_port = int(message_tokens["broker_port"])
        r_port = int(message_tokens["requester_port"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_ping_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed PING message
     if yes, returns a PingMessage object
     if not, returns None
     """

    if not basic_broker_messages_verification("ping", PingMessage, message_tokens):
        return False

    try:
        b_port = int(message_tokens["broker_port"])
        r_port = int(message_tokens["requester_port"])
        timestamp = int(message_tokens["timestamp"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False
        if timestamp <= 0:
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_subscribe_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed subscribe message
     if yes, returns a SubscribeMessage object
     if not, returns None
     """

    if not basic_broker_messages_verification("subscribe", SubscribeMessage, message_tokens):
        return False

    try:
        s_port = int(message_tokens["subscriber_port"])
        duration = int(message_tokens["duration"])

        if not (0 < s_port <= 65535):
            return False
        if duration <= 0:
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_nack_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed nack message
     if yes, returns a NackMessage object
     if not, returns None
     """

    if not basic_broker_messages_verification("nack", NackMessage, message_tokens):
        return False

    try:
        b_port = int(message_tokens["broker_port"])
        r_port = int(message_tokens["requester_port"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_update_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed update message
     if yes, returns a UpdateMessage object
     if not, returns None
     """
    # broker_logger.debug("message_tokens : " + str(message_tokens))

    if not basic_broker_messages_verification("update", UpdateMessage, message_tokens):
        return False

    try:
        p_port = int(message_tokens["provider_port"])

        if not (0 < p_port <= 65535):
            return False

        # validity end should come after the current broker time
        if broker_internal_clock.current_broker_time() >= int(message_tokens["validity_end"]):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_context_response_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed context_response message
     if yes, returns a ContextResponseMessage object
     if not, returns None
     """

    try:

        p_port = int(message_tokens["provider_port"])
        b_port = int(message_tokens["broker_port"])

        if not (0 < p_port <= 65535):
            return False
        if not (0 < b_port <= 65535):
            return False

        # validity end should come after the current broker time
        if broker_internal_clock.current_broker_time() >= int(message_tokens["validity_end"]):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_broker_neighbors_request_msg(message_tokens=None) -> bool:
    if not basic_broker_messages_verification("broker_neighbors_request", BrokerNeighborsRequestMessage,
                                              message_tokens):
        return False

    try:

        if not (0 < int(message_tokens["broker_port"]) <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_as_broker_subscribe_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed broker_subscribe message
     if yes, returns a BrokerSubscribeMessage object
     if not, returns None
     """

    if not basic_broker_messages_verification("broker_subscribe", BrokerSubscribeMessage,
                                              message_tokens):
        return False

    if not isinstance(message_tokens["checked_brokers"], list):
        return False

    for broker_id in message_tokens["checked_brokers"]:
        if not isinstance(broker_id, str) or not broker_id:
            return False

    try:
        s_port = int(message_tokens["subscriber_port"])
        duration = int(message_tokens["duration"])
        max_hops = int(message_tokens["max_hops"])

        if not (0 < s_port <= 65535):
            return False
        if duration <= 0:
            return False
        if max_hops < 0:
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_broker_neighbors_response_msg(
        message_tokens=None) -> bool:
    if not basic_broker_messages_verification("broker_neighbors_response", BrokerNeighborsResponseMessage,
                                              message_tokens):
        return False

    # this loop checks the broker_neighbors list
    # each entry is a dict with {broker_id, broker_address,broker_port}
    # broker_id should be a non-empty string
    # broker_address should be a non-empty string
    if not isinstance(message_tokens["broker_neighbors"], list):
        return False

    for neighbor in message_tokens["checked_brokers"]:
        if not isinstance(neighbor, dict) or not neighbor or neighbor.keys() != {"broker_id", "broker_address",
                                                                                 "broker_port"}:
            return False
        if not (isinstance(neighbor["broker_id"], str) and neighbor["broker_id"].strip()):
            return False
        if not (isinstance(neighbor["broker_address"], str) and neighbor["broker_address"].strip()):
            return False

    try:

        if not (0 < int(message_tokens["broker_port"]) <= 65535):
            return False

        # this loop checks the broker_neighbors list
        # broker_port should be a valid port
        for neighbor in message_tokens["broker_neighbors"]:
            if not (0 < int(neighbor["broker_port"]) <= 65535):
                return False

    except (TypeError, ValueError):
        return False

    return True

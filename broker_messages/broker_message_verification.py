"""
This module contains all functions necessary to verify if messages received or sent by the broker are valid, i.e. properly formatted

The types of messages we are referring here can be found in broker_messages.py

Functions:
--------
basic_broker_messages_verification
verify_ack_msg
verify_ping_msg
verify_subscribe_msg
verify_nack_msg
verify_update_msg
verify_context_info_msg
verify_broker_neighbors_request_msg
verify_broker_neighbors_response_msg
verify_broker_subscribe_msg
"""

from dataclasses import fields, is_dataclass
from typing import Any

from broker_messages.broker_message_classes import AckMessage, PingMessage, UpdateMessage, NackMessage, \
    SubscribeMessage, BrokerNeighborsRequestMessage, BrokerSubscribeMessage, BrokerNeighborsResponseMessage
from global_data import broker_internal_clock


def basic_broker_messages_verification(msg_type: str, msg_cls_def: Any, message_fields=None) -> bool:
    """
       Conducts a basic verification of a message received or sent by the broker. This function checks elements present in all broker messages

    Parameters
    ----------
    msg_type : str
        the type of the message, a list of possible types can be found in broker_message_classes
    msg_cls_def : Any
        the definition of the dataclass that describes this message type. It should be one of the classes defined in
        broker_message_classes
    message_fields : dict
        a dict with all the message fields (default is None)


    Returns
    -------
    bool
        True if the message passes basic verification, False otherwise
    """

    # verifying the parameters
    if message_fields is None or not isinstance(message_fields, dict):
        return False

    if msg_cls_def is None or not is_dataclass(msg_cls_def):
        return False

    # the message fields must match exactly in number and name with the broker message class provided
    if message_fields.keys() != {f.name for f in fields(msg_cls_def)}:
        return False

    if message_fields["type"] != msg_type:
        return False

    # if a message field is a string, it must be a valid string and non-empty
    for str_field in [f.name for f in fields(msg_cls_def) if f.type in (str, 'str')]:
        val = message_fields[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    return True


def verify_ack_msg(message_fields=None) -> bool:
    """
        Checks if an ack message is valid i.e. properly formed

        more info in the message format can be found in broker_message_classes.AckMessage

        Parameters
        ----------
         message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("ack", AckMessage, message_fields):
        return False

    try:
        # are the values given for the ports valid?
        b_port = int(message_fields["broker_port"])
        r_port = int(message_fields["requester_port"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_ping_msg(message_fields=None) -> bool:
    """
        Checks if a ping message is valid i.e. properly formed

        More info on the message format can be found in broker_message_classes.PingMessage

        Parameters
        ----------
         message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("ping", PingMessage, message_fields):
        return False

    try:
        # are the values for ports valid
        b_port = int(message_fields["broker_port"])
        r_port = int(message_fields["requester_port"])
        # timestamps must be > 0
        timestamp = int(message_fields["timestamp"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False
        if timestamp <= 0:
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_subscribe_msg(message_fields=None) -> bool:
    """
        Checks if a subscribe message is valid i.e. properly formed

        More info on the message format can be found in broker_message_classes.SubscribeMessage

        Parameters
        ----------
        message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("subscribe", SubscribeMessage, message_fields):
        return False

    try:
        # valid port value?
        s_port = int(message_fields["subscriber_port"])
        # subscription duration must be > 0
        duration = int(message_fields["duration"])

        if not (0 < s_port <= 65535):
            return False
        if duration <= 0:
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_nack_msg(message_fields=None) -> bool:
    """
        Checks if a nack message is valid i.e. properly formed

        More info on the message format can be found in broker_message_classes.NackMessage

        Parameters
        ----------
        message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("nack", NackMessage, message_fields):
        return False

    try:
        # check if port values are valid
        b_port = int(message_fields["broker_port"])
        r_port = int(message_fields["requester_port"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_update_msg(message_fields=None) -> bool:
    """
    Checks if an update message is valid i.e. properly formed

    More info on the message format can be found in broker_message_classes.UpdateMessage

    Parameters
    ----------
    message_fields : dict
        a dict with all the message fields (default is None)


    Returns
    -------
    bool
        True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("update", UpdateMessage, message_fields):
        return False

    try:
        # check if port value is valid
        p_port = int(message_fields["provider_port"])

        if not (0 < p_port <= 65535):
            return False

        # validity_end should come after the current broker time
        if broker_internal_clock.current_broker_time() >= int(message_fields["validity_end"]):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_context_info_msg(message_fields=None) -> bool:
    """
    Checks if a context_info message is valid i.e. properly formed

    More info on the message format can be found in broker_message_classes.ContextInfoMessage

    Parameters
    ----------
    message_fields : dict
        a dict with all the message fields (default is None)


    Returns
    -------
    bool
        True if the message is valid, False otherwise
    """

    try:
        # check if the port values are valid
        p_port = int(message_fields["provider_port"])
        b_port = int(message_fields["broker_port"])

        if not (0 < p_port <= 65535):
            return False
        if not (0 < b_port <= 65535):
            return False

        # validity_end should come after the current broker time
        if broker_internal_clock.current_broker_time() >= int(message_fields["validity_end"]):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_broker_neighbors_request_msg(message_fields=None) -> bool:
    """
    Checks if a broker_neighbors_request message is valid i.e. properly formed

        More info on the message format can be found in broker_message_classes.BrokerNeighborsRequestMessage

        Parameters
        ----------
        message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("broker_neighbors_request", BrokerNeighborsRequestMessage,
                                              message_fields):
        return False

    try:
        # check if the broker_port value is valid
        if not (0 < int(message_fields["broker_port"]) <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_broker_subscribe_msg(message_fields=None) -> bool:
    """Checks if a broker_subscribe message is valid i.e. properly formed

        More info on the message format can be found in broker_message_classes.BrokerSubscribeMessage

        Parameters
        ----------
        message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """

    if not basic_broker_messages_verification("broker_subscribe", BrokerSubscribeMessage,
                                              message_fields):
        return False

    if not isinstance(message_fields["checked_brokers"], list):
        return False

    for broker_id in message_fields["checked_brokers"]:
        if not isinstance(broker_id, str) or not broker_id:
            return False

    try:
        s_port = int(message_fields["subscriber_port"])
        duration = int(message_fields["duration"])
        max_hops = int(message_fields["max_hops"])

        if not (0 < s_port <= 65535):
            return False
        if duration <= 0:
            return False
        if max_hops < 0:
            return False

    except (TypeError, ValueError):
        return False

    return True


def verify_broker_neighbors_response_msg(message_fields=None) -> bool:
    """Checks if a broker_neighbors_response message is valid i.e. properly formed

        More info on the message format can be found in broker_message_classes.BrokerNeighborsResponseMessage

        Parameters
        ----------
        message_fields : dict
            a dict with all the message fields (default is None)


        Returns
        -------
        bool
            True if the message is valid, False otherwise
    """
    if not basic_broker_messages_verification("broker_neighbors_response", BrokerNeighborsResponseMessage,
                                              message_fields):
        return False

    # this loop checks the broker_neighbors list
    # each entry is a dict with {broker_id, broker_address,broker_port}
    # broker_id should be a non-empty string
    # broker_address should be a non-empty string
    if not isinstance(message_fields["broker_neighbors"], list):
        return False

    for neighbor in message_fields["broker_neighbors"]:
        if not isinstance(neighbor, dict) or not neighbor or neighbor.keys() != {"broker_id", "broker_address",
                                                                                 "broker_port"}:
            return False
        if not (isinstance(neighbor["broker_id"], str) and neighbor["broker_id"].strip()):
            return False
        if not (isinstance(neighbor["broker_address"], str) and neighbor["broker_address"].strip()):
            return False

    try:

        if not (0 < int(message_fields["broker_port"]) <= 65535):
            return False

        # this loop checks the broker_neighbors list
        # broker_port should be a valid port
        for neighbor in message_fields["broker_neighbors"]:
            if not (0 < int(neighbor["broker_port"]) <= 65535):
                return False

    except (TypeError, ValueError):
        return False

    return True

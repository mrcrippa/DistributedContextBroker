import json

from global_data import broker_neighbors, broker_clock
from tables.context_info_table import ContextInfo
from tables.subscription_table import Subscription



# utility code for creating messages
# given a certain input, they all return a JSON string

# returns a NACK message string
def create_nack_message_string(this_broker_info: dict, operation: str,
                               requester_address: str, reason: str) -> str:
    """returns a NACK message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "NACK", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"], "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string


def create_ack_message_string(this_broker_info: dict, operation: str,
                              requester_address: str, reason: str) -> str:
    """returns an ACK message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "ACK", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"], "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string


def create_ping_message_string(this_broker_info: dict) -> str:
    """returns a PING message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "PING", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"]})

    return dict_as_string


def create_context_response_message_string(this_broker_info: dict,
                                           ctx_info: ContextInfo) -> str:
    """returns a RESPONSE message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "CONTEXT_RESPONSE", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"], "PROVIDER_ID": ctx_info.get_provider_id(),
         "ENTITY_ID": ctx_info.get_entity_id(),
         "ENTITY_TYPE": ctx_info.get_entity_type(), "SCOPE": ctx_info.get_scope(),
         "VALIDITY_BEGIN": str(ctx_info.get_validity_begin),
         "VALIDITY_END": str(ctx_info.get_validity_end()), "DATA": ctx_info.get_data()})
    return dict_as_string


def create_broker_neighbors_request_message_string(this_broker_info: dict) -> str:
    """returns a BROKER_NEIGHBORS_REQUEST message string - JSON"""
    msg_string = json.dumps(
        {"TYPE": "BROKER_NEIGHBORS_REQUEST", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"]})
    return msg_string


def create_broker_subscribe_message_string(this_broker_info: dict, sub: Subscription, max_hops: str,
                                           checked_brokers: list) -> str:
    """returns a BROKER_SUBSCRIBE message string - JSON"""
    msg_string = json.dumps(
        {"TYPE": "BROKER_SUBSCRIBE", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"], "ENTITY_ID": sub.get_entity_id(),
         "ENTITY_TYPE": sub.get_entity_type(),
         "SCOPE": sub.get_scope(), "SUBSCRIBER_ADDRESS": sub.get_subscriber_address(),
         "SUBSCRIBER_PORT": sub.get_subscriber_port(),
         "DURATION": sub.get_duration(), "MAXIMUM_HOPS": max_hops,
         "CHECKED_BROKERS": checked_brokers})
    return msg_string


def create_broker_neighbors_response_message_string(this_broker_info: dict) -> str:
    """returns a BROKER_NEIGHBORS_RESPONSE message string - JSON"""
    msg_string = json.dumps(
        {"TYPE": "BROKER_NEIGHBORS_RESPONSE", "TIMESTAMP": broker_clock.current_broker_time(), "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"],
         "BROKER_NEIGHBORS": broker_neighbors.print_table()})
    return msg_string

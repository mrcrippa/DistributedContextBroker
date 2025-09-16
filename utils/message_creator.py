import json
import time

from global_data import broker_neighbors
from tables.context_info import ContextInfo


# utility code for creating messages
# given a certain input, they all return a JSON string

# returns a NACK message string
def create_nack_message_string(this_broker_id: str, this_broker_address: str, this_broker_port: str, operation: str,
                               requester_address: str, reason: str) -> str:
    """returns a NACK message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "NACK", "TIMESTAMP": str(int(round(time.time() * 1000))), "BROKER_ID": this_broker_id,
         "BROKER_ADDRESS": this_broker_address,
         "BROKER_PORT": this_broker_port, "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string


def create_ack_message_string(this_broker_id: str, this_broker_address: str, this_broker_port: str, operation: str,
                              requester_address: str, reason: str) -> str:
    """returns an ACK message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "ACK", "TIMESTAMP": str(int(round(time.time() * 1000))), "BROKER_ID": this_broker_id,
         "BROKER_ADDRESS": this_broker_address,
         "BROKER_PORT": this_broker_port, "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string


def create_ping_message_string(this_broker_id: str, this_broker_address: str, this_broker_port: str) -> str:
    """returns a PING message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "PING", "TIMESTAMP": str(int(round(time.time() * 1000))), "BROKER_ID": this_broker_id,
         "BROKER_ADDRESS": this_broker_address,
         "BROKER_PORT": this_broker_port})

    return dict_as_string


def create_context_response_message_string(this_broker_id: str, this_broker_address: str, this_broker_port: str,
                                           ctx_info: ContextInfo) -> str:
    """returns a RESPONSE message string - JSON"""
    dict_as_string = json.dumps({"TYPE": "CONTEXT_RESPONSE", "TIMESTAMP": str(int(round(
        time.time() * 1000))), "BROKER_ID": this_broker_id,
                                 "BROKER_ADDRESS": this_broker_address,
                                 "BROKER_PORT": this_broker_port, "PROVIDER_ID": ctx_info.get_provider_id(),
                                 "ENTITY_ID": ctx_info.get_entity_id(),
                                 "ENTITY_TYPE": ctx_info.get_entity_type(), "SCOPE": ctx_info.get_scope(),
                                 "VALIDITY_BEGIN": str(ctx_info.get_validity_begin),
                                 "VALIDITY_END": str(ctx_info.get_validity_end()), "DATA": ctx_info.get_data()})
    return dict_as_string


def create_broker_neighbors_request_message_string(this_broker_id: str, this_broker_address: str,
                                                   this_broker_port: str) -> str:
    """returns a BROKER_NEIGHBORS_REQUEST message string - JSON"""
    msg_string = json.dumps({"TYPE": "BROKER_NEIGHBORS_REQUEST", "TIMESTAMP": str(int(round(
        time.time() * 1000))), "BROKER_ID": this_broker_id, "BROKER_ADDRESS": this_broker_address,
                             "BROKER_PORT": this_broker_port})
    return msg_string


def create_broker_subscribe_message_string_no_forward(this_broker_id: str, this_broker_address: str,
                                           this_broker_port: str, entity_id: str, entity_type: str, scope: str,
                                           sub_addr: str, sub_port: str, duration: str, max_hops: str, checked_brokers:list) -> str:
    """returns a BROKER_SUBSCRIBE message string - JSON"""
    msg_string = json.dumps({"TYPE": "BROKER_SUBSCRIBE", "TIMESTAMP": str(int(round(
        time.time() * 1000))), "BROKER_ID": this_broker_id, "BROKER_ADDRESS": this_broker_address,
                             "BROKER_PORT": this_broker_port, "ENTITY_ID": entity_id, "ENTITY_TYPE": entity_type,
                             "SCOPE": scope, "SUBSCRIBER_ADDRESS": sub_addr, "SUBSCRIBER_PORT": sub_port,
                             "DURATION": duration, "MAXIMUM_HOPS": max_hops, "FORWARD": False, "CHECKED_BROKERS": checked_brokers})
    return msg_string

def create_broker_subscribe_message_string_with_forward(this_broker_id: str, this_broker_address: str,
                                           this_broker_port: str, entity_id: str, entity_type: str, scope: str,
                                           sub_addr: str, sub_port: str, duration: str, max_hops: str, checked_brokers:list) -> str:
    """returns a BROKER_SUBSCRIBE message string - JSON"""
    msg_string = json.dumps({"TYPE": "BROKER_SUBSCRIBE", "TIMESTAMP": str(int(round(
        time.time() * 1000))), "BROKER_ID": this_broker_id, "BROKER_ADDRESS": this_broker_address,
                             "BROKER_PORT": this_broker_port, "ENTITY_ID": entity_id, "ENTITY_TYPE": entity_type,
                             "SCOPE": scope, "SUBSCRIBER_ADDRESS": sub_addr, "SUBSCRIBER_PORT": sub_port,
                             "DURATION": duration, "MAXIMUM_HOPS": max_hops, "FORWARD": True, "CHECKED_BROKERS": checked_brokers})
    return msg_string


def create_broker_neighbors_response_message_string(this_broker_id: str, this_broker_address: str,
                                                    this_broker_port: str) -> str:
    """returns a BROKER_NEIGHBORS_RESPONSE message string - JSON"""
    msg_string = json.dumps({"TYPE": "BROKER_NEIGHBORS_RESPONSE", "TIMESTAMP": str(int(round(
        time.time() * 1000))), "BROKER_ID": this_broker_id, "BROKER_ADDRESS": this_broker_address,
                             "BROKER_PORT": this_broker_port, "BROKER_NEIGHBORS": broker_neighbors.print_table()})
    return msg_string

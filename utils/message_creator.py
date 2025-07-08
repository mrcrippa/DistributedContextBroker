import json
import time

from tables.context_info import ContextInfo


# utility code for creating messages
# given a certain input, they all return a JSON string

# returns a NACK message string
def create_nack_message_string(operation: str, requester_address: str, reason: str) -> str:
    """returns a NACK message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "NACK", "TIMESTAMP": str(int(round(time.time() * 1000))), "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string


def create_ack_message_string(operation: str, requester_address: str, reason: str) -> str:
    """returns an ACK message string - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "ACK", "TIMESTAMP": str(int(round(time.time() * 1000))), "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string


def create_ping_message_string() -> str:
    """returns a PING message string - JSON"""
    dict_as_string = json.dumps(
        dict(TYPE="PING", TIMESTAMP=str(int(round(time.time() * 1000)))))

    return dict_as_string


def create_request_response_message_string(ctx_info: ContextInfo) -> str:
    """returns a RESPONSE message string - JSON"""
    dict_as_string = json.dumps({"TYPE": "RESPONSE", "TIMESTAMP": str(int(round(
        time.time() * 1000))), "PROVIDER_ID": ctx_info.get_provider_id(), "ENTITY_ID": ctx_info.get_entity_id(),
                                 "ENTITY_TYPE": ctx_info.get_entity_type(), "SCOPE": ctx_info.get_scope(),
                                 "VALIDITY_BEGIN": str(ctx_info.get_validity_begin),
                                 "VALIDITY_END": str(ctx_info.get_validity_end()), "DATA": ctx_info.get_data()})
    return dict_as_string

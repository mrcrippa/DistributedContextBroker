import json
import logging

from global_data import broker_clock

# set with the valid types of messages the broker will receive
message_type_set = {"ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE", "CONTEXT_RESPONSE",
                    "BROKER_NEIGHBORS_REQUEST", "BROKER_NEIGHBORS_RESPONSE", "BROKER_SUBSCRIBE"}


def is_string_valid_int_bigger_than_zero(value: str) -> bool:
    """check if a string is a valid integer bigger than zero"""
    try:
        if int(value) <= 0:
            return False
    except ValueError:
        return False
    return True


class ParsedMessage(object):
    """
    Class representing a message received by the Broker
    The raw message will be parsed and verified
    Only well-formed messages should be used
    """

    def __init__(self, message_raw_data):

        self._message_tokens = {}
        self._well_formed = False

        # the message strings are in JSON
        # check if the message is valid JSON
        try:
            self._message_tokens = json.loads(message_raw_data)
        except json.JSONDecodeError:
            logging.error("Message decode error")

        msg_type = self._message_tokens.get("TYPE")
        # all message need at least the type
        if msg_type is not None and msg_type in message_type_set:
            # verify the message further based on the type
            match msg_type:
                case "ACK":
                    self._well_formed = self.is_ack_message_well_formed()
                case "NACK":
                    self._well_formed = self.is_nack_message_well_formed()
                case "PING":  #
                    self._well_formed = self.is_ping_message_well_formed()
                case "SUBSCRIBE":
                    self._well_formed = self.is_sub_message_well_formed()
                case "UPDATE":
                    self._well_formed = self.is_update_message_well_formed()
                case "BROKER_NEIGHBORS_REQUEST":
                    self._well_formed = self.is_broker_neighbors_request_message_well_formed()
                case "BROKER_NEIGHBORS_RESPONSE":
                    self._well_formed = self.is_broker_neighbors_response_message_well_formed()
                case "BROKER_SUBSCRIBE":
                    self._well_formed = self.is_broker_subscribe_message_well_formed

    def is_ack_message_well_formed(self) -> bool:
        """check if an ACK message is well-formed"""
        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get("OPERATION") is None:
            return False
        elif self._message_tokens.get("REQUESTER") is None:
            return False
        elif self._message_tokens.get("REASON") is None:
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            return False

        return True

    def is_nack_message_well_formed(self) -> bool:
        """check if a NACK message is well-formed"""
        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get("OPERATION") is None:
            return False
        elif self._message_tokens.get("REQUESTER") is None:
            return False
        elif self._message_tokens.get("REASON") is None:
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            return False

        return True

    def is_ping_message_well_formed(self) -> bool:
        """check if a PING message is well-formed"""
        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get("TIMESTAMP") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("TIMESTAMP")):
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            return False

        return True

    def is_broker_neighbors_response_message_well_formed(self) -> bool:
        """check if a BROKER_NEIGHBORS_RESPONSE message is well-formed"""
        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get("BROKER_NEIGHBORS") is None or type(
                self._message_tokens.get("BROKER_NEIGHBORS")).__name__ != "list":
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            return False

        for neighbor in self._message_tokens.get("BROKER_NEIGHBORS"):
            if neighbor.get("broker_id") is None or neighbor.get("broker_address") is None or neighbor.get(
                    "broker_port") is None:
                return False

        return True

    def is_broker_neighbors_request_message_well_formed(self) -> bool:
        """check if a BROKER_NEIGHBORS_REQUEST message is well-formed"""
        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            return False

        return True

    def is_sub_message_well_formed(self) -> bool:
        """check if a SUBSCRIBE message is well-formed"""

        if self._message_tokens.get("ENTITY_ID") is None:
            return False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            return False
        elif self._message_tokens.get("SCOPE") is None:
            return False
        elif self._message_tokens.get("SUBSCRIBER_ADDRESS") is None:
            return False
        elif self._message_tokens.get(
                "SUBSCRIBER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("SUBSCRIBER_PORT")):
            return False
        elif self._message_tokens.get("DURATION") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("DURATION")):
            return False

        return True

    @property
    def is_broker_subscribe_message_well_formed(self) -> bool:
        """check if a BROKER_SUBSCRIBE message is well-formed"""

        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            return False
        elif self._message_tokens.get("ENTITY_ID") is None:
            return False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            return False
        elif self._message_tokens.get("SCOPE") is None:
            return False
        elif self._message_tokens.get("SUBSCRIBER_ADDRESS") is None:
            return False
        elif self._message_tokens.get(
                "SUBSCRIBER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("SUBSCRIBER_PORT")):
            return False
        elif self._message_tokens.get("DURATION") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("DURATION")):
            return False
        elif self._message_tokens.get("MAXIMUM_HOPS") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("MAXIMUM_HOPS")):
            return False
        elif self._message_tokens.get("CHECKED_BROKERS") is None or type(
                self._message_tokens.get("CHECKED_BROKERS")).__name__ != "list":
            return False

        return True

    def is_update_message_well_formed(self) -> bool:
        """check if a UPDATE message is well-formed"""
        if self._message_tokens.get("ENTITY_ID") is None:
            return False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            return False
        elif self._message_tokens.get("SCOPE") is None:
            return False
        elif self._message_tokens.get("PROVIDER_ID") is None:
            return False
        elif self._message_tokens.get("DATA") is None:
            return False
        elif self._message_tokens.get("VALIDITY_END") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("VALIDITY_END")):
            return False

        # validity end should come after the current broker time
        if not (broker_clock.current_broker_time() < int(self._message_tokens.get("VALIDITY_END"))):
            return False

        return True

    def is_well_formed(self) -> bool:
        return self._well_formed

    def get_token(self, key: str):
        if key is not None and self._message_tokens.get(key):
            return self._message_tokens.get(key)
        else:
            return None

    def print_message(self) -> str:
        return str(self._message_tokens)

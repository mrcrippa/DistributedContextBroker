import json
import logging
from dataclasses import dataclass

from global_data import broker_clock

# set with the valid types of messages the broker will receive
message_type_set = {"ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE", "CONTEXT_RESPONSE",
                    "BROKER_NEIGHBORS_REQUEST", "BROKER_NEIGHBORS_RESPONSE", "BROKER_SUBSCRIBE"}





class BrokerMessage(object):
    """
    Class representing a message accepted by the Broker.

    The accepted raw messages are valid JSON strings that will be converted into Python dicts.

    One of the keys of this dict must be "TYPE", which has the following accepted values:
    "ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE", "CONTEXT_RESPONSE",
    "BROKER_NEIGHBORS_REQUEST", "BROKER_NEIGHBORS_RESPONSE", "BROKER_SUBSCRIBE"

    This class will be used as a parent class for each type of Broker message.
    """

    def __init__(self, raw_data_from_socket):

        self._message_tokens = {}
        self._well_formed = False

        # check if the raw data is valid JSON string
        try:
            self._message_tokens = json.loads(raw_data_from_socket)
        except json.JSONDecodeError:
            logging.error("Message decode error")

        msg_type = self._message_tokens.get("TYPE")
        # all message need at least the type
        if msg_type is not None and msg_type in message_type_set:
            # verify the message further based on the type
            match msg_type:

                case "SUBSCRIBE":
                    self._well_formed = self.is_sub_message_well_formed()

                case "BROKER_NEIGHBORS_REQUEST":
                    self._well_formed = self.is_broker_neighbors_request_message_well_formed()
                case "BROKER_NEIGHBORS_RESPONSE":
                    self._well_formed = self.is_broker_neighbors_response_message_well_formed()
                case "BROKER_SUBSCRIBE":
                    self._well_formed = self.is_broker_subscribe_message_well_formed


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
                "BROKER_PORT") is None or self._message_tokens.get("BROKER_PORT") <= 0:
            return False

        for neighbor in self._message_tokens.get("BROKER_NEIGHBORS"):
            if neighbor.get("BROKER_ID") is None or neighbor.get("BROKER_ADDRESS") is None or neighbor.get(
                    "BROKER_PORT") is None:
                return False

        return True

    def is_broker_neighbors_request_message_well_formed(self) -> bool:
        """check if a BROKER_NEIGHBORS_REQUEST message is well-formed"""
        if self._message_tokens.get("BROKER_ID") is None:
            return False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or self._message_tokens.get("BROKER_PORT") <= 0:
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
                "SUBSCRIBER_PORT") is None or self._message_tokens.get("SUBSCRIBER_PORT") <= 0:
            return False
        elif self._message_tokens.get("DURATION") is None or self._message_tokens.get("DURATION") <= 0:
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
                "BROKER_PORT") is None or self._message_tokens.get("BROKER_PORT") <= 0:
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
                "SUBSCRIBER_PORT") is None or self._message_tokens.get("SUBSCRIBER_PORT") <= 0:
            return False
        elif self._message_tokens.get("DURATION") is None or self._message_tokens.get("DURATION") <= 0:
            return False
        elif self._message_tokens.get("MAX_HOPS") is None or self._message_tokens.get("MAX_HOPS") < 0:
            return False
        elif self._message_tokens.get("CHECKED_BROKERS") is None or type(
                self._message_tokens.get("CHECKED_BROKERS")).__name__ != "list":
            return False

        return True



    def is_well_formed(self) -> bool:
        return self._well_formed

    def get_token(self, key: str):
        return self._message_tokens.get(key)

    def print_message(self) -> str:
        return str(self._message_tokens)

class PingMessage(BrokerMessage):
    def __init__(self, raw_data_from_socket):
        super().__init__(raw_data_from_socket)

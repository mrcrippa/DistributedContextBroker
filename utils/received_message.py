import json
import logging

from global_data import broker_clock
from utils.misc_utils import is_string_valid_int_bigger_than_zero

# set with the valid types of messages the broker will receive
message_type_set = {"ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE", "CONTEXT_RESPONSE",
                    "BROKER_NEIGHBORS_REQUEST", "BROKER_NEIGHBORS_RESPONSE", "BROKER_SUBSCRIBE"}


class ReceivedMessage(object):
    """
    Class representing a message received by the Broker
    """

    def __init__(self, message_raw_data):

        self._message_tokens = {}
        self._well_formed = False
        self._message_type = ""
        self._message_timestamp = 0

        # the message strings are in JSON
        # check if the message is valid JSON
        try:
            self._message_tokens = json.loads(message_raw_data)
        except json.JSONDecodeError:
            logging.error("Message decode error")

        # if the deserialization worked
        if self._message_tokens:
            # all message need at least the type and timestamp
            if self._message_tokens.get("TYPE") and self._message_tokens.get("TIMESTAMP"):
                # check if type is a valid one, and if the timestamp is correct
                if self._message_tokens.get(
                        "TYPE") in message_type_set and is_string_valid_int_bigger_than_zero(
                    self._message_tokens.get("TIMESTAMP")):

                    self._message_type = self._message_tokens.get("TYPE")
                    self._message_timestamp = self._message_tokens.get("TIMESTAMP")

                    # verify the message further based on the type
                    if self._message_tokens.get("TYPE") == "SUBSCRIBE":
                        self._well_formed = self.is_sub_message_well_formed()
                    elif self._message_tokens.get("TYPE") == "UPDATE":
                        self._well_formed = self.is_update_message_well_formed()
                    elif self._message_tokens.get("TYPE") == "BROKER_NEIGHBORS_REQUEST":
                        self._well_formed = self.is_broker_neighbors_request_message_well_formed()
                    elif self._message_tokens.get("TYPE") == "BROKER_NEIGHBORS_RESPONSE":
                        self._well_formed = self.is_broker_neighbors_response_message_well_formed()
                    elif self._message_tokens.get("TYPE") == "BROKER_SUBSCRIBE":
                        self._well_formed = self.is_broker_subscribe_message_well_formed
                    elif self._message_tokens.get("TYPE") == "PING":
                        self._well_formed = self.is_ping_message_well_formed()
                    elif self._message_tokens.get("TYPE") == "ACK":
                        self._well_formed = self.is_ack_message_well_formed()
                    elif self._message_tokens.get("TYPE") == "NACK":
                        self._well_formed = self.is_nack_message_well_formed()

    def is_ack_message_well_formed(self) -> bool:
        """check if an ACK message is well-formed"""
        well_formed = True
        if self._message_tokens.get("BROKER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get("OPERATION") is None:
            well_formed = False
        elif self._message_tokens.get("REQUESTER") is None:
            well_formed = False
        elif self._message_tokens.get("REASON") is None:
            well_formed = False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            well_formed = False

        return well_formed

    def is_nack_message_well_formed(self) -> bool:
        """check if a NACK message is well-formed"""
        well_formed = True
        if self._message_tokens.get("BROKER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get("OPERATION") is None:
            well_formed = False
        elif self._message_tokens.get("REQUESTER") is None:
            well_formed = False
        elif self._message_tokens.get("REASON") is None:
            well_formed = False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            well_formed = False

        return well_formed

    def is_ping_message_well_formed(self) -> bool:
        """check if a PING message is well-formed"""
        well_formed = True
        if self._message_tokens.get("BROKER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            well_formed = False

        return well_formed

    def is_broker_neighbors_response_message_well_formed(self) -> bool:
        """check if a BROKER_NEIGHBORS_RESPONSE message is well-formed"""
        well_formed = True
        if self._message_tokens.get("BROKER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_NEIGHBORS") is None or type(
                self._message_tokens.get("BROKER_NEIGHBORS")).__name__ != "list":
            well_formed = False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            well_formed = False

        if well_formed:
            for neighbor in self._message_tokens.get("BROKER_NEIGHBORS"):
                if neighbor.get("broker_id") is None or neighbor.get("broker_address") is None or neighbor.get(
                        "broker_port") is None:
                    well_formed = False

        return well_formed

    def is_broker_neighbors_request_message_well_formed(self) -> bool:
        """check if a BROKER_NEIGHBORS_REQUEST message is well-formed"""
        well_formed = True
        if self._message_tokens.get("BROKER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            well_formed = False

        return well_formed

    def is_sub_message_well_formed(self) -> bool:
        """check if a SUBSCRIBE message is well-formed"""
        well_formed = True

        if self._message_tokens.get("ENTITY_ID") is None:
            well_formed = False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            well_formed = False
        elif self._message_tokens.get("SCOPE") is None:
            well_formed = False
        elif self._message_tokens.get("SUBSCRIBER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get(
                "SUBSCRIBER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("SUBSCRIBER_PORT")):
            well_formed = False
        elif self._message_tokens.get("DURATION") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("DURATION")):
            well_formed = False

        return well_formed

    @property
    def is_broker_subscribe_message_well_formed(self) -> bool:
        """check if a BROKER_SUBSCRIBE message is well-formed"""
        well_formed = True

        if self._message_tokens.get("BROKER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("BROKER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get(
                "BROKER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("BROKER_PORT")):
            well_formed = False
        elif self._message_tokens.get("ENTITY_ID") is None:
            well_formed = False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            well_formed = False
        elif self._message_tokens.get("SCOPE") is None:
            well_formed = False
        elif self._message_tokens.get("SUBSCRIBER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get(
                "SUBSCRIBER_PORT") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("SUBSCRIBER_PORT")):
            well_formed = False
        elif self._message_tokens.get("DURATION") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("DURATION")):
            well_formed = False
        elif self._message_tokens.get("MAXIMUM_HOPS") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("MAXIMUM_HOPS")):
            well_formed = False
        elif self._message_tokens.get("FORWARD") is None or type(
                self._message_tokens.get("FORWARD")).__name__ != "bool":
            well_formed = False
        elif self._message_tokens.get("CHECKED_BROKERS") is None or type(
                self._message_tokens.get("CHECKED_BROKERS")).__name__ != "list":
            well_formed = False

        return well_formed

    def is_update_message_well_formed(self) -> bool:
        """check if a UPDATE message is well-formed"""
        well_formed = True

        if self._message_tokens.get("ENTITY_ID") is None:
            well_formed = False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            well_formed = False
        elif self._message_tokens.get("SCOPE") is None:
            well_formed = False
        elif self._message_tokens.get("PROVIDER_ID") is None:
            well_formed = False
        elif self._message_tokens.get("DATA") is None:
            well_formed = False
        elif self._message_tokens.get(
                "VALIDITY_BEGIN") is None or not is_string_valid_int_bigger_than_zero(
            self._message_tokens.get("VALIDITY_BEGIN")):
            well_formed = False
        elif self._message_tokens.get("VALIDITY_END") is None or not is_string_valid_int_bigger_than_zero(
                self._message_tokens.get("VALIDITY_END")):
            well_formed = False
        else:
            # checks if the validity timestamps are correct
            #  there two valid situations:
            #  the validity of the update begins before the current time and ends after the current time
            #  the validity of the update begins and ends after the current time (in the future)
            # any request for the data will only return when the data is valid
            begin_tag = int(self._message_tokens.get("VALIDITY_BEGIN"))
            end_tag = int(self._message_tokens.get("VALIDITY_END"))
            current_time = broker_clock.current_broker_time()

            # begin comes before end
            end_after_begin = begin_tag < end_tag
            # end comes after the current time
            end_after_current = current_time < end_tag

            if not (end_after_begin and end_after_current):
                well_formed = False

        return well_formed

    def is_well_formed(self) -> bool:
        return self._well_formed

    def get_token(self, key: str):
        if key is not None and self._message_tokens.get(key):
            return self._message_tokens.get(key)
        else:
            return None

    def print_message(self) -> str:
        return str(self._message_tokens)

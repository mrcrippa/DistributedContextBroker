import json
import logging
import time

message_type_set = {"ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE", "CONTEXT_REQUEST", "RESPONSE"}

#check if a string is a valid integer bigger than zero
def check_integer_bigger_than_zero(value):
    result = False
    try:
        s = int(value)
        if s > 0:
            result = True
    except ValueError:
        result = False
    return result


class Message(object):
    """
    Class representing a message received or sent by the Broker
    """

    def __init__(self, message_raw_data):

        self._message_tokens = {}
        self._well_formed = False
        self._message_type = ""
        self._message_timestamp = 0

        try:
            self._message_tokens = json.loads(message_raw_data)
        except json.JSONDecodeError:
            logging.error("Message decode error")

        if self._message_tokens:
            if self._message_tokens.get("TYPE") and self._message_tokens.get("TIMESTAMP"):
                if self._message_tokens.get("TYPE") in message_type_set and check_integer_bigger_than_zero(
                        self._message_tokens.get("TIMESTAMP")):
                    self._message_type = self._message_tokens.get("TYPE")
                    self._message_timestamp = self._message_tokens.get("TIMESTAMP")
                    if self._message_tokens.get("TYPE") == "SUBSCRIBE" and self.verify_sub_message():
                        self._well_formed = True
                    elif self._message_tokens.get("TYPE") == "UPDATE" and self.verify_update_message():
                        self._well_formed = True
                    elif self._message_tokens.get("TYPE") == "REQUEST" and self.verify_request_message():
                        self._well_formed = True
                    else:
                        self._well_formed = True

    def verify_sub_message(self):

        well_formed = True

        if self._message_tokens.get("ENTITY_ID") is None:
            well_formed = False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            well_formed = False
        elif self._message_tokens.get("SCOPE") is None:
            well_formed = False
        elif self._message_tokens.get("SUBSCRIBER_ADDRESS") is None:
            well_formed = False
        elif self._message_tokens.get("SUBSCRIBER_PORT") is None and not check_integer_bigger_than_zero(
                self._message_tokens.get("SUBSCRIBER_PORT")):
            well_formed = False
        elif self._message_tokens.get("DURATION") is None and not check_integer_bigger_than_zero(
                self._message_tokens.get("DURATION")):
            well_formed = False

        return well_formed

    def verify_update_message(self):

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
        elif self._message_tokens.get("VALIDITY_BEGIN") is None and not check_integer_bigger_than_zero(
                self._message_tokens.get("VALIDITY_BEGIN")):
            well_formed = False
        elif self._message_tokens.get("VALIDITY_END") is None and not check_integer_bigger_than_zero(
                self._message_tokens.get("VALIDITY_END")):
            well_formed = False
        else:
            begin_tag = int(self._message_tokens.get("VALIDITY_BEGIN"))
            end_tag = int(self._message_tokens.get("VALIDITY_END"))
            current_timestamp = int(round(time.time() * 1000))

            tags_correct = begin_tag < current_timestamp < end_tag

            if not tags_correct:
                well_formed = False

        return well_formed

    def verify_request_message(self):

        well_formed = True

        if self._message_tokens.get("ENTITY_ID") is None:
            well_formed = False
        elif self._message_tokens.get("ENTITY_TYPE") is None:
            well_formed = False
        elif self._message_tokens.get("SCOPE") is None:
            well_formed = False

        return well_formed

    def is_well_formed(self):
        return self._well_formed

    def get_message_token(self, key):
        if self._message_tokens.get(key):
            return self._message_tokens.get(key)
        else:
            return None

    def print_message(self):
        return str(self._message_tokens)

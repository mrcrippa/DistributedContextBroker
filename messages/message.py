from global_data import *

def checkInteger(s):
    try:
        s = int(s)
        result = True
    except ValueError as e:
        result = False
    return result


class Message(object):
    """
    Class representing a message received or sent by the Broker
    """

    def __init__(self, message_raw_data):
        # TODO: parse the message for the two initial headers

        self._message_tokens = {}
        self._well_formed = False
        self._message_type = ""
        self._message_timestamp = 0

        try:
            self._message_tokens = json.loads(message_raw_data)
        except json.JSONDecodeError:
            # TODO: log error trace
            print("Message decode error")

        if self._message_tokens:
            if self._message_tokens.get("TYPE") and self._message_tokens.get("TIMESTAMP"):
                if self._message_tokens.get("TYPE") in message_type_set and checkInteger(self._message_tokens.get("TIMESTAMP")):
                    self._message_type = self._message_tokens.get("TYPE")
                    self._message_timestamp = self._message_tokens.get("TIMESTAMP")
                    self._well_formed = True


    def is_well_formed(self):
        return self._well_formed

    def get_message_token(self, key):
        if self._message_tokens.get(key):
            return self._message_tokens.get(key)
        else:
            return None

    def print_message(self):
        return str(self._message_tokens)



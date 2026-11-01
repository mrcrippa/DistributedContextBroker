import json
from dataclasses import dataclass, fields

@dataclass
class SubscribeMessage:
    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    # for how many seconds should this subscription be valid - integer > 0
    duration: int
    type: str = "subscribe"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_message_tokens_as_subscribe_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed subscribe message
     if yes, returns a SubscribeMessage object
     if not, returns None
     """

    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(SubscribeMessage)}:
        return False

    if message_tokens["type"] != "subscribe":
        return False

    for str_field in [f.name for f in fields(SubscribeMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
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

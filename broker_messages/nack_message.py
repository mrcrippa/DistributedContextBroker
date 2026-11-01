import json
from dataclasses import dataclass, fields


@dataclass
class NackMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    requested_operation: str
    requester_address: str
    requester_port: int
    information: str
    type: str = "nack"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_nack_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed nack message
     if yes, returns a NackMessage object
     if not, returns None
     """

    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(NackMessage)}:
        return False

    if message_tokens["type"] != "nack":
        return False

    for str_field in [f.name for f in fields(NackMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    try:
        b_port = int(message_tokens["broker_port"])
        r_port = int(message_tokens["requester_port"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True

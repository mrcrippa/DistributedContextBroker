import json
from dataclasses import dataclass, fields


@dataclass
class PingMessage:
    # time from the broker internal clock - broker_clock.current_broker_time()
    timestamp: int
    broker_id: str
    broker_address: str
    broker_port: int
    requester_address: str
    requester_port: int
    type: str = "ping"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_message_tokens_as_ping_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed PING message
     if yes, returns a PingMessage object
     if not, returns None
     """

    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(PingMessage)}:
        return False

    if message_tokens["type"] != "ping":
        return False

    for str_field in [f.name for f in fields(PingMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    try:
        b_port = int(message_tokens["broker_port"])
        r_port = int(message_tokens["requester_port"])
        timestamp = int(message_tokens["timestamp"])

        if not (0 < b_port <= 65535):
            return False
        if not (0 < r_port <= 65535):
            return False
        if timestamp <= 0:
            return False

    except (TypeError, ValueError):
        return False

    return True

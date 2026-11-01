import json
from dataclasses import dataclass, fields


@dataclass
class BrokerNeighborsRequestMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    type: str = "broker_neighbors_request"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_message_tokens_as_broker_neighbors_request_msg(message_tokens=None) -> bool:

    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(BrokerNeighborsRequestMessage)}:
        return False

    if message_tokens["type"] != "broker_neighbors_request":
        return False

    for str_field in [f.name for f in fields(BrokerNeighborsRequestMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    try:

        if not (0 < int(message_tokens["broker_port"]) <= 65535):
            return False

    except (TypeError, ValueError):
        return False

    return True

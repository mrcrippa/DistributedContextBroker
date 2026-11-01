import json
from dataclasses import dataclass, field, fields


@dataclass
class BrokerNeighborsResponseMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    broker_neighbors: list[dict] = field(default_factory=list)
    type: str = "broker_neighbors_response"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_message_tokens_as_broker_neighbors_response_msg(
        message_tokens=None) -> bool:
    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(BrokerNeighborsResponseMessage)}:
        return False

    if message_tokens["type"] != "broker_neighbors_response":
        return False

    for str_field in [f.name for f in fields(BrokerNeighborsResponseMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    # this loop checks the broker_neighbors list
    # each entry is a dict with {broker_id, broker_address,broker_port}
    # broker_id should be a non-empty string
    # broker_address should be a non-empty string
    if not isinstance(message_tokens["broker_neighbors"], list):
        return False

    for neighbor in message_tokens["checked_brokers"]:
        if not isinstance(neighbor, dict) or not neighbor or neighbor.keys() != {"broker_id", "broker_address", "broker_port"}:
            return False
        if not (isinstance(neighbor["broker_id"], str) and neighbor["broker_id"].strip()):
            return False
        if not (isinstance(neighbor["broker_address"], str) and neighbor["broker_address"].strip()):
            return False

    try:

        if not (0 < int(message_tokens["broker_port"]) <= 65535):
            return False

        # this loop checks the broker_neighbors list
        # broker_port should be a valid port
        for neighbor in message_tokens["broker_neighbors"]:
            if not (0 < int(neighbor["broker_port"]) <= 65535):
                return False

    except (TypeError, ValueError):
        return False

    return True

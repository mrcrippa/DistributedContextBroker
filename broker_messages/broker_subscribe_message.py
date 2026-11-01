import json
from dataclasses import dataclass, field, fields


@dataclass
class BrokerSubscribeMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    # for how many seconds should this subscription be valid - integer > 0
    duration: int
    max_hops: int
    checked_brokers: list = field(default_factory=list)
    type: str = "broker_subscribe"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_message_tokens_as_broker_subscribe_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed broker_subscribe message
     if yes, returns a BrokerSubscribeMessage object
     if not, returns None
     """

    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(BrokerSubscribeMessage)}:
        return False

    if message_tokens["type"] != "broker_subscribe":
        return False

    for str_field in [f.name for f in fields(BrokerSubscribeMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    if not isinstance(message_tokens["checked_brokers"], list):
        return False

    for broker_id in message_tokens["checked_brokers"]:
        if not isinstance(broker_id, str) or not broker_id:
            return False

    try:
        s_port = int(message_tokens["subscriber_port"])
        duration = int(message_tokens["duration"])
        max_hops = int(message_tokens["max_hops"])

        if not (0 < s_port <= 65535):
            return False
        if duration <= 0:
            return False
        if max_hops < 0:
            return False

    except (TypeError, ValueError):
        return False

    return True

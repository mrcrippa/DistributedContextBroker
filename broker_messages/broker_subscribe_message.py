import json
from dataclasses import dataclass, field


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
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_broker_subscribe_msg(message_tokens=None) -> BrokerSubscribeMessage | None:
    """check if the message provided is a well-formed broker_subscribe message
     if yes, returns a BrokerSubscribeMessage object
     if not, returns None
     """
    try:
        if message_tokens is None:
            return None
        elif message_tokens.get("broker_id") is None or not message_tokens.get("broker_id"):
            return None
        elif message_tokens.get("broker_address") is None or not message_tokens.get("broker_address"):
            return None
        elif message_tokens.get(
                "broker_port") is None or not message_tokens.get(
            "broker_port") or int(message_tokens.get("broker_port")) <= 0:
            return None
        elif message_tokens.get("entity_id") is None or not message_tokens.get("entity_id"):
            return None
        elif message_tokens.get("entity_type") is None or not message_tokens.get("entity_type"):
            return None
        elif message_tokens.get("scope") is None or not message_tokens.get("scope"):
            return None
        elif message_tokens.get("subscriber_address") is None or not message_tokens.get("subscriber_address"):
            return None
        elif message_tokens.get(
                "subscriber_port") is None or not message_tokens.get(
            "subscriber_port") or int(message_tokens.get("subscriber_port")) <= 0:
            return None
        elif message_tokens.get("duration") is None or not message_tokens.get("duration") or int(
                message_tokens.get("duration")) <= 0:
            return None
        elif message_tokens.get("max_hops") is None or not message_tokens.get("max_hops") or int(
                message_tokens.get("max_hops")) < 0:
            return None
        elif message_tokens.get("checked_brokers") is None or not message_tokens.get("checked_brokers") or type(
                message_tokens.get("checked_brokers")).__name__ != "list":
            return None

        for checked_broker_id in message_tokens.get("checked_brokers"):
            if checked_broker_id is None or not checked_broker_id:
                return None

    except (TypeError, ValueError):
        return None

    return BrokerSubscribeMessage(
        broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
        broker_port=message_tokens.get("broker_port"),
        entity_id=message_tokens.get("entity_id"), entity_type=message_tokens.get("entity_type"),
        duration=message_tokens.get("duration"), scope=message_tokens.get("scope"),
        subscriber_address=message_tokens.get("subscriber_address"),
        subscriber_port=message_tokens.get("subscriber_port"),
        checked_brokers=message_tokens.get_token("checked_brokers"),
        max_hops=message_tokens.get("max_hops"),
        well_formed=True
    )

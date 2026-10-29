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
    type: str = "BROKER_SUBSCRIBE"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_broker_subscribe_msg(message_tokens=None) -> BrokerSubscribeMessage | None:
    """check if the message provided is a well-formed BROKER_SUBSCRIBE message
     if yes, returns a BrokerSubscribeMessage object
     if not, returns None
     """
    try:
        if message_tokens is None:
            return None
        elif message_tokens.get("BROKER_ID") is None or message_tokens.get("BROKER_ID"):
            return None
        elif message_tokens.get("BROKER_ADDRESS") is None or message_tokens.get("BROKER_ADDRESS"):
            return None
        elif message_tokens.get(
                "BROKER_PORT") is None or message_tokens.get(
            "BROKER_PORT") or int(message_tokens.get("BROKER_PORT")) <= 0:
            return None
        elif message_tokens.get("ENTITY_ID") is None or message_tokens.get("ENTITY_ID"):
            return None
        elif message_tokens.get("ENTITY_TYPE") is None or message_tokens.get("ENTITY_TYPE"):
            return None
        elif message_tokens.get("SCOPE") is None or message_tokens.get("SCOPE"):
            return None
        elif message_tokens.get("SUBSCRIBER_ADDRESS") is None or message_tokens.get("SUBSCRIBER_ADDRESS"):
            return None
        elif message_tokens.get(
                "SUBSCRIBER_PORT") is None or message_tokens.get(
            "SUBSCRIBER_PORT") or int(message_tokens.get("SUBSCRIBER_PORT")) <= 0:
            return None
        elif message_tokens.get("DURATION") is None or message_tokens.get("DURATION") or int(
                message_tokens.get("DURATION")) <= 0:
            return None
        elif message_tokens.get("MAX_HOPS") is None or message_tokens.get("MAX_HOPS") or int(
                message_tokens.get("MAX_HOPS")) < 0:
            return None
        elif message_tokens.get("CHECKED_BROKERS") is None or message_tokens.get("CHECKED_BROKERS") or type(
                message_tokens.get("CHECKED_BROKERS")).__name__ != "list":
            return None

    except (TypeError, ValueError):
        return None

    return BrokerSubscribeMessage(
        broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
        broker_port=message_tokens.get("BROKER_PORT"),
        entity_id=message_tokens.get("ENTITY_ID"), entity_type=message_tokens.get("ENTITY_TYPE"),
        duration=message_tokens.get("DURATION"), scope=message_tokens.get("SCOPE"),
        subscriber_address=message_tokens.get("SUBSCRIBER_ADDRESS"),
        subscriber_port=message_tokens.get("SUBSCRIBER_PORT"),
        checked_brokers=message_tokens.get_token("CHECKED_BROKERS"),
        well_formed=True
    )

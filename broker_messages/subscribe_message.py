import json
from dataclasses import dataclass


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
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_subscribe_msg(message_tokens=None) -> SubscribeMessage | None:
    """check if the message provided is a well-formed subscribe message
     if yes, returns a SubscribeMessage object
     if not, returns None
     """
    try:
        if message_tokens is None:
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

    except (TypeError, ValueError):
        return None

    return SubscribeMessage(
        entity_id=message_tokens.get("entity_id"), entity_type=message_tokens.get("entity_type"),
        duration=message_tokens.get("duration"), scope=message_tokens.get("scope"),
        subscriber_address=message_tokens.get("subscriber_address"),
        subscriber_port=message_tokens.get("subscriber_port"),
        well_formed=True
    )

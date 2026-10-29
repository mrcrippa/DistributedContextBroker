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
    type: str = "SUBSCRIBE"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_subscribe_msg(message_tokens=None) -> SubscribeMessage | None:
    """check if the message provided is a well-formed SUBSCRIBE message
     if yes, returns a SubscribeMessage object
     if not, returns None
     """
    try:
        if message_tokens is None:
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

    except (TypeError, ValueError):
        return None

    return SubscribeMessage(
        entity_id=message_tokens.get("ENTITY_ID"), entity_type=message_tokens.get("ENTITY_TYPE"),
        duration=message_tokens.get("DURATION"), scope=message_tokens.get("SCOPE"),
        subscriber_address=message_tokens.get("SUBSCRIBER_ADDRESS"),
        subscriber_port=message_tokens.get("SUBSCRIBER_PORT"),
        well_formed=True
    )

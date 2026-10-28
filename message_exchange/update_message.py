import json
from dataclasses import dataclass

from global_data import broker_clock


@dataclass
class UpdateMessage:
    entity_id: str
    entity_type: str
    scope: str
    provider_id: str
    data: str
    # timestamp for the end of the time period when the context info is valid
    # time expressed as milliseconds UNIX time
    validity_end: int
    type: str = "UPDATE"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_update_message(message_tokens=None):
    """check if the message provided is a well-formed UPDATE message """

    try:
        if message_tokens is None:
            return False
        elif message_tokens.get("ENTITY_ID") is None:
            return False
        elif message_tokens.get("ENTITY_TYPE") is None:
            return False
        elif message_tokens.get("SCOPE") is None:
            return False
        elif message_tokens.get("PROVIDER_ID") is None:
            return False
        elif message_tokens.get("DATA") is None:
            return False
        elif message_tokens.get("VALIDITY_END") is None or int(message_tokens.get("VALIDITY_END")) <= 0:
            return False

        # validity end should come after the current broker time
        if not (broker_clock.current_broker_time() < int(message_tokens.get("VALIDITY_END"))):
            return False

    except (TypeError, ValueError):
        return False

    return True


def create_UPDATE_msg(message_tokens=None) -> UpdateMessage | None:
    if verify_update_message(message_tokens):
        return UpdateMessage(
            entity_id=message_tokens.get("ENTITY_ID"), entity_type=message_tokens.get("ENTITY_TYPE"),
            provider_id=message_tokens.get("PROVIDER_ID"), scope=message_tokens.get("SCOPE"),
            data=message_tokens.get("DATA"), validity_end=message_tokens.get("VALIDITY_END"),
            well_formed=True
        )
    return None

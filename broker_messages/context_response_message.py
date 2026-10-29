import json
from dataclasses import dataclass

from global_data import broker_clock


@dataclass
class ContextResponseMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    entity_id: str
    entity_type: str
    scope: str
    provider_id: str
    provider_address: str
    provider_port: int
    data: str
    # timestamp for the end of the time period when the context info is valid
    # time expressed as milliseconds UNIX time
    validity_end: int
    type: str = "CONTEXT_RESPONSE"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_context_response_msg(message_tokens=None) -> ContextResponseMessage | None:
    """check if the message provided is a well-formed CONTEXT_RESPONSE message
     if yes, returns a ContextResponseMessage object
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
        elif message_tokens.get("PROVIDER_ID") is None or message_tokens.get("PROVIDER_ID"):
            return None
        elif message_tokens.get("PROVIDER_ID") is None or message_tokens.get("PROVIDER_ID"):
            return None
        elif message_tokens.get("PROVIDER_ADDRESS") is None or message_tokens.get("PROVIDER_ADDRESS"):
            return None
        elif message_tokens.get("DATA") is None or message_tokens.get("DATA"):
            return None
        elif message_tokens.get("VALIDITY_END") is None or message_tokens.get("VALIDITY_END") or int(
                message_tokens.get("VALIDITY_END")) <= 0:
            return None

        # validity end should come after the current broker time
        if not (broker_clock.current_broker_time() < int(message_tokens.get("VALIDITY_END"))):
            return None

    except (TypeError, ValueError):
        return None

    return ContextResponseMessage(
        broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
        broker_port=message_tokens.get("BROKER_PORT"),
        entity_id=message_tokens.get("ENTITY_ID"), entity_type=message_tokens.get("ENTITY_TYPE"),
        provider_id=message_tokens.get("PROVIDER_ID"), scope=message_tokens.get("SCOPE"),
        data=message_tokens.get("DATA"), validity_end=message_tokens.get("VALIDITY_END"),
        well_formed=True
    )

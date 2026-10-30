import json
from dataclasses import dataclass

from global_data import broker_internal_clock


@dataclass
class UpdateMessage:
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
    type: str = "update"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_update_msg(message_tokens=None) -> UpdateMessage | None:
    """check if the message provided is a well-formed update message
     if yes, returns a UpdateMessage object
     if not, returns None
     """
    # broker_logger.debug("message_tokens : " + str(message_tokens))

    try:
        if message_tokens is None:
            return None
        elif message_tokens.get("entity_id") is None or not message_tokens.get("entity_id"):
            return None
        elif message_tokens.get("entity_type") is None or not message_tokens.get("entity_type"):
            return None
        elif message_tokens.get("scope") is None or not message_tokens.get("scope"):
            return None
        elif message_tokens.get("provider_id") is None or not message_tokens.get("provider_id"):
            return None
        elif message_tokens.get("provider_address") is None or not message_tokens.get("provider_address"):
            return None
        elif message_tokens.get(
                "provider_port") is None or not message_tokens.get(
            "provider_port") or int(message_tokens.get("provider_port")) <= 0:
            return None
        elif message_tokens.get("data") is None or not message_tokens.get("data"):
            return None
        elif message_tokens.get("validity_end") is None or not message_tokens.get("validity_end") or int(
                message_tokens.get("validity_end")) <= 0:
            return None

        # validity end should come after the current broker time
        if not (broker_internal_clock.current_broker_time() < int(message_tokens.get("validity_end"))):
            return None

    except (TypeError, ValueError):
        return None

    return UpdateMessage(
        entity_id=message_tokens.get("entity_id"), entity_type=message_tokens.get("entity_type"),
        provider_id=message_tokens.get("provider_id"), scope=message_tokens.get("scope"),
        data=message_tokens.get("data"), validity_end=message_tokens.get("validity_end"),
        provider_address=message_tokens.get("provider_address"), provider_port=message_tokens.get("provider_port"),
        well_formed=True
    )

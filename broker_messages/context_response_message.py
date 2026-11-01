import json
from dataclasses import dataclass, fields

from global_data import broker_internal_clock


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
    type: str = "context_response"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_context_response_msg(message_tokens=None) -> bool:
    """check if the message provided is a well-formed context_response message
     if yes, returns a ContextResponseMessage object
     if not, returns None
     """

    if message_tokens is None or not isinstance(message_tokens, dict):
        return False

    if message_tokens.keys() != {f.name for f in fields(ContextResponseMessage)}:
        return False

    if message_tokens["type"] != "context_response":
        return False

    for str_field in [f.name for f in fields(ContextResponseMessage) if f.type in (str, 'str')]:
        val = message_tokens[str_field]
        if not isinstance(val, str) or not val.strip():
            return False

    try:

        p_port = int(message_tokens["provider_port"])
        b_port = int(message_tokens["broker_port"])

        if not (0 < p_port <= 65535):
            return False
        if not (0 < b_port <= 65535):
            return False

        # validity end should come after the current broker time
        if broker_internal_clock.current_broker_time() >= int(message_tokens["validity_end"]):
            return False

    except (TypeError, ValueError):
        return False

    return True

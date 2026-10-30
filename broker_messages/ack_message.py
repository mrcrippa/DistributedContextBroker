import json
from dataclasses import dataclass


@dataclass
class AckMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    requested_operation: str
    requester_address: str
    requester_port: int
    information: str
    type: str = "ack"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_ack_msg(message_tokens=None) -> AckMessage | None:
    """check if the message provided is a well-formed ack message
     if yes, returns a AckMessage object
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
        elif message_tokens.get("requested_operation") is None or not message_tokens.get("requested_operation"):
            return None
        elif message_tokens.get("requester_address") is None or not message_tokens.get("requester_address"):
            return None
        elif message_tokens.get(
                "requester_port") is None or not message_tokens.get(
            "requester_port") or int(message_tokens.get("requester_port")) <= 0:
            return None
        elif message_tokens.get("information") is None or not message_tokens.get("information"):
            return None
    except (TypeError, ValueError):
        return None

    return AckMessage(
        broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
        broker_port=message_tokens.get("broker_port"),
        requested_operation=message_tokens.get("requested_operation"),
        requester_address=message_tokens.get("requester_address"),
        requester_port=message_tokens.get("requester_port"),
        information=message_tokens.get("information"), well_formed=True
    )

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
    type: str = "ACK"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_ack_message(message_tokens=None):
    """check if the message provided is a well-formed ACK message """

    return True


def process_message_tokens_as_ack_msg(message_tokens=None) -> AckMessage | None:
    """check if the message provided is a well-formed ACK message
     if yes, returns a AckMessage object
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
        elif message_tokens.get("REQUESTED_OPERATION") is None or message_tokens.get("REQUESTED_OPERATION"):
            return None
        elif message_tokens.get("REQUESTER_ADDRESS") is None or message_tokens.get("REQUESTER_ADDRESS"):
            return None
        elif message_tokens.get(
                "REQUESTER_PORT") is None or message_tokens.get(
            "REQUESTER_PORT") or int(message_tokens.get("REQUESTER_PORT")) <= 0:
            return None
        elif message_tokens.get("INFORMATION") is None or message_tokens.get("INFORMATION"):
            return None
    except (TypeError, ValueError):
        return None

    return AckMessage(
        broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
        broker_port=message_tokens.get("BROKER_PORT"),
        requested_operation=message_tokens.get("REQUESTED_OPERATION"),
        requester_address=message_tokens.get("REQUESTER_ADDRESS"),
        requester_port=message_tokens.get("REQUESTER_PORT"),
        information=message_tokens.get("INFORMATION"), well_formed=True
    )

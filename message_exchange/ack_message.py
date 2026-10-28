import json
from dataclasses import dataclass


@dataclass
class ACKMessage:
    broker_id: str
    broker_address: str
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

    try:
        if message_tokens is None:
            return False
        elif message_tokens.get("BROKER_ID") is None:
            return False
        elif message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif message_tokens.get(
                "BROKER_PORT") is None or int(message_tokens.get("BROKER_PORT")) <= 0:
            return False
        elif message_tokens.get("REQUESTED_OPERATION") is None:
            return False
        elif message_tokens.get("REQUESTER_ADDRESS") is None:
            return False
        elif message_tokens.get(
                "REQUESTER_PORT") is None or int(message_tokens.get("REQUESTER_PORT")) <= 0:
            return False
        elif message_tokens.get("INFORMATION") is None:
            return False
    except (TypeError, ValueError):
        return False

    return True

def create_ACK_msg(message_tokens=None) -> ACKMessage | None:
    if verify_ack_message(message_tokens):
        return ACKMessage(
            broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
            broker_port=message_tokens.get("BROKER_PORT"),
            requested_operation=message_tokens.get("REQUESTED_OPERATION"),
            requester_address=message_tokens.get("REQUESTER_ADDRESS"),
            requester_port=message_tokens.get("REQUESTER_PORT"),
            information=message_tokens.get("INFORMATION"), well_formed=True
        )
    return None

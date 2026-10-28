import json
from dataclasses import dataclass


@dataclass
class PINGMessage:
    timestamp: int
    broker_id: str
    broker_address: str
    broker_port: int
    requester_address: str
    requester_port: int
    type: str = "PING"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_ping_message(message_tokens=None):
    """check if the message provided is a well-formed PING message """

    try:
        if message_tokens is None:
            return False
        elif message_tokens.get("TIMESTAMP") is None or int(message_tokens.get("TIMESTAMP")) <= 0:
            return False
        elif message_tokens.get("BROKER_ID") is None:
            return False
        elif message_tokens.get("BROKER_ADDRESS") is None:
            return False
        elif message_tokens.get(
                "BROKER_PORT") is None or int(message_tokens.get("BROKER_PORT")) <= 0:
            return False
        elif message_tokens.get("REQUESTER_ADDRESS") is None:
            return False
        elif message_tokens.get(
                "REQUESTER_PORT") is None or int(message_tokens.get("REQUESTER_PORT")) <= 0:
            return False
    except (TypeError, ValueError):
        return False

    return True


def create_PING_msg(message_tokens=None) -> PINGMessage | None:
    if verify_ping_message(message_tokens):
        return PINGMessage(
            broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
            broker_port=message_tokens.get("BROKER_PORT"), timestamp=message_tokens.get("TIMESTAMP"), well_formed=True,
            requester_port=message_tokens.get("REQUESTER_PORT"),
            requester_address=message_tokens.get("REQUESTER_ADDRESS")
        )
    return None

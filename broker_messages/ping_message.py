import json
from dataclasses import dataclass


@dataclass
class PingMessage:
    # time from the broker internal clock - broker_clock.current_broker_time()
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


def process_message_tokens_as_ping_msg(message_tokens=None) -> PingMessage | None:
    """check if the message provided is a well-formed PING message
     if yes, returns a PingMessage object
     if not, returns None
     """

    try:
        if message_tokens is None:
            return None
        elif message_tokens.get("TIMESTAMP") is None or message_tokens.get("TIMESTAMP") or int(
                message_tokens.get("TIMESTAMP")) <= 0:
            return None
        elif message_tokens.get("BROKER_ID") is None or message_tokens.get("BROKER_ID"):
            return None
        elif message_tokens.get("BROKER_ADDRESS") is None or message_tokens.get("BROKER_ADDRESS"):
            return None
        elif message_tokens.get(
                "BROKER_PORT") is None or message_tokens.get(
            "BROKER_PORT") or int(message_tokens.get("BROKER_PORT")) <= 0:
            return None
        elif message_tokens.get("REQUESTER_ADDRESS") is None or message_tokens.get("REQUESTER_ADDRESS"):
            return None
        elif message_tokens.get(
                "REQUESTER_PORT") is None or message_tokens.get(
            "REQUESTER_PORT") or int(message_tokens.get("REQUESTER_PORT")) <= 0:
            return None
    except (TypeError, ValueError):
        return None

    return PingMessage(
        broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
        broker_port=message_tokens.get("BROKER_PORT"), timestamp=message_tokens.get("TIMESTAMP"), well_formed=True,
        requester_port=message_tokens.get("REQUESTER_PORT"),
        requester_address=message_tokens.get("REQUESTER_ADDRESS")
    )

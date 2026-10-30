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
    type: str = "ping"
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
        elif message_tokens.get("timestamp") is None or not message_tokens.get("timestamp") or int(
                message_tokens.get("timestamp")) <= 0:
            return None
        elif message_tokens.get("broker_id") is None or not message_tokens.get("broker_id"):
            return None
        elif message_tokens.get("broker_address") is None or not message_tokens.get("broker_address"):
            return None
        elif message_tokens.get(
                "broker_port") is None or not message_tokens.get(
            "broker_port") or int(message_tokens.get("broker_port")) <= 0:
            return None
        elif message_tokens.get("requester_address") is None or not message_tokens.get("requester_address"):
            return None
        elif message_tokens.get(
                "requester_port") is None or not message_tokens.get(
            "requester_port") or int(message_tokens.get("requester_port")) <= 0:
            return None
    except (TypeError, ValueError):
        return None

    return PingMessage(
        broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
        broker_port=message_tokens.get("broker_port"), timestamp=message_tokens.get("timestamp"), well_formed=True,
        requester_port=message_tokens.get("requester_port"),
        requester_address=message_tokens.get("requester_address")
    )

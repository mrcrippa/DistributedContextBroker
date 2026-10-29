import json
from dataclasses import dataclass


@dataclass
class BrokerNeighborsRequestMessage:
    # time from the broker internal clock - broker_clock.current_broker_time()
    broker_id: str
    broker_address: str
    broker_port: int
    type: str = "BROKER_NEIGHBORS_REQUEST"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def verify_broker_neighbors_request_message(message_tokens=None):
    """check if the message provided is a well-formed BROKER_NEIGHBORS_REQUEST message """

    try:
        if message_tokens is None:
            return False
        elif message_tokens.get("BROKER_ID") is None or message_tokens.get("BROKER_ID"):
            return False
        elif message_tokens.get("BROKER_ADDRESS") is None or message_tokens.get("BROKER_ADDRESS"):
            return False
        elif message_tokens.get(
                "BROKER_PORT") is None or message_tokens.get(
            "BROKER_PORT") or int(message_tokens.get("BROKER_PORT")) <= 0:
            return False
    except (TypeError, ValueError):
        return False

    return True


def process_message_tokens_as_broker_neighbors_request_msg(message_tokens=None) -> BrokerNeighborsRequestMessage | None:
    if verify_broker_neighbors_request_message(message_tokens):
        return BrokerNeighborsRequestMessage(
            broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
            broker_port=message_tokens.get("BROKER_PORT"), well_formed=True
        )
    return None

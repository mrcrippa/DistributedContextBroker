import json
from dataclasses import dataclass


@dataclass
class BrokerNeighborsRequestMessage:
    # time from the broker internal clock - broker_clock.current_broker_time()
    broker_id: str
    broker_address: str
    broker_port: int
    type: str = "broker_neighbors_request"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_broker_neighbors_request_msg(message_tokens=None) -> BrokerNeighborsRequestMessage | None:
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
    except (TypeError, ValueError):
        return None

    return BrokerNeighborsRequestMessage(
        broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
        broker_port=message_tokens.get("broker_port"), well_formed=True
    )

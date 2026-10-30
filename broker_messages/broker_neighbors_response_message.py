import json
from dataclasses import dataclass, field


@dataclass
class BrokerNeighborsResponseMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    broker_neighbors: list[dict] = field(default_factory=list)
    type: str = "broker_neighbors_response"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_broker_neighbors_response_msg(
        message_tokens=None) -> BrokerNeighborsResponseMessage | None:
    try:
        if message_tokens is None:
            return None
        elif message_tokens.get("broker_id") is None or not message_tokens.get("broker_id"):
            return None
        elif message_tokens.get("broker_address") is None or not message_tokens.get("broker_address"):
            return None
        elif message_tokens.get("broker_neighbors") is None or not message_tokens.get("broker_neighbors") or type(
                message_tokens.get("broker_neighbors")).__name__ != "list":
            return None
        elif message_tokens.get(
                "broker_port") is None or not message_tokens.get(
            "broker_port") or int(message_tokens.get("broker_port")) <= 0:
            return None

        for neighbor in message_tokens.get("broker_neighbors"):
            if neighbor.get("broker_id") is None or not neighbor.get("broker_id") or neighbor.get(
                    "broker_address") is None or not neighbor.get("broker_address") or neighbor.get(
                "broker_port") is None or not neighbor.get(
                "broker_port") or int(neighbor.get("broker_port")) <= 0:
                return None

    except (TypeError, ValueError):
        return None

    neighbors = list()
    for entry in message_tokens.get("broker_neighbors"):
        neighbor = {"broker_id": str(entry.get("broker_id")), "broker_address": str(entry.get("broker_address")),
                    "broker_port": int(entry.get("broker_port"))}
        neighbors.append(neighbor)

    return BrokerNeighborsResponseMessage(
        broker_id=message_tokens.get("broker_id"), broker_address=message_tokens.get("broker_address"),
        broker_port=message_tokens.get("broker_port"), well_formed=True, broker_neighbors=neighbors
    )

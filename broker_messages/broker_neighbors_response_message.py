import json
from dataclasses import dataclass, field


@dataclass
class BrokerNeighborsResponseMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    broker_neighbors: list[dict] = field(default_factory=list)
    type: str = "BROKER_NEIGHBORS_REQUEST"
    well_formed: bool = False

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def process_message_tokens_as_broker_neighbors_response_msg(
        message_tokens=None) -> BrokerNeighborsResponseMessage | None:
    try:
        if message_tokens is None:
            return None
        elif message_tokens.get("BROKER_ID") is None or message_tokens.get("BROKER_ID"):
            return None
        elif message_tokens.get("BROKER_ADDRESS") is None or message_tokens.get("BROKER_ADDRESS"):
            return None
        elif message_tokens.get("BROKER_NEIGHBORS") is None or message_tokens.get("BROKER_NEIGHBORS") or type(
                message_tokens.get("BROKER_NEIGHBORS")).__name__ != "list":
            return None
        elif message_tokens.get(
                "BROKER_PORT") is None or message_tokens.get(
            "BROKER_PORT") or int(message_tokens.get("BROKER_PORT")) <= 0:
            return None

        for neighbor in message_tokens.get("BROKER_NEIGHBORS"):
            if neighbor.get("BROKER_ID") is None or neighbor.get("BROKER_ID") or neighbor.get(
                    "BROKER_ADDRESS") is None or neighbor.get("BROKER_ADDRESS") or neighbor.get(
                "BROKER_PORT") is None or int(neighbor.get("BROKER_PORT")) <= 0:
                return None

    except (TypeError, ValueError):
        return None

    neighbors = list()
    for entry in message_tokens.get_token("BROKER_NEIGHBORS"):
        neighbor = {"BROKER_ID": str(entry.get("BROKER_ID")), "BROKER_ADDRESS": str(entry.get("BROKER_ADDRESS")),
                    "BROKER_PORT": int(entry.get("BROKER_PORT"))}
        neighbors.append(neighbor)

    return BrokerNeighborsResponseMessage(
        broker_id=message_tokens.get("BROKER_ID"), broker_address=message_tokens.get("BROKER_ADDRESS"),
        broker_port=message_tokens.get("BROKER_PORT"), well_formed=True, broker_neighbors=neighbors
    )

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

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


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

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class UpdateMessage:
    entity_id: str
    entity_type: str
    scope: str
    provider_id: str
    provider_address: str
    provider_port: int
    data: str
    # timestamp for the end of the time period when the context info is valid
    # time expressed as milliseconds UNIX time
    validity_end: int
    type: str = "update"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class SubscribeMessage:
    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    # for how many seconds should this subscription be valid - integer > 0
    duration: int
    type: str = "subscribe"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class NackMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    requested_operation: str
    requester_address: str
    requester_port: int
    information: str
    type: str = "nack"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class ContextResponseMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    entity_id: str
    entity_type: str
    scope: str
    provider_id: str
    provider_address: str
    provider_port: int
    data: str
    # timestamp for the end of the time period when the context info is valid
    # time expressed as milliseconds UNIX time
    validity_end: int
    type: str = "context_response"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BrokerSubscribeMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    # for how many seconds should this subscription be valid - integer > 0
    duration: int
    max_hops: int
    checked_brokers: list = field(default_factory=list)
    type: str = "broker_subscribe"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BrokerNeighborsResponseMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    broker_neighbors: list[dict] = field(default_factory=list)
    type: str = "broker_neighbors_response"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BrokerNeighborsRequestMessage:
    broker_id: str
    broker_address: str
    broker_port: int
    type: str = "broker_neighbors_request"

    def as_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

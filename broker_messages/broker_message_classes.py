"""
This module contains dataclasses defining all the messages exchanged between (context) broker and (context) consumers
broker and (context) providers, and between brokers.

All messages exchanged are JSON strings, so all classes provide a method for JSON serialization into a string.

Classes
--------
AckMessage
NackMessage
PingMessage
UpdateMessage
ContextResponseMessage
SubscribeMessage
BrokerSubscribeMessage
BrokerNeighborsRequestMessage
BrokerNeighborsResponseMessage
"""

import json
from dataclasses import dataclass, field


@dataclass
class AckMessage:
    """
        Class that defines an acknowledgement message sent by the Broker to consumers or providers,
        when a requested operation is successful.

        Attributes
        ----------
        broker_id : str
            unique identifier for this broker
        broker_address : str
            address for this broker
        broker_port : int
            port for this broker
        requested_operation : str
            the operation requested (for instance, update or subscribe)
        requester_address : str
            address for the requester
        requester_port : int
            port for the requester
        information : str
            extra information related to the results of the requested operation
        type: str
            message type (default is "ack")
    """
    broker_id: str
    broker_address: str
    broker_port: int
    requested_operation: str
    requester_address: str
    requester_port: int
    information: str
    type: str = "ack"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class NackMessage:
    """
        Class that defines a non-acknowledgement message sent by the Broker to consumers or providers,
        when a requested operation is unsuccessful.

        Attributes
        ----------
        broker_id : str
            unique identifier for this broker
        broker_address : str
            address for this broker
        broker_port : int
            port for this broker
        requested_operation : str
            the operation requested (for instance, update or subscribe)
        requester_address : str
            address for the requester
        requester_port : int
            port for the requester
        information : str
            extra information related to the results of the requested operation
        type: str
            message type (default is "nack")
    """

    broker_id: str
    broker_address: str
    broker_port: int
    requested_operation: str
    requester_address: str
    requester_port: int
    information: str
    type: str = "nack"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class PingMessage:
    """
        Class that defines a ping message sent or received by the Broker

        Attributes
        ----------
        timestamp : int
            timestamp expressed as milliseconds UNIX time
        broker_id : str
            unique identifier for this broker
        broker_address : str
            address for this broker
        broker_port : int
            port for this broker
        requester_address : str
            address for the requester
        requester_port : int
            port for the requester
        type: str
            message type (default is "ping")
    """

    timestamp: int
    broker_id: str
    broker_address: str
    broker_port: int
    requester_address: str
    requester_port: int
    type: str = "ping"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class UpdateMessage:
    """
        Class that defines an update message with context information sent to the Broker by a context provider

        Attributes
        ----------
        entity_id : str
            unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
        entity_type : str
            specifies the type of entity (for instance, "city" or "person")
        scope : str
            specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")
        provider_id : str
            unique identifier for the context provider
        provider_address : str
            address for the provider
        provider_port : int
            port for the provider
        data : str
            string with the value for the indicated scope (for instance, "temperature_celsius=21.0" or "height=1.8m")
        validity_end : int
                timestamp for the end of the time period when the context info is still valid,
                time expressed as milliseconds UNIX time, should come after the current internal broker clock
        type: str
            message type (default is "update")
    """

    entity_id: str
    entity_type: str
    scope: str
    provider_id: str
    provider_address: str
    provider_port: int
    data: str
    validity_end: int
    type: str = "update"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class ContextInfoMessage:
    """
            Class that defines a context information message, sent by the broker to all consumers subscribed to this
            (entity_id, entity_type,scope) context information.

            Attributes
            ----------
            broker_id : str
                unique identifier for this broker
            broker_address : str
                address for this broker
            broker_port : int
                port for this broker
            entity_id : str
                unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
            entity_type : str
                specifies the type of entity (for instance, "city" or "person")
            scope : str
                specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")
            provider_id : str
                unique identifier for the context provider
            provider_address : str
                address for the provider
            provider_port : int
                port for the provider
            data : str
                string with the value for the indicated scope (for instance, "temperature_celsius=21.0" or "height=1.8m")
            validity_end : int
                    timestamp for the end of the time period when the context info is still valid,
                    time expressed as milliseconds UNIX time, should come after the current internal broker clock
            type: str
                message type (default is "context_info")
        """

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
    validity_end: int
    type: str = "context_info"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class SubscribeMessage:
    """
        Class that defines a subscription request message sent to the Broker by a context consumer

        Attributes
        ----------
        entity_id : str
            unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
        entity_type : str
            specifies the type of entity (for instance, "city" or "person")
        scope : str
            specifies which contextual attribute this subscription relates to (for instance, "current_temperature" or "body_measurements")
        subscriber_address : str
            address for the subscriber
        subscriber_port : int
            port for the subscriber
        duration : int
            for how many seconds should this subscription active - should be an integer > 0
        type: str
            message type (default is "subscribe")
    """

    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    duration: int
    type: str = "subscribe"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BrokerSubscribeMessage:
    """
         Class that defines a broker subscription request message sent by a broker to another broker, when searching
         the broker network for a match for a subscription request

        Attributes
        ----------
        broker_id : str
            unique identifier for this broker
        broker_address : str
            address for this broker
        broker_port : int
            port for this broker
        entity_id : str
            unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
        entity_type : str
            specifies the type of entity (for instance, "city" or "person")
        scope : str
            specifies which contextual attribute this subscription relates to (for instance, "current_temperature" or "body_measurements")
        subscriber_address : str
            address for the subscriber
        subscriber_port : int
            port for the subscriber
        duration : int
            for how many seconds should this subscription active - should be an integer > 0
        max_hops: int
            maximum number of hops allowed for this search. If 0, then this broker searches for a match locally but does
            not forward the broker subscribe any further
        checked_brokers : list
            list with the broker ids of all broker already visited by the search. This avoids repeated requests. (default is [])
        type: str
            message type (default is "broker_subscribe")
        """

    broker_id: str
    broker_address: str
    broker_port: int
    entity_id: str
    entity_type: str
    scope: str
    subscriber_address: str
    subscriber_port: int
    duration: int
    max_hops: int
    checked_brokers: list = field(default_factory=list)
    type: str = "broker_subscribe"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BrokerNeighborsRequestMessage:
    """
        Class that defines a broker neighbors request message sent by a broker to this broker,
        asking for this broker's list of neighbors. This can also be used as a still alive check between brokers.

        Attributes
        ----------
        broker_id : str
            unique identifier for the requesting broker
        broker_address : str
            address for the requesting broker
        broker_port : int
            port for the requesting broker
        type: str
            message type (default is "broker_neighbors_request")
    """

    broker_id: str
    broker_address: str
    broker_port: int
    type: str = "broker_neighbors_request"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BrokerNeighborsResponseMessage:
    """
        Class that defines a broker neighbors response message sent by this broker to another broker that
        requested this broker's list of neighbors.

        Attributes
        ----------
        broker_id : str
            unique identifier for this broker
        broker_address : str
            address for this broker
        broker_port : int
            port for this broker
        broker_neighbors : list
            a list of dict's with the neighbor brokers from the requested broker. each entry is a dict with
            {broker_id, broker_address,broker_port}. (default is [])
        type: str
            message type (default is "broker_neighbors_response")
    """
    broker_id: str
    broker_address: str
    broker_port: int
    broker_neighbors: list[dict] = field(default_factory=list)
    type: str = "broker_neighbors_response"

    def as_json(self):
        """Returns the data inside the dataclass instance as a JSON string"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

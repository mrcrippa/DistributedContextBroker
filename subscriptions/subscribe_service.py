"""
This module contains functions that processes subscriptions, either from subscribe or broker_subscribe messages.


Functions:
--------
process_broker_subscribe_msg
subscription_broker_network_search
check_for_local_context_info
process_subscribe_msg
"""

from broker_messages.broker_message_classes import AckMessage, BrokerSubscribeMessage, ContextInfoMessage, \
    SubscribeMessage
from global_data import sub_table, broker_logger, ctx_table, \
    broker_neighbors, this_broker_info
from message_exchange.message_sender import send_message
from subscriptions.subscription_table import Subscription


def process_broker_subscribe_msg(rcv_msg: BrokerSubscribeMessage):
    """
        Checks if this broker has context info matching the subscription request. If so, then handles the subscription
        locally. If not, then starts the subscription search in the broker network.

        BrokerSubscribeMessage messages come from other brokers in the network.

        More info about BrokerSubscribeMessage can be found in broker_messages.broker_message_classes.BrokerSubscribeMessage

        This distributed broker uses random walks with small local flooding as its search method. More info about this method
        can be found in "Peer_to_Peer_Distributed_Context_Broker.pdf"

        Parameters
        ----------
        rcv_msg: BrokerSubscribeMessage
            a dataclass instance representing the received broker_subscribe message
        """

    # are the input parameters valid?
    if rcv_msg is not None and isinstance(rcv_msg, BrokerSubscribeMessage):

        if not check_for_local_context_info(rcv_msg.entity_id, rcv_msg.entity_type, rcv_msg.scope,
                                            rcv_msg.subscriber_address,
                                            rcv_msg.subscriber_port, rcv_msg.duration):

            # marks the broker that sent this broker_subscribe message as online
            if rcv_msg.broker_id != this_broker_info["id"]:
                broker_neighbors.set_entry_online(rcv_msg.broker_id)

            # creates the checked_brokers list for the subscription network search
            # add this broker to the list
            # checked brokers list makes sure no broker is queried more than once during the search
            checked_brokers = [this_broker_info["id"]]
            #reads from the list given in the received broker_subscribe message
            for b_id in rcv_msg.checked_brokers:
                if b_id not in checked_brokers:
                    checked_brokers.append(b_id)

            # receiving this message means one more hop in the random walk was completed
            # so this broker has one LESS hop available
            # there are three possibilities:
            #       received max_hops > 1, which means the random walk will continue further
            #       received max_hops == 1, which means this is the end of a random walk, since it will become 0
            #       received max_hops == 0, for direct request with no forwarding, max_hops will become -1
            max_hops = rcv_msg.max_hops - 1

            # only asks other brokers if you still have hops in the random walk
            if max_hops > 0:
                subscription_search_in_broker_network(max_hops, checked_brokers, rcv_msg.entity_id, rcv_msg.entity_type,
                                                      rcv_msg.scope,
                                                      rcv_msg.subscriber_address, rcv_msg.subscriber_port,
                                                      rcv_msg.duration)


def subscription_search_in_broker_network(max_hops: int, checked_brokers: list, entity_id: str, entity_type: str,
                                          scope: str,
                                          subscriber_address: str,
                                          subscriber_port: int, duration: int):
    """
        Checks if the context info matching (entity_id, entity_type,scope) exists in the context info table.
        If so, creates a Subscription, adds it to the Subscription table, notifies the subscriber the
        subscription was successful and sends the matching context info to the subscriber

        Parameters
        ----------
        max_hops: int
            maximum number of hops allowed for this search. If <= 0, then this broker searches for a match locally but does
            not forward the broker subscribe any further
        checked_brokers : list
            list with the broker ids of all broker already visited by the search. This avoids repeated requests.
        entity_id : str
            unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
        entity_type : str
            specifies the type of entity (for instance, "city" or "person")
        scope : str
            specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")
        subscriber_address : str
            address for the subscriber
        subscriber_port : int
            port for the subscriber
        duration : int
            for how many seconds should this subscription active - should be an integer > 0

    """
    # get a list of neighbors for the random walk and small local flooding
    small_local_flood = broker_neighbors.get_small_local_flooding_list(checked_brokers)
    broker_logger.debug(small_local_flood)

    for neighbor in small_local_flood:
        # add broker in the list to the list of already queried brokers
        checked_brokers.append(neighbor.broker_neighbor_id)

    broker_logger.debug(checked_brokers)

    broker_subscribe_msg = BrokerSubscribeMessage(broker_id=this_broker_info["id"],
                                                  broker_address=this_broker_info["address"],
                                                  broker_port=this_broker_info["port"],
                                                  entity_id=entity_id,
                                                  entity_type=entity_type,
                                                  scope=scope,
                                                  subscriber_address=subscriber_address,
                                                  subscriber_port=subscriber_port,
                                                  duration=duration,
                                                  max_hops=max_hops, checked_brokers=checked_brokers)

    if small_local_flood:
        # the first broker in the small local flood list will be part of the random walk
        # this means it will forward the request if necessary
        send_message(broker_subscribe_msg.as_json(), this_broker_info["address"],
                     (small_local_flood[0].broker_neighbor_address,
                      small_local_flood[0].broker_neighbor_port))
        small_local_flood.pop(0)

    broker_subscribe_msg.max_hops = 0
    msg_string = broker_subscribe_msg.as_json()
    while small_local_flood:
        # other brokers in the small local flood list will only be queried
        # and not forward the request any further
        send_message(msg_string, this_broker_info["address"],
                     (small_local_flood[0].broker_neighbor_address,
                      small_local_flood[0].broker_neighbor_port))
        small_local_flood.pop(0)


def check_for_local_context_info(entity_id: str, entity_type: str, scope: str, subscriber_address: str,
                                 subscriber_port: int, duration: int) -> bool:
    """
        Checks if the context info matching (entity_id, entity_type,scope) exists in the context info table.
        If so, creates a Subscription, adds it to the Subscription table, notifies the subscriber the
        subscription was successful and sends the matching context info to the subscriber

        Parameters
        ----------
        entity_id : str
            unique identifier for the entity the context info relates to (for instance, "Porto" or "John Doe")
        entity_type : str
            specifies the type of entity (for instance, "city" or "person")
        scope : str
            specifies which contextual attribute this update relates to (for instance, "current_temperature" or "body_measurements")
        subscriber_address : str
            address for the subscriber
        subscriber_port : int
            port for the subscriber
        duration : int
            for how many seconds should this subscription active - should be an integer > 0


        Returns
        -------
        bool
            True if context info was found and the subscription can be handled locally, False otherwise
    """

    # check if there is already data in the context info table matching (entity_id, entity_type,scope)
    current_ctx_info = ctx_table.search_entry(entity_id, entity_type, scope)

    if current_ctx_info is not None:
        # create a subscription object
        sub = Subscription(entity_id=entity_id, entity_type=entity_type, scope=scope,
                           subscriber_address=subscriber_address, subscriber_port=subscriber_port,
                           duration=duration)

        # remove any subscription already there, since this will replace it
        # if no subscription exists, then nothing happens
        sub_table.remove_entry(sub.object_id)
        # add new subscription
        sub_table.add_entry(sub)

        broker_logger.debug("sub_table : " + sub_table.print())

        # notify the subscriber that the subscription was successfully done, sending an ACK message
        ack_msg = AckMessage(
            broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
            broker_port=this_broker_info["port"], requested_operation="subscribe",
            requester_address=sub.subscriber_address, requester_port=sub.subscriber_port,
            information=f"Consumer subscribed to [{sub.entity_id},{sub.entity_type},{sub.scope}]"
        )

        send_message(ack_msg.as_json(), this_broker_info["address"],
                     (ack_msg.requester_address, ack_msg.requester_port))

        # send data to the subscriber
        context_info_msg = ContextInfoMessage(broker_id=this_broker_info["id"],
                                              broker_address=this_broker_info["address"],
                                              broker_port=this_broker_info["port"],
                                              entity_id=current_ctx_info.entity_id,
                                              entity_type=current_ctx_info.entity_type,
                                              provider_id=current_ctx_info.provider_id,
                                              provider_address=current_ctx_info.provider_address,
                                              provider_port=current_ctx_info.provider_port,
                                              scope=current_ctx_info.scope,
                                              data=current_ctx_info.data,
                                              validity_end=current_ctx_info.validity_end
                                              )

        send_message(context_info_msg.as_json(), this_broker_info["address"],
                     (sub.subscriber_address, sub.subscriber_port))
        return True

    return False


def process_subscribe_msg(rcv_msg: SubscribeMessage):
    """
        Checks if this broker has context info matching the subscription request. If so, then handles the subscription
        locally. If not, then starts the subscription search in the broker network.

        SubscribeMessage messages come from context consumers.

        More info about SubscribeMessage can be found in broker_messages.broker_message_classes.SubscribeMessage

        Parameters
        ----------
        rcv_msg: SubscribeMessage
            a dataclass instance representing the received subscribe message
        """

    # are the input parameters valid?
    if rcv_msg is not None and isinstance(rcv_msg, SubscribeMessage):

        if not check_for_local_context_info(rcv_msg.entity_id, rcv_msg.entity_type, rcv_msg.scope,
                                            rcv_msg.subscriber_address,
                                            rcv_msg.subscriber_port, rcv_msg.duration):
            subscription_search_in_broker_network(5, [this_broker_info["id"]], rcv_msg.entity_id, rcv_msg.entity_type,
                                                  rcv_msg.scope,
                                                  rcv_msg.subscriber_address, rcv_msg.subscriber_port, rcv_msg.duration)

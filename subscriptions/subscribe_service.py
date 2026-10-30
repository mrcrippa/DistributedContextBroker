from broker_messages.ack_message import AckMessage
from broker_messages.broker_subscribe_message import BrokerSubscribeMessage
from broker_messages.context_response_message import ContextResponseMessage
from broker_messages.subscribe_message import SubscribeMessage
from global_data import sub_table, broker_logger, ctx_table, \
    broker_neighbors, this_broker_info
from message_exchange.message_sender import send_message
from subscriptions.subscription_table import Subscription


def process_broker_subscribe_msg(rcv_msg: BrokerSubscribeMessage):
    sub = check_for_local_subscription(rcv_msg.entity_id, rcv_msg.entity_type, rcv_msg.scope,
                                       rcv_msg.subscriber_address,
                                       rcv_msg.subscriber_port, rcv_msg.duration)

    if sub is None:
        return

    if rcv_msg.broker_id != this_broker_info["id"]:
        broker_neighbors.set_entry_online(rcv_msg.broker_id)

    broker_logger.debug(rcv_msg.broker_id)

    checked_brokers = [this_broker_info["id"]]
    broker_logger.debug(checked_brokers)
    max_hops = 2

    if rcv_msg.max_hops is not None and rcv_msg.checked_brokers is not None:
        # broker_logger.debug(rcv_msg.get_token("CHECKED_BROKERS"))
        # broker_logger.debug(rcv_msg.get_token("MAX_HOPS"))
        for b_id in rcv_msg.checked_brokers:
            if b_id not in checked_brokers:
                checked_brokers.append(b_id)
        broker_logger.debug(checked_brokers)
        max_hops = rcv_msg.max_hops - 1

    if max_hops > 0:
        small_flood = broker_neighbors.get_small_flooding_list(checked_brokers)
        for neighbour in small_flood:
            checked_brokers.append(neighbour.get_broker_neighbour_id())

        broker_logger.debug(checked_brokers)

        broker_subscribe_msg = BrokerSubscribeMessage(broker_id=this_broker_info["id"],
                                                      broker_address=this_broker_info["address"],
                                                      broker_port=this_broker_info["port"],
                                                      entity_id=sub.entity_id,
                                                      entity_type=sub.entity_type,
                                                      scope=sub.scope,
                                                      subscriber_address=sub.subscriber_address,
                                                      subscriber_port=sub.subscriber_port,
                                                      duration=sub.duration,
                                                      max_hops=max_hops, checked_brokers=checked_brokers)

        if small_flood:
            send_message(broker_subscribe_msg.as_json(), this_broker_info["address"],
                         (small_flood[0].get_broker_neighbour_address(),
                          small_flood[0].get_broker_neighbour_port()))
            small_flood.pop(0)

        broker_subscribe_msg.max_hops = 0
        msg_string = broker_subscribe_msg.as_json()
        while small_flood:
            send_message(msg_string, this_broker_info["address"], (small_flood[0].get_broker_neighbour_address(),
                                                                   small_flood[0].get_broker_neighbour_port()))
            small_flood.pop(0)


def check_for_local_subscription(entity_id: str, entity_type: str, scope: str, subscriber_address: str,
                                 subscriber_port: int, duration: int) -> Subscription | None:
    # returns None if the subscription can be handled locally, if not, then return a Subscription object
    # check if there is already data in the context info table matching the subscription

    current_ctx_info = ctx_table.search_entry(entity_id,
                                              entity_type,
                                              scope)

    # create subscription object
    sub = Subscription(entity_id=entity_id, entity_type=entity_type, scope=scope,
                       subscriber_address=subscriber_address, subscriber_port=subscriber_port,
                       duration=duration)

    if current_ctx_info is not None:
        # remove any subscription already there, since this will replace it
        # if no subscription exists, then nothing happens
        sub_table.remove_entry(sub.object_id)
        # add new subscription
        sub_table.add_entry(sub)

        # broker_logger.debug("sub_table : " + sub_table.print())

        # notify the subscriber that the subscription was successfully done, sending an ACK message
        ack_msg = AckMessage(
            broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
            broker_port=this_broker_info["port"], requested_operation="subscribe",
            requester_address=sub.subscriber_address, requester_port=sub.subscriber_port,
            information=f"Consumer subscribed to [{sub.entity_id},{sub.entity_type},{sub.scope}]",
            well_formed=True
        )

        send_message(ack_msg.as_json(), this_broker_info["address"],
                     (ack_msg.requester_address, ack_msg.requester_port))

        # send data to the subscriber
        context_response_msg = ContextResponseMessage(broker_id=this_broker_info["id"],
                                                      broker_address=this_broker_info["address"],
                                                      broker_port=this_broker_info["port"],
                                                      entity_id=current_ctx_info.entity_id,
                                                      entity_type=current_ctx_info.entity_type,
                                                      provider_id=current_ctx_info.provider_id,
                                                      provider_address=current_ctx_info.provider_address,
                                                      provider_port=current_ctx_info.provider_port,
                                                      scope=current_ctx_info.scope,
                                                      data=current_ctx_info.data,
                                                      validity_end=current_ctx_info.validity_end,
                                                      well_formed=True)

        send_message(context_response_msg.as_json(), this_broker_info["address"],
                     (sub.subscriber_address, sub.subscriber_port))
        return None

    return sub


def process_subscribe_msg(rcv_msg: SubscribeMessage):
    """adds a subscription to the subscription table, received message was already verified and it's well-formed"""

    check_for_local_subscription(rcv_msg.entity_id, rcv_msg.entity_type, rcv_msg.scope, rcv_msg.subscriber_address,
                                 rcv_msg.subscriber_port, rcv_msg.duration)

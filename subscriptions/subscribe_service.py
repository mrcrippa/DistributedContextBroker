from broker_messages.broker_message_classes import AckMessage, BrokerSubscribeMessage, ContextInfoMessage, \
    SubscribeMessage
from global_data import sub_table, broker_logger, ctx_table, \
    broker_neighbors, this_broker_info
from message_exchange.message_sender import send_message
from subscriptions.subscription_table import Subscription


def process_broker_subscribe_msg(rcv_msg: BrokerSubscribeMessage):

    if rcv_msg is not None and isinstance(rcv_msg, BrokerSubscribeMessage):
        if not check_for_local_subscription(rcv_msg.entity_id, rcv_msg.entity_type, rcv_msg.scope,
                                            rcv_msg.subscriber_address,
                                            rcv_msg.subscriber_port, rcv_msg.duration):

            if rcv_msg.broker_id != this_broker_info["id"]:
                broker_neighbors.set_entry_online(rcv_msg.broker_id)
            broker_logger.debug("here")
            checked_brokers = [this_broker_info["id"]]
            broker_logger.debug("here")
            for b_id in rcv_msg.checked_brokers:
                if b_id not in checked_brokers:
                    checked_brokers.append(b_id)
            broker_logger.debug("here")
            broker_logger.debug(checked_brokers)
            max_hops = rcv_msg.max_hops - 1
            broker_logger.debug("here")
            if max_hops > 0:
                broker_logger.debug("here")
                subscription_broker_network_search(max_hops, checked_brokers, rcv_msg.entity_id, rcv_msg.entity_type,
                                                   rcv_msg.scope,
                                                   rcv_msg.subscriber_address, rcv_msg.subscriber_port,
                                                   rcv_msg.duration)


def subscription_broker_network_search(max_hops: int, checked_brokers: list, entity_id: str, entity_type: str,
                                       scope: str,
                                       subscriber_address: str,
                                       subscriber_port: int, duration: int):
    small_flood = broker_neighbors.get_small_flooding_list(checked_brokers)
    broker_logger.debug(small_flood)

    for neighbor in small_flood:
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

    if small_flood:
        send_message(broker_subscribe_msg.as_json(), this_broker_info["address"],
                     (small_flood[0].broker_neighbor_address,
                      small_flood[0].broker_neighbor_port))
        small_flood.pop(0)

    broker_subscribe_msg.max_hops = 0
    msg_string = broker_subscribe_msg.as_json()
    while small_flood:
        send_message(msg_string, this_broker_info["address"],
                     (small_flood[0].broker_neighbor_address,
                      small_flood[0].broker_neighbor_port))
        small_flood.pop(0)


def check_for_local_subscription(entity_id: str, entity_type: str, scope: str, subscriber_address: str,
                                 subscriber_port: int, duration: int) -> bool:

    # returns True if the subscription can be handled locally, if not, then return False

    # check if there is already data in the context info table matching the subscription
    current_ctx_info = ctx_table.search_entry(entity_id, entity_type, scope)

    if current_ctx_info is not None:
        # create subscription object
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
    """adds a subscription to the subscription table, received message was already verified and it's well-formed"""

    if rcv_msg is not None and isinstance(rcv_msg, SubscribeMessage):

        if not check_for_local_subscription(rcv_msg.entity_id, rcv_msg.entity_type, rcv_msg.scope,
                                            rcv_msg.subscriber_address,
                                            rcv_msg.subscriber_port, rcv_msg.duration):
            subscription_broker_network_search(5, [this_broker_info["id"]], rcv_msg.entity_id, rcv_msg.entity_type,
                                               rcv_msg.scope,
                                               rcv_msg.subscriber_address, rcv_msg.subscriber_port, rcv_msg.duration)

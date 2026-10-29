import json

from broker_messages.ack_message import AckMessage
from broker_messages.broker_subscribe_message import BrokerSubscribeMessage
from broker_messages.context_response_message import ContextResponseMessage
from broker_messages.subscribe_message import SubscribeMessage
from global_data import sub_table, broker_logger, ctx_table, \
    broker_neighbors, this_broker_info
from message_exchange.message_sender import send_message
from subscriptions.subscription_table import Subscription


def process_broker_subscription_msg(rcv_msg: BrokerSubscribeMessage):
    pass



def process_subscribe_msg(rcv_msg: SubscribeMessage):
    """adds a subscription to the subscription table, received message was already verified and it's well-formed"""

    # check if there is already data in the context info table matching the subscription

    current_ctx_info = ctx_table.search_entry(rcv_msg.get_token("ENTITY_ID"),
                                              rcv_msg.get_token("ENTITY_TYPE"),
                                              rcv_msg.get_token("SCOPE"))

    # create subscription object

    # self._entity_id = rcv_msg.get_token("ENTITY_ID")
    # self._entity_type = rcv_msg.get_token("ENTITY_TYPE")
    # self._scope = rcv_msg.get_token("SCOPE")
    # self._subscriber_address = rcv_msg.get_token("SUBSCRIBER_ADDRESS")
    # self._subscriber_port = rcv_msg.get_token("SUBSCRIBER_PORT")
    # "for how many seconds should this subscription be valid - integer > 0"
    # self._duration = rcv_msg.get_token("DURATION")
    sub = Subscription(rcv_msg)

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
            broker_port=this_broker_info["port"], requested_operation="SUBSCRIBE",
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

    else:

        if rcv_msg.get_token("BROKER_ID") != this_broker_info["id"]:
            broker_neighbors.set_entry_online(rcv_msg.get_token("BROKER_ID"))

        broker_logger.debug(rcv_msg.get_token("BROKER_ID"))

        checked_brokers = [this_broker_info["id"]]
        broker_logger.debug(checked_brokers)
        max_hops = 2

        if rcv_msg.get_token("MAX_HOPS") is not None and rcv_msg.get_token("CHECKED_BROKERS") is not None:
            # broker_logger.debug(rcv_msg.get_token("CHECKED_BROKERS"))
            # broker_logger.debug(rcv_msg.get_token("MAX_HOPS"))
            for b_id in rcv_msg.get_token("CHECKED_BROKERS"):
                if b_id not in checked_brokers:
                    checked_brokers.append(b_id)
            broker_logger.debug(checked_brokers)
            # checked_brokers.append(rcv_msg.get_token("CHECKED_BROKERS"))
            max_hops = rcv_msg.get_token("MAX_HOPS") - 1

        if max_hops > 0:
            small_flood = broker_neighbors.get_small_flooding_list(checked_brokers)
            for neighbour in small_flood:
                checked_brokers.append(neighbour.get_broker_neighbour_id())

            broker_logger.debug(checked_brokers)

            msg_dict = {"TYPE": "BROKER_SUBSCRIBE", "BROKER_ID": this_broker_info["id"],
                        "BROKER_ADDRESS": this_broker_info["address"],
                        "BROKER_PORT": this_broker_info["port"], "ENTITY_ID": sub.get_entity_id(),
                        "ENTITY_TYPE": sub.get_entity_type(),
                        "SCOPE": sub.get_scope(), "SUBSCRIBER_ADDRESS": sub.get_subscriber_address(),
                        "SUBSCRIBER_PORT": sub.get_subscriber_port(),
                        "DURATION": sub.get_duration(), "MAX_HOPS": max_hops,
                        "CHECKED_BROKERS": checked_brokers}

            if small_flood:
                send_message(json.dumps(msg_dict), this_broker_info["address"],
                             (small_flood[0].get_broker_neighbour_address(),
                              small_flood[0].get_broker_neighbour_port()))
                small_flood.pop(0)

            msg_dict["MAX_HOPS"] = 0
            msg_string = json.dumps(msg_dict)
            while small_flood:
                send_message(msg_string, this_broker_info["address"], (small_flood[0].get_broker_neighbour_address(),
                                                                       small_flood[0].get_broker_neighbour_port()))
                small_flood.pop(0)

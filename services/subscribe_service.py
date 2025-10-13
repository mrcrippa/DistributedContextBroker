from global_data import sub_table, broker_logger, ctx_table, \
    broker_neighbors
from tables.subscription_table import Subscription
from utils.message_sender import send_context_response, send_ack, send_broker_subscribe

from utils.parsed_message import ParsedMessage


def process_subscription(rcv_msg: ParsedMessage, this_broker_info: dict):
    """adds a subscription to the subscription table, received message was already verified and it's well-formed"""

    # check if there is already data in the context info table matching the subscription

    current_ctx_info = ctx_table.search_entry(rcv_msg.get_token("ENTITY_ID"),
                                              rcv_msg.get_token("ENTITY_TYPE"),
                                              rcv_msg.get_token("SCOPE"))

    # create subscription object
    sub = Subscription(rcv_msg)

    if current_ctx_info is not None:

        # remove any subscription already there, since this will replace it
        # if no subscription exists, then nothing happens
        sub_table.remove_entry(sub.get_object_id())
        # add new subscription
        sub_table.add_entry(sub)

        broker_logger.debug("sub_table : " + sub_table.print())

        # notify the subscriber that the subscription was successfully done, sending an ACK message
        send_ack(this_broker_info, "SUBSCRIBE",
                 str((sub.get_subscriber_address(), sub.get_subscriber_port())),
                 "Consumer subscribed to [ " + sub.get_entity_id() + " , " + sub.get_entity_type() + " , " + sub.get_scope() + " ]",
                 (sub.get_subscriber_address(), sub.get_subscriber_port()))

        # send data to the subscriber
        send_context_response(this_broker_info, current_ctx_info,
                              (sub.get_subscriber_address(), sub.get_subscriber_port()))

    else:

        checked_brokers = [this_broker_info["id"]]
        max_hops = 2

        if rcv_msg.get_token("MAX_HOPS") is not None and rcv_msg.get_token("CHECKED_BROKERS") is not None:
            checked_brokers.append(rcv_msg.get_token("CHECKED_BROKERS"))
            max_hops = int(rcv_msg.get_token("MAX_HOPS")) - 1

        if max_hops > 0:
            small_flood = broker_neighbors.get_small_flooding_list(checked_brokers)

            if small_flood:
                send_broker_subscribe(this_broker_info,
                                      sub, str(max_hops),
                                      checked_brokers,
                                      (small_flood[0].get_broker_neighbour_address(),
                                       int(small_flood[0].get_broker_neighbour_port())))
                small_flood.pop(0)

            while small_flood:
                send_broker_subscribe(this_broker_info,
                                      sub, "0",
                                      checked_brokers,
                                      (small_flood[0].get_broker_neighbour_address(),
                                       int(small_flood[0].get_broker_neighbour_port())))
                small_flood.pop(0)

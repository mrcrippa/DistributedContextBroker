import json

from global_data import sub_table, broker_logger, ctx_table, \
    broker_neighbors
from tables.subscription_table import Subscription
from utils.message_sender import send_context_response, send_ack, send_message
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

        # broker_logger.debug("sub_table : " + sub_table.print())

        # notify the subscriber that the subscription was successfully done, sending an ACK message
        send_ack(this_broker_info, "SUBSCRIBE",
                 str((sub.get_subscriber_address(), sub.get_subscriber_port())),
                 "Consumer subscribed to [ " + sub.get_entity_id() + " , " + sub.get_entity_type() + " , " + sub.get_scope() + " ]",
                 (sub.get_subscriber_address(), sub.get_subscriber_port()))

        # send data to the subscriber
        send_context_response(this_broker_info, current_ctx_info,
                              (sub.get_subscriber_address(), sub.get_subscriber_port()))

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

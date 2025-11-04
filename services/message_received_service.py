import json

from global_data import *
from services.neighbour_service import process_broker_neighbors_request, process_broker_neighbors_response
from services.subscribe_service import process_subscription
from services.update_service import process_update_message
from utils.message_sender import send_message
from utils.parsed_message import ParsedMessage


def message_received_service_task(received_raw_data: str, sender_address: tuple, this_broker_info: dict):
    """thread used to process received messages"""
    # the creation of the ReceivedMessage object already verifies if the message is well-formed

    parsed_msg = ParsedMessage(received_raw_data)

    if parsed_msg.is_well_formed():

        match parsed_msg.get_token("TYPE"):
            case "PING":
                broker_logger.debug("PING message received from " + str(sender_address))
                dict_as_string = json.dumps(
                    {"TYPE": "PING", "TIMESTAMP": broker_clock.current_broker_time(),
                     "BROKER_ID": this_broker_info["id"],
                     "BROKER_ADDRESS": this_broker_info["address"],
                     "BROKER_PORT": this_broker_info["port"]})
                send_message(dict_as_string, this_broker_info["address"], sender_address)
            case "SUBSCRIBE" | "BROKER_SUBSCRIBE":
                broker_logger.debug("SUBSCRIBE or BROKER_SUBSCRIBE message received from " + str(sender_address))
                broker_logger.debug("raw data : " + str(received_raw_data))
                process_subscription(parsed_msg, this_broker_info)
            case "UPDATE":
                # broker_logger.debug("UPDATE message received from " + str(sender_address))
                process_update_message(parsed_msg, sender_address, this_broker_info)
            case "BROKER_NEIGHBORS_REQUEST":
                # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
                # broker_logger.debug("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_address))
                process_broker_neighbors_request(parsed_msg, this_broker_info)
            case "BROKER_NEIGHBORS_RESPONSE":
                # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
                # broker_logger.debug("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_address))
                process_broker_neighbors_response(parsed_msg, this_broker_info)
    else:
        # if the message is not well-formed, send a NACK message
        broker_logger.debug("received message not well formed from" + str(sender_address) + "  raw data : " + str(received_raw_data))
        dict_as_string = json.dumps(
            {"TYPE": "NACK", "BROKER_ID": this_broker_info["id"],
             "BROKER_ADDRESS": this_broker_info["address"],
             "BROKER_PORT": this_broker_info["port"], "OPERATION": "MESSAGE",
             "REQUESTER": str(sender_address), "REASON": "Received message is not well formed"})

        send_message(dict_as_string, this_broker_info["address"], sender_address)

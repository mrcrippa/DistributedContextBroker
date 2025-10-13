from global_data import *
from services.neighbour_service import process_broker_neighbors_request, process_broker_neighbors_response
from services.subscribe_service import process_subscription
from services.update_service import process_update_message
from utils.message_sender import send_ping, send_nack
from utils.parsed_message import ParsedMessage


def message_received_service_task(received_raw_data: str, sender_address: tuple, this_broker_info: dict):
    """thread used to process received messages"""
    # the creation of the ReceivedMessage object already verifies if the message is well-formed
    parsed_msg = ParsedMessage(received_raw_data)
    if parsed_msg.is_well_formed():

        match parsed_msg.get_token("TYPE"):
            case "PING":
                broker_logger.debug("PING message received from " + str(sender_address))
                send_ping(this_broker_info, sender_address)
            case "SUBSCRIBE":
                broker_logger.debug("SUBSCRIBE message received from " + str(sender_address))
                process_subscription(parsed_msg, this_broker_info)
            case "BROKER_SUBSCRIBE":
                broker_logger.debug("BROKER_SUBSCRIBE message received from " + str(sender_address))
                process_subscription(parsed_msg, this_broker_info)
            case "UPDATE":
                # context provider update the context table with its data
                broker_logger.debug("UPDATE message received from " + str(sender_address))
                process_update_message(parsed_msg, sender_address, this_broker_info)
            case "BROKER_NEIGHBORS_REQUEST":
                # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
                broker_logger.debug("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_address))
                process_broker_neighbors_request(parsed_msg, this_broker_info)
            case "BROKER_NEIGHBORS_RESPONSE":
                # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
                broker_logger.debug("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_address))
                process_broker_neighbors_response(parsed_msg)
    else:
        # if the message is not well-formed, send a NACK message
        broker_logger.debug("received message not well formed from" + str(sender_address))
        send_nack(this_broker_info, "MESSAGE", str(sender_address),
                  "Received message is not well formed", sender_address)

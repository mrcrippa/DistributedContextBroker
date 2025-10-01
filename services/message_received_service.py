from global_data import *
from services.neighbour_service import process_broker_neighbors_request, process_broker_neighbors_response
from services.subscribe_service import process_subscription
from services.update_service import process_update_message
from utils.message_creator import create_ping_message_string, create_nack_message_string
from utils.message_sender import send_message
from utils.parsed_message import ParsedMessage


def message_received_service_task(received_raw_data: str, sender_address: tuple, this_broker_info: dict):
    """thread used to process received messages"""
    # the creation of the ReceivedMessage object already verifies if the message is well-formed
    parsed_msg = ParsedMessage(received_raw_data)
    if parsed_msg.is_well_formed():
        broker_logger.info("well formed message: " + parsed_msg.print_message() + " from " + str(sender_address))

        match parsed_msg.get_token("TYPE"):
            case "PING":
                broker_logger.info("PING message received from " + str(sender_address))
                send_message(create_ping_message_string(this_broker_info), this_broker_info["address"], sender_address)
            case "SUBSCRIBE":
                broker_logger.info("SUBSCRIBE message received from " + str(sender_address))
                process_subscription(parsed_msg, this_broker_info)
            case "BROKER_SUBSCRIBE":
                broker_logger.info("BROKER_SUBSCRIBE message received from " + str(sender_address))
                process_subscription(parsed_msg, this_broker_info)
            case "UPDATE":
                # context provider update the context table with its data
                broker_logger.info("UPDATE message received from " + str(sender_address))
                process_update_message(parsed_msg, sender_address, this_broker_info)
            case "BROKER_NEIGHBORS_REQUEST":
                # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
                broker_logger.info("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_address))
                process_broker_neighbors_request(parsed_msg, this_broker_info)
            case "BROKER_NEIGHBORS_RESPONSE":
                # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
                broker_logger.info("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_address))
                process_broker_neighbors_response(parsed_msg)
    else:
        # if the message is not well-formed, send a NACK message
        broker_logger.info("received message not well formed from" + str(sender_address))
        send_message(create_nack_message_string(this_broker_info, "MESSAGE", str(sender_address),
                                                "Received message is not well formed"), this_broker_info["address"],
                     sender_address)

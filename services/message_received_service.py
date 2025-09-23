from global_data import *
from services.neighbour_service import process_broker_neighbors_request, process_broker_neighbors_response
from services.subscribe_service import process_subscription
from services.update_service import process_update_message
from utils.message_creator import create_ping_message_string, create_nack_message_string
from utils.received_message import ReceivedMessage


def message_received_service_task(received_raw_data, sender_addr, broker_sock, broker_sock_lock, this_broker_info):
    """thread used to process received messages"""
    # the creation of the ReceivedMessage object already verifies if the message is well-formed
    received_msg = ReceivedMessage(received_raw_data)
    if received_msg.is_well_formed():
        logger.info("well formed message: " + received_msg.print_message() + " from " + str(sender_addr))

        if received_msg.get_token("TYPE") == "PING":
            # PING messages get a ping message in return
            # this should be used to check if the broker is alive and the current time at the broker
            logger.info("PING message received from " + str(sender_addr))
            broker_sock_lock.acquire()
            broker_sock.sendto(str.encode(create_ping_message_string(this_broker_info)), sender_addr)
            broker_sock_lock.release()

        elif received_msg.get_token("TYPE") == "SUBSCRIBE" or received_msg.get_token("TYPE") == "BROKER_SUBSCRIBE":
            logger.info("SUBSCRIBE message received from " + str(sender_addr))

            process_subscription(received_msg, sender_addr, this_broker_info, broker_sock, broker_sock_lock)

        elif received_msg.get_token("TYPE") == "UPDATE":
            # context provider update the context table with its data
            logger.info("UPDATE message received from " + str(sender_addr))

            process_update_message(received_msg, sender_addr, this_broker_info, broker_sock, broker_sock_lock)

        elif received_msg.get_token("TYPE") == "BROKER_NEIGHBORS_REQUEST":
            # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
            logger.info("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_addr))

            process_broker_neighbors_request(received_msg, this_broker_info, broker_sock, broker_sock_lock)

        elif received_msg.get_token("TYPE") == "BROKER_NEIGHBORS_RESPONSE":
            # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
            logger.info("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_addr))

            process_broker_neighbors_response(received_msg)

    else:
        # if the message is not well-formed, send a NACK message
        logger.info("received message not well formed from" + str(sender_addr))
        broker_sock_lock.acquire()
        broker_sock.sendto(
            str.encode(create_nack_message_string(this_broker_info, "MESSAGE", str(sender_addr),
                                                  "Received message is not well formed")),
            sender_addr)
        broker_sock_lock.release()

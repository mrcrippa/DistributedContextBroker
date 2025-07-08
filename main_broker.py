import queue
import socket
import sys
import threading
from logging.handlers import QueueHandler, QueueListener
from threading import Thread

from global_data import *
from services.subscribe_service import add_subscription
from services.update_service import process_update
from utils.received_message import *
from utils.message_creator import *


def service_task(received_raw_data, sender_addr, broker_sock, broker_lock):
    """thread used to process received messages"""
    # by create the ReceivedMessage object, you already check if the message is well-formed
    received_msg = ReceivedMessage(received_raw_data)
    if received_msg.is_well_formed():
        logger.info("well formed message: " + received_msg.print_message() + " from " + str(sender_addr))

        if received_msg.get_message_token("TYPE") == "PING":
            # PING messages get a ping message in return
            # this should be used to check if the broker is alive and the current time at the broker
            logger.info("PING message received from " + str(sender_addr))
            broker_lock.acquire()
            broker_sock.sendto(str.encode(create_ping_message_string()), sender_addr)
            broker_lock.release()

        elif received_msg.get_message_token("TYPE") == "SUBSCRIBE":
            # someone wants to subscribe to data
            logger.info("SUBSCRIBE message received from " + str(sender_addr))

            # add subscription to the table
            sub = add_subscription(received_msg)

            logger.info("sub_table : " + sub_table.print())
            logger.info("sub_timer_threads : " + str(sub_timer_threads))

            # notify the subscriber that the subscription was successfully done, sending an ACK message
            broker_lock.acquire()
            broker_sock.sendto(str.encode(create_ack_message_string("SUBSCRIBE", str(sender_addr),
                                                                    "Consumer subscribed to [ " + sub.get_entity_id() + " , " + sub.get_entity_type() + " , " + sub.get_scope() + " ]")),
                               (sub.get_subscriber_address(), sub.get_subscriber_port()))

            # check if there is already data in the context info table matching the subscription
            ctx_table_lock.acquire()
            current_ctx_info = ctx_table.search_entry(sub.get_entity_id(), sub.get_entity_type(), sub.get_scope())
            ctx_table_lock.release()

            # if there is data, then send it to the subscriber
            if current_ctx_info is not None:
                broker_sock.sendto(str.encode(create_request_response_message_string(current_ctx_info)),
                                   (sub.get_subscriber_address(), sub.get_subscriber_port()))
            broker_lock.release()

        elif received_msg.get_message_token("TYPE") == "UPDATE":
            # context provider update the context table with its data
            logger.info("UPDATE message received from " + str(sender_addr))

            # add the data to the table
            ctx_data = process_update(received_msg)

            logger.info("ctx_table : " + ctx_table.print())
            logger.info("ctx_timer_threads : " + str(ctx_timer_threads))

            # send an ACK message confirming the update
            broker_lock.acquire()
            broker_sock.sendto(str.encode(create_ack_message_string("UPDATE", str(sender_addr),
                                                                    "Update to [ " + ctx_data.get_entity_id() + " , " + ctx_data.get_entity_type() + " , " + ctx_data.get_scope() + " ]")),
                               sender_addr)

            # if there are any subscriptions to this data, then notify all subscribers
            for sub in sub_table.search_entry(ctx_data.get_entity_id(), ctx_data.get_entity_type(),
                                              ctx_data.get_scope()):
                broker_sock.sendto(str.encode(create_request_response_message_string(ctx_data)),
                                   (sub.get_subscriber_address(), sub.get_subscriber_port()))
            broker_lock.release()

        elif received_msg.get_message_token("TYPE") == "REQUEST":
            # a request for context data
            logger.info("REQUEST message received from " + str(sender_addr))

            ctx_table_lock.acquire()
            current_ctx_info = ctx_table.search_entry(received_msg.get_message_token("ENTITY_ID"),
                                                      received_msg.get_message_token("ENTITY_TYPE"),
                                                      received_msg.get_message_token("SCOPE"))
            ctx_table_lock.release()

            broker_lock.acquire()
            if current_ctx_info is not None:
                # if any matching data found, then send it to the requester
                broker_sock.sendto(str.encode(create_request_response_message_string(current_ctx_info)), sender_addr)
            else:
                # if no data found, notify the requester
                broker_sock.sendto(str.encode(create_nack_message_string("REQUEST", str(sender_addr),
                                                                         "No context data found with the given parameters")),
                                   sender_addr)
            broker_lock.release()

    else:
        # if the message is not well-formed, send a NACK message
        logger.info("received message not well formed from" + str(sender_addr))
        broker_lock.acquire()
        broker_sock.sendto(
            str.encode(create_nack_message_string("MESSAGE", str(sender_addr), "Received message is not well formed")),
            sender_addr)
        broker_lock.release()


def main():
    # set up logging environment
    log_queue = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file_name)
    file_handler.setFormatter(formatter)

    listener = QueueListener(log_queue, console_handler, file_handler)
    listener.start()

    # create a new socket using UDP
    broker_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        broker_sock.bind((host_broker_ip, port_broker_nr))
    except socket.error as error_msg:
        logger.critical("Binding Broker socket failed. Message : " + str(error_msg))
        sys.exit()

    # lock used to coordinate among various threads, so only one can access the socket at a time
    broker_lock = threading.Lock()

    while True:
        # main loop
        # wait for messages, when one is received, create a service_task thread
        print("Waiting for messages...")
        received_raw_data, sender_addr = broker_sock.recvfrom(1024)
        logger.info("received message from " + str(sender_addr))

        Thread(target=service_task, args=[received_raw_data, sender_addr, broker_sock, broker_lock]).start()


main()

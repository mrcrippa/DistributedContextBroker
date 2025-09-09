import configparser
import queue
import random
import socket
import sys
import threading
import uuid
from logging.handlers import QueueHandler, QueueListener
from threading import Thread

from global_data import *
from services.subscribe_service import add_subscription
from services.update_service import process_update
from tables.broker_neighbor_info import BrokerNeighborInfo
from utils.received_message import *
from utils.message_creator import *


def broker_ping(broker_sock, broker_sock_lock, this_broker_id, this_broker_address, this_broker_port):
    # checking if any of your neighbours is offline, by requesting their list of neighbours
    logger.info("broker ping")

    msg = create_broker_neighbors_request_message_string(this_broker_id, this_broker_address, this_broker_port)

    broker_neighbors_lock.acquire()
    for entry in broker_neighbors.get_table():
        broker_sock_lock.acquire()
        broker_sock.sendto(str.encode(msg), (entry.get_broker_address(), entry.get_broker_port()))
        broker_sock_lock.release()
    broker_neighbors_lock.release()

    threading.Timer(30, broker_ping,
                    [broker_sock, broker_sock_lock, this_broker_id, this_broker_address, this_broker_port]).start()


def service_task(received_raw_data, sender_addr, broker_sock, broker_sock_lock, this_broker_id, this_broker_address,
                 this_broker_port):
    """thread used to process received messages"""
    # the creation of the ReceivedMessage object already verifies if the message is well-formed
    received_msg = ReceivedMessage(received_raw_data)
    if received_msg.is_well_formed():
        logger.info("well formed message: " + received_msg.print_message() + " from " + str(sender_addr))

        if received_msg.get_message_token("TYPE") == "PING":
            # PING messages get a ping message in return
            # this should be used to check if the broker is alive and the current time at the broker
            logger.info("PING message received from " + str(sender_addr))
            broker_sock_lock.acquire()
            broker_sock.sendto(str.encode(create_ping_message_string(this_broker_id, this_broker_address,
                                                                     this_broker_port)), sender_addr)
            broker_sock_lock.release()

        elif received_msg.get_message_token("TYPE") == "SUBSCRIBE":
            logger.info("SUBSCRIBE message received from " + str(sender_addr))

            sub = add_subscription(received_msg)

            logger.info("sub_table : " + sub_table.print())
            logger.info("sub_timer_threads : " + str(sub_timer_threads))

            # notify the subscriber that the subscription was successfully done, sending an ACK message
            broker_sock_lock.acquire()
            broker_sock.sendto(str.encode(create_ack_message_string(this_broker_id, this_broker_address,
                                                                    this_broker_port, "SUBSCRIBE", str(sender_addr),
                                                                    "Consumer subscribed to [ " + sub.get_entity_id() + " , " + sub.get_entity_type() + " , " + sub.get_scope() + " ]")),
                               (sub.get_subscriber_address(), sub.get_subscriber_port()))

            # check if there is already data in the context info table matching the subscription
            ctx_table_lock.acquire()
            current_ctx_info = ctx_table.search_entry(sub.get_entity_id(), sub.get_entity_type(), sub.get_scope())
            ctx_table_lock.release()

            # if there is data, then send it to the subscriber
            if current_ctx_info is not None:
                broker_sock.sendto(str.encode(create_context_response_message_string(current_ctx_info)),
                                   (sub.get_subscriber_address(), sub.get_subscriber_port()))
            broker_sock_lock.release()

        elif received_msg.get_message_token("TYPE") == "UPDATE":
            # context provider update the context table with its data
            logger.info("UPDATE message received from " + str(sender_addr))

            ctx_data = process_update(received_msg)

            logger.info("ctx_table : " + ctx_table.print())
            logger.info("ctx_timer_threads : " + str(ctx_timer_threads))

            # send an ACK message confirming the update
            broker_sock_lock.acquire()
            broker_sock.sendto(str.encode(create_ack_message_string(this_broker_id, this_broker_address,
                                                                    this_broker_port, "UPDATE", str(sender_addr),
                                                                    "Update to [ " + ctx_data.get_entity_id() + " , " + ctx_data.get_entity_type() + " , " + ctx_data.get_scope() + " ]")),
                               sender_addr)

            # if there are any subscriptions to this data, then notify all subscribers
            for sub in sub_table.search_entry(ctx_data.get_entity_id(), ctx_data.get_entity_type(),
                                              ctx_data.get_scope()):
                broker_sock.sendto(str.encode(create_context_response_message_string(ctx_data)),
                                   (sub.get_subscriber_address(), sub.get_subscriber_port()))
            broker_sock_lock.release()

        elif received_msg.get_message_token("TYPE") == "CONTEXT_REQUEST":
            # a request for context data
            logger.info("CONTEXT_REQUEST message received from " + str(sender_addr))

            ctx_table_lock.acquire()
            current_ctx_info = ctx_table.search_entry(received_msg.get_message_token("ENTITY_ID"),
                                                      received_msg.get_message_token("ENTITY_TYPE"),
                                                      received_msg.get_message_token("SCOPE"))
            ctx_table_lock.release()

            broker_sock_lock.acquire()
            if current_ctx_info is not None:
                # if any matching data found, then send it to the requester
                broker_sock.sendto(
                    str.encode(create_context_response_message_string(this_broker_id, this_broker_address,
                                                                      this_broker_port, current_ctx_info)), sender_addr)
            else:
                # if no data found, notify the requester
                broker_sock.sendto(str.encode(create_nack_message_string(this_broker_id, this_broker_address,
                                                                         this_broker_port, "CONTEXT_REQUEST",
                                                                         str(sender_addr),
                                                                         "No context data found with the given parameters")),
                                   sender_addr)
            broker_sock_lock.release()
        elif received_msg.get_message_token("TYPE") == "BROKER_NEIGHBORS_REQUEST":
            # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
            logger.info("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_addr))
            broker_neighbors_lock.acquire()

            # information about the requester broker
            requester_broker_id = received_msg.get_message_token("BROKER_ID")
            requester_broker_address = received_msg.get_message_token("BROKER_ADDRESS")
            requester_broker_port = received_msg.get_message_token("BROKER_PORT")

            broker_sock_lock.acquire()
            broker_sock.sendto(
                str.encode(create_broker_neighbors_response_message_string(this_broker_id, this_broker_address,
                                                                           this_broker_port)),
                (requester_broker_address, int(requester_broker_port)))
            broker_sock_lock.release()

            if broker_neighbors.search_entry(requester_broker_id) is None:
                neighbor = BrokerNeighborInfo()
                neighbor.set_broker_id(requester_broker_id)
                neighbor.set_broker_address(requester_broker_address)
                neighbor.set_broker_port(requester_broker_port)
                broker_neighbors.add_entry(neighbor)
            broker_neighbors_lock.release()

        elif received_msg.get_message_token("TYPE") == "BROKER_NEIGHBORS_RESPONSE":
            # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
            logger.info("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_addr))
            broker_neighbors_lock.acquire()

            # information about the responder broker
            responder_broker_id = received_msg.get_message_token("BROKER_ID")
            responder_broker_address = received_msg.get_message_token("BROKER_ADDRESS")
            responder_broker_port = received_msg.get_message_token("BROKER_PORT")
            responder_neighbors_list = received_msg.get_message_token("BROKER_NEIGHBORS")

            # add responder broker if not already in the broker neighbour table
            if broker_neighbors.search_entry(responder_broker_id) is None:
                neighbor = BrokerNeighborInfo()
                neighbor.set_broker_id(responder_broker_id)
                neighbor.set_broker_address(responder_broker_address)
                neighbor.set_broker_port(responder_broker_port)
                broker_neighbors.add_entry(neighbor)

            # insert neighbours in broker neighbours table
            received_neighbour_list = json.loads(responder_neighbors_list)
            for entry in received_neighbour_list:
                if broker_neighbors.search_entry(entry.get("BROKER_ID")) is None:
                    neighbor = BrokerNeighborInfo()
                    neighbor.set_broker_id(entry.get("BROKER_ID"))
                    neighbor.set_broker_address(entry.get("BROKER_ADDRESS"))
                    neighbor.set_broker_port(entry.get("BROKER_PORT"))
                    broker_neighbors.add_entry(neighbor)

            broker_neighbors_lock.release()

    else:
        # if the message is not well-formed, send a NACK message
        logger.info("received message not well formed from" + str(sender_addr))
        broker_sock_lock.acquire()
        broker_sock.sendto(
            str.encode(create_nack_message_string(this_broker_id, this_broker_address,
                                                  this_broker_port, "MESSAGE", str(sender_addr),
                                                  "Received message is not well formed")),
            sender_addr)
        broker_sock_lock.release()


def main():
    # reads the config file
    config = configparser.ConfigParser()
    config.read('broker_config_file')

    if config['config_values']['broker_id'] == '0':
        this_broker_id = str(uuid.uuid4()).replace('-', '')[:8]
    else:
        this_broker_id = config['config_values']['broker_id']

    # set up logging environment
    log_queue = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(config['config_values']['log_file_name'] + "_" + this_broker_id + ".log")
    file_handler.setFormatter(formatter)

    listener = QueueListener(log_queue, console_handler, file_handler)
    listener.start()

    # create a new socket using UDP

    broker_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_connected = False

    port_broker_nr = int(config['config_values']['port_broker_nr'])
    while not socket_connected:
        if config['config_values']['port_broker_nr'] == "0":
            port_broker_nr = random.randint(50000, 60000)
        try:
            broker_sock.bind((config['config_values']['host_broker_ip'], port_broker_nr))
        except socket.error as error_msg:
            logger.critical("Binding Broker socket failed. Message : " + str(error_msg))
            if config['config_values']['port_broker_nr'] == "0":
                continue
            sys.exit()
        socket_connected = True

    this_broker_address = config['config_values']['host_broker_ip']
    this_broker_port = str(port_broker_nr)

    # lock used to coordinate among various threads, so only one can access the socket at a time
    broker_sock_lock = threading.Lock()

    # start the broker ping regular timer
    threading.Timer(30, broker_ping,
                    [broker_sock, broker_sock_lock, this_broker_id, this_broker_address, this_broker_port]).start()

    while True:
        # main loop
        # wait for messages, when one is received, create a service_task thread
        print("Waiting for messages...")
        received_raw_data, sender_addr = broker_sock.recvfrom(1024)
        logger.info("received message from " + str(sender_addr))

        Thread(target=service_task,
               args=[received_raw_data, sender_addr, broker_sock, broker_sock_lock, this_broker_id, this_broker_address,
                     this_broker_port]).start()


main()

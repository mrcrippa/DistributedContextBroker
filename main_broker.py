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
from services.neighbour_service import neighbour_broker_live_check, process_broker_neighbors_request, \
    process_broker_neighbors_response
from services.subscribe_service import process_subscription
from services.update_service import process_update_message
from tables.broker_neighbor_info import BrokerNeighborInfo
from utils.received_message import *
from utils.message_creator import *


def message_received_service_task(received_raw_data, sender_addr, broker_sock, broker_sock_lock, this_broker_id,
                                  this_broker_address,
                                  this_broker_port):
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
            broker_sock.sendto(str.encode(create_ping_message_string(this_broker_id, this_broker_address,
                                                                     this_broker_port)), sender_addr)
            broker_sock_lock.release()

        elif received_msg.get_token("TYPE") == "SUBSCRIBE":
            logger.info("SUBSCRIBE message received from " + str(sender_addr))

            process_subscription(received_msg, sender_addr, this_broker_id, this_broker_address,
                                   this_broker_port, broker_sock, broker_sock_lock, False)

        elif received_msg.get_token("TYPE") == "BROKER_SUBSCRIBE":
            logger.info("BROKER_SUBSCRIBE message received from " + str(sender_addr))

            process_subscription(received_msg, sender_addr, this_broker_id, this_broker_address,
                                 this_broker_port, broker_sock, broker_sock_lock, True)

        elif received_msg.get_token("TYPE") == "UPDATE":
            # context provider update the context table with its data
            logger.info("UPDATE message received from " + str(sender_addr))

            process_update_message(received_msg, sender_addr, this_broker_id, this_broker_address,
                                   this_broker_port, broker_sock, broker_sock_lock)

        elif received_msg.get_token("TYPE") == "BROKER_NEIGHBORS_REQUEST":
            # BROKER_NEIGHBORS_REQUEST messages get the list of broker neighbors in return
            logger.info("BROKER_NEIGHBORS_REQUEST message received from " + str(sender_addr))

            process_broker_neighbors_request(received_msg, this_broker_id, this_broker_address,
                                             this_broker_port, broker_sock, broker_sock_lock)

        elif received_msg.get_token("TYPE") == "BROKER_NEIGHBORS_RESPONSE":
            # BROKER_NEIGHBORS_RESPONSE has a list of broker neighbors from another broker
            logger.info("BROKER_NEIGHBORS_RESPONSE message received from " + str(sender_addr))

            process_broker_neighbors_response(received_msg)

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

    # get list of start neighbours
    print(config['config_values']['start_neighbours'])
    start_neighbours = json.loads(config['config_values']['start_neighbours'])

    broker_neighbors_lock.acquire()

    for index in range(0, len(start_neighbours), 3):
        neighbor = BrokerNeighborInfo()
        neighbor.set_broker_neighbour_id(start_neighbours[index])
        neighbor.set_broker_neighbour_address(start_neighbours[index + 1])
        neighbor.set_broker_neighbour_port(start_neighbours[index + 2])
        neighbor.set_broker_neighbour_online(True)
        broker_neighbors.add_entry(neighbor)

    broker_neighbors_lock.release()

    # start the broker ping regular timer
    threading.Timer(1, neighbour_broker_live_check,
                    [broker_sock, broker_sock_lock, this_broker_id, this_broker_address, this_broker_port]).start()

    while True:
        # main loop
        # wait for messages, when one is received, create a service_task thread
        print(this_broker_id + " - Waiting for messages...")
        received_raw_data, sender_addr = broker_sock.recvfrom(1024)
        logger.info("received message from " + str(sender_addr))

        Thread(target=message_received_service_task,
               args=[received_raw_data, sender_addr, broker_sock, broker_sock_lock, this_broker_id, this_broker_address,
                     this_broker_port]).start()


main()

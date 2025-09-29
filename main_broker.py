import configparser
import queue
import random
import socket
import sys
import uuid
from logging.handlers import QueueHandler, QueueListener
from threading import Thread

from global_data import *
from services.message_received_service import message_received_service_task
from services.neighbour_service import neighbour_broker_live_check
from tables.broker_neighbor_table import BrokerNeighborInfo
from utils.message_creator import *
from utils.received_message import *
from utils.broker_clock import *


def main():
    # reads the config file
    config = configparser.ConfigParser()
    config.read('broker_config_file')

    this_broker_info = {}

    if config['config_values']['broker_id'] == '0':
        this_broker_info["id"] = str(uuid.uuid4()).replace('-', '')[:8]
    else:
        this_broker_info["id"] = config['config_values']['broker_id']

    # set up logging environment
    log_queue = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    broker_logger.addHandler(queue_handler)
    broker_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(config['config_values']['log_file_name'] + "_" + this_broker_info["id"] + ".log")
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
            broker_logger.critical("Binding Broker socket failed. Message : " + str(error_msg))
            if config['config_values']['port_broker_nr'] == "0":
                continue
            sys.exit()
        socket_connected = True

    this_broker_info["address"] = config['config_values']['host_broker_ip']
    this_broker_info["port"] = str(port_broker_nr)

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

    # start the neighbour live check regular timer
    threading.Timer(1, neighbour_broker_live_check,
                    [broker_sock, broker_sock_lock, this_broker_info]).start()

    # start the time correction with ntp regular timer
    broker_clock.time_correction_with_ntp()

    while True:
        # main loop
        # wait for messages, when one is received, create a service_task thread
        # print(this_broker_info["id"] + " - Waiting for messages...")
        received_raw_data, sender_address = broker_sock.recvfrom(1024)
        broker_logger.info("received message from " + str(sender_address))

        Thread(target=message_received_service_task,
               args=[received_raw_data, sender_address, broker_sock, broker_sock_lock, this_broker_info]).start()


main()

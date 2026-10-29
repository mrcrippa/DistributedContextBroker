import configparser
import json
import logging
import queue
import sys
import threading
import uuid
from logging.handlers import QueueHandler, QueueListener
from threading import Thread

from global_data import broker_neighbors, broker_clock, this_broker_info
from message_exchange.message_received import message_received
from message_exchange.message_sender import *
from neighbor_management.broker_neighbor_table import BrokerNeighborInfo
from neighbor_management.neighbour_service import neighbour_broker_live_check


def main():
    args = sys.argv[1:]
    if len(args) != 2 or args[0] != '-cfg_file':
        sys.exit('Usage: %s -cfg_file config_file_location' % sys.argv[0])

    # reads the config file
    config = configparser.ConfigParser()
    config.read(args[1])

    if config['config_values']['broker_id'] == '0':
        this_broker_info["id"] = str(uuid.uuid4()).replace('-', '')[:8]
    else:
        this_broker_info["id"] = config['config_values']['broker_id']

    # set up logging environment
    log_queue = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    broker_logger.addHandler(queue_handler)
    broker_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s')
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
    this_broker_info["port"] = port_broker_nr

    # get list of start neighbors
    # print(config['config_values']['start_neighbors'])
    start_neighbours = json.loads(config['config_values']['start_neighbors'])

    for index in range(0, len(start_neighbours), 3):
        neighbor = BrokerNeighborInfo(broker_neighbor_id=start_neighbours[index],
                                      broker_neighbor_address=start_neighbours[index + 1],
                                      broker_neighbor_port=start_neighbours[index + 2],
                                      broker_neighbor_last_online_timestamp=broker_clock.current_broker_time())
        broker_neighbors.add_entry(neighbor)

    # start the neighbor live check regular timer
    threading.Timer(1, neighbour_broker_live_check).start()

    # start the time correction with ntp regular timer
    broker_clock.time_correction_with_ntp()

    while True:
        received_raw_data, sender_address = broker_sock.recvfrom(1024)
        Thread(target=message_received,
               args=[received_raw_data, sender_address]).start()


if __name__ == "__main__":
    main()

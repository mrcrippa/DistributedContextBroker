import socket
import sys
import logging
from datetime import datetime

from global_data import *
from tables.context_info import ContextInfo
from tables.context_info_table import ContextInfoTable
from tables.subscription import Subscription
from utils.message import *
from utils.message_creator import *
from tables import *

def main():

    logging.basicConfig(filename=log_file_name,level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # TODO: setup data structures and semaphores

    # create a new socket using UDP
    broker_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        broker_sock.bind((host_broker_ip, port_broker_nr))
    except socket.error as error_msg:
       logging.critical("Binding Broker socket failed. Message : " + str(error_msg))
       sys.exit()

    while True:
        print("Waiting for messages...")
        received_raw_data, sender_addr = broker_sock.recvfrom(1024)
        logging.info("received message from " + str(sender_addr))

        # TODO: check if message is properly constructed
        received_msg = Message(received_raw_data)
        if received_msg.is_well_formed():
            logging.info("well formed message: " + received_msg.print_message() + " from " + str(sender_addr))

            if received_msg.get_message_token("TYPE") == "PING":
                logging.info("PING message received from " + str(sender_addr))
                broker_sock.sendto(str.encode(create_ping_message_string()), sender_addr)

        else:
            # if not, send NACK
            response = create_nack_message_string("MESSAGE", str(sender_addr), "Received message is not well formed")
            logging.info("received message not well formed from" + str(sender_addr))
            logging.info("sending response: " + response + " to " + str(sender_addr))
            broker_sock.sendto(str.encode(response), sender_addr)


main()

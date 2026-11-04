"""
This module contains a function that sends a message using UDP
The port used is chosen randomly

Functions:
--------
send_message
"""

import random
import socket

from global_data import broker_logger

def send_message(message: str, broker_address: str, receiver_address: tuple):
    """
    Sends message to sender_address using UDP

    Parameters
    ----------
    message: str
        the message to be sent
    broker_address : str
        the address of the broker, port will be chosen randomly
    receiver_address : tuple
        (address, port) of the receiver
    """

    new_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_connected = False

    while not socket_connected:
        port_broker_nr = random.randint(50000, 60000)
        try:
            new_sock.bind((broker_address, port_broker_nr))
        except socket.error:
            continue
        socket_connected = True
    try:
        new_sock.sendto(str.encode(message), receiver_address)
    except socket.error as e:
        broker_logger.error(e)

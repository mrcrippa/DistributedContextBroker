import random
import socket

from global_data import broker_logger

def send_message(message: str, broker_address: str, sender_address: tuple):
    """sends message to sender_address using UDP"""
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
        new_sock.sendto(str.encode(message), sender_address)
    except socket.error as e:
        broker_logger.error(e)

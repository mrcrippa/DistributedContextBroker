import json
import random
import socket

from global_data import broker_logger, this_broker_info
from context_management.context_info_table import ContextInfo


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


def send_context_response(ctx_info: ContextInfo, sender_address: tuple):
    """sends a CONTEXT_RESPONSE message - JSON"""
    dict_as_string = json.dumps(
        {"TYPE": "CONTEXT_RESPONSE", "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"], "PROVIDER_ID": ctx_info.get_provider_id(),
         "ENTITY_ID": ctx_info.get_entity_id(),
         "ENTITY_TYPE": ctx_info.get_entity_type(), "SCOPE": ctx_info.get_scope(),
         "VALIDITY_END": ctx_info.get_validity_end(), "DATA": ctx_info.get_data()})

    send_message(dict_as_string, this_broker_info["address"], sender_address)

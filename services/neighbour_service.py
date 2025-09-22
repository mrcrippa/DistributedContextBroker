import json
import socket
import threading

from global_data import *
from tables.broker_neighbor_table import BrokerNeighborInfo
from utils.message_creator import create_broker_neighbors_request_message_string, \
    create_broker_neighbors_response_message_string
from utils.received_message import ReceivedMessage


def neighbour_broker_live_check(broker_sock: socket.socket, broker_sock_lock: threading.Lock, this_broker_info: dict):
    # checking if any of your neighbours is offline, by requesting their list of neighbours
    logger.info("broker ping")

    msg = create_broker_neighbors_request_message_string(this_broker_info)

    broker_neighbors_lock.acquire()
    for entry in broker_neighbors.get_table():
        if entry.is_broker_neighbour_potentially_offline():
            broker_sock_lock.acquire()
            broker_sock.sendto(str.encode(msg),
                               (entry.get_broker_neighbour_address(), entry.get_broker_neighbour_port()))
            broker_sock_lock.release()
    broker_neighbors_lock.release()

    threading.Timer(15, neighbour_broker_live_check,
                    [broker_sock, broker_sock_lock, this_broker_info]).start()


def process_broker_neighbors_response(rcv_msg: ReceivedMessage):
    # information about the responder broker
    responder_broker_id = rcv_msg.get_token("BROKER_ID")
    responder_broker_address = rcv_msg.get_token("BROKER_ADDRESS")
    responder_broker_port = rcv_msg.get_token("BROKER_PORT")
    responder_neighbors_list = rcv_msg.get_token("BROKER_NEIGHBORS")

    # add responder broker if not already in the broker neighbour table
    broker_neighbors_lock.acquire()
    searched_neighbour = broker_neighbors.search_entry(responder_broker_id)

    if searched_neighbour is None:
        neighbor = BrokerNeighborInfo()
        neighbor.set_broker_neighbour_id(responder_broker_id)
        neighbor.set_broker_neighbour_address(responder_broker_address)
        neighbor.set_broker_neighbour_port(responder_broker_port)
        neighbor.set_broker_neighbour_online(True)
        broker_neighbors.add_entry(neighbor)
    else:
        searched_neighbour.set_broker_neighbour_online(True)

    # insert neighbours in broker neighbours table
    received_neighbour_list = json.loads(responder_neighbors_list)
    for entry in received_neighbour_list:
        s_b_n = broker_neighbors.search_entry(entry.get("BROKER_ID"))
        if s_b_n is None:
            neighbor = BrokerNeighborInfo()
            neighbor.set_broker_neighbour_id(entry.get("BROKER_ID"))
            neighbor.set_broker_neighbour_address(entry.get("BROKER_ADDRESS"))
            neighbor.set_broker_neighbour_port(entry.get("BROKER_PORT"))
            neighbor.set_broker_neighbour_online(True)
            broker_neighbors.add_entry(neighbor)
        else:
            s_b_n.set_broker_neighbour_online(True)

    broker_neighbors_lock.release()


def process_broker_neighbors_request(rcv_msg: ReceivedMessage, this_broker_info: dict, broker_sock: socket.socket,
                                     broker_sock_lock: threading.Lock):
    # information about the requester broker
    requester_broker_id = rcv_msg.get_token("BROKER_ID")
    requester_broker_address = rcv_msg.get_token("BROKER_ADDRESS")
    requester_broker_port = rcv_msg.get_token("BROKER_PORT")

    broker_sock_lock.acquire()
    broker_sock.sendto(
        str.encode(create_broker_neighbors_response_message_string(this_broker_info)),
        (requester_broker_address, int(requester_broker_port)))
    broker_sock_lock.release()

    broker_neighbors_lock.acquire()
    searched_neighbour = broker_neighbors.search_entry(requester_broker_id)

    if searched_neighbour is None:
        neighbor = BrokerNeighborInfo()
        neighbor.set_broker_neighbour_id(requester_broker_id)
        neighbor.set_broker_neighbour_address(requester_broker_address)
        neighbor.set_broker_neighbour_port(requester_broker_port)
        neighbor.set_broker_neighbour_online(True)
        broker_neighbors.add_entry(neighbor)
    else:
        searched_neighbour.set_broker_neighbour_online(True)

    broker_neighbors_lock.release()

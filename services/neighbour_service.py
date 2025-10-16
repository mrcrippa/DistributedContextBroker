import json
import threading

from global_data import *
from tables.broker_neighbor_table import BrokerNeighborInfo
from utils.message_sender import send_message
from utils.parsed_message import ParsedMessage


def neighbour_broker_live_check(this_broker_info: dict):

    # checking if any of your neighbours is offline, by requesting their list of neighbours
    broker_neighbors.check_table_for_potentially_offline_brokers(this_broker_info)

    threading.Timer(15, neighbour_broker_live_check,
                    [this_broker_info]).start()


def process_broker_neighbors_response(rcv_msg: ParsedMessage, this_broker_info: dict):

    # add responder broker if not already in the broker neighbour table
    searched_neighbour = broker_neighbors.search_entry(rcv_msg.get_token("BROKER_ID"))

    if searched_neighbour is None:
        neighbor = BrokerNeighborInfo(rcv_msg.get_token("BROKER_ID"), rcv_msg.get_token("BROKER_ADDRESS"),
                                      rcv_msg.get_token("BROKER_PORT"))
        neighbor.set_broker_neighbour_online(True)
        broker_neighbors.add_entry(neighbor)
    else:
        searched_neighbour.set_broker_neighbour_online(True)

    # insert neighbours in broker neighbours table
    for entry in rcv_msg.get_token("BROKER_NEIGHBORS"):
        if entry.get("BROKER_ID") == this_broker_info.get("id"):
            continue
        s_b_n = broker_neighbors.search_entry(entry.get("BROKER_ID"))
        if s_b_n is None:
            neighbor = BrokerNeighborInfo(entry.get("BROKER_ID"), entry.get("BROKER_ADDRESS"), entry.get("BROKER_PORT"))
            neighbor.set_broker_neighbour_online(True)
            broker_neighbors.add_entry(neighbor)
        else:
            broker_neighbors.set_entry_online(entry.get("BROKER_ID"))


def process_broker_neighbors_request(rcv_msg: ParsedMessage, this_broker_info: dict):
    # send a response to the requester
    msg_string = json.dumps(
        {"TYPE": "BROKER_NEIGHBORS_RESPONSE", "BROKER_ID": this_broker_info["id"],
         "BROKER_ADDRESS": this_broker_info["address"],
         "BROKER_PORT": this_broker_info["port"],
         "BROKER_NEIGHBORS": broker_neighbors.get_table()})

    send_message(msg_string, this_broker_info["address"], (rcv_msg.get_token("BROKER_ADDRESS"), int(rcv_msg.get_token("BROKER_PORT"))))

    # if the requester broker is already in the neighbour broker, update its online status
    # if not, then insert into the neighbour broker table
    searched_neighbour = broker_neighbors.search_entry(rcv_msg.get_token("BROKER_ID"))

    if searched_neighbour is None:
        neighbor = BrokerNeighborInfo(rcv_msg.get_token("BROKER_ID"), rcv_msg.get_token("BROKER_ADDRESS"),
                                      rcv_msg.get_token("BROKER_PORT"))
        neighbor.set_broker_neighbour_online(True)
        broker_neighbors.add_entry(neighbor)
    else:
        broker_neighbors.set_entry_online(searched_neighbour.get_broker_neighbour_id())

import threading

from broker_messages.broker_neighbors_request_message import BrokerNeighborsRequestMessage
from broker_messages.broker_neighbors_response_message import BrokerNeighborsResponseMessage
from global_data import broker_neighbors, this_broker_info, broker_clock
from message_exchange.message_sender import send_message
from neighbor_management.broker_neighbor_table import BrokerNeighborInfo


def neighbour_broker_live_check():
    # checking if any of your neighbors is offline, by requesting their list of neighbors
    broker_neighbors.check_table_for_potentially_offline_brokers()

    threading.Timer(15, neighbour_broker_live_check).start()


def process_broker_neighbors_response(rcv_msg: BrokerNeighborsResponseMessage):
    # add responder broker if not already in the broker neighbor table

    if broker_neighbors.search_entry(rcv_msg.broker_id) is None:
        broker_neighbors.add_entry(BrokerNeighborInfo(broker_neighbor_id=rcv_msg.broker_id,
                                                      broker_neighbor_address=rcv_msg.broker_address,
                                                      broker_neighbor_port=rcv_msg.broker_port,
                                                      broker_neighbor_last_online_timestamp=broker_clock.current_broker_time()))
    else:
        broker_neighbors.set_entry_online(rcv_msg.broker_id)

    # insert neighbors in broker neighbors table
    for entry in rcv_msg.broker_neighbors:
        if entry["BROKER_ID"] == this_broker_info["id"]:
            continue
        if broker_neighbors.search_entry(entry["BROKER_ID"]) is None:
            broker_neighbors.add_entry(BrokerNeighborInfo(broker_neighbor_id=entry["BROKER_ID"],
                                                          broker_neighbor_address=entry["BROKER_ADDRESS"],
                                                          broker_neighbor_port=entry["BROKER_PORT"],
                                                          broker_neighbor_last_online_timestamp=broker_clock.current_broker_time()))
        else:
            broker_neighbors.set_entry_online(entry["BROKER_ID"])


def process_broker_neighbors_request(rcv_msg: BrokerNeighborsRequestMessage):
    # send a response to the requester

    neighbors_response_msg = BrokerNeighborsResponseMessage(
        broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
        broker_port=this_broker_info["port"], well_formed=True, broker_neighbors=broker_neighbors.get_table_as_list()
    )

    send_message(neighbors_response_msg.as_json(), this_broker_info["address"],
                 (rcv_msg.broker_address, rcv_msg.broker_port))

    # if the requester broker is already in the neighbor broker, update its online status
    # if not, then insert into the neighbor broker table
    if broker_neighbors.search_entry(rcv_msg.broker_id) is None:
        broker_neighbors.add_entry(BrokerNeighborInfo(broker_neighbor_id=rcv_msg.broker_id,
                                                      broker_neighbor_address=rcv_msg.broker_address,
                                                      broker_neighbor_port=rcv_msg.broker_port,
                                                      broker_neighbor_last_online_timestamp=broker_clock.current_broker_time()))
    else:
        broker_neighbors.set_entry_online(rcv_msg.broker_id)

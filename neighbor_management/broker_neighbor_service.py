"""
This module contains function related to the management of broker neighbors. A neighbor is another context broker
in the network which this broker has the identifying information (id, address, port) in the broker_neighbor_table

Functions:
--------
neighbor_broker_live_check
process_broker_neighbors_response
process_broker_neighbors_request
"""

import threading

from broker_messages.broker_message_classes import BrokerNeighborsRequestMessage, BrokerNeighborsResponseMessage
from global_data import broker_neighbors, this_broker_info, broker_internal_clock
from message_exchange.message_sender import send_message
from neighbor_management.broker_neighbor_table import BrokerNeighborInfo


def broker_neighbor_still_online_check():
    """
       Function called regularly to check if any neighbor is potentially offline
    """

    broker_neighbors.check_table_for_potentially_offline_brokers()

    threading.Timer(15, broker_neighbor_still_online_check).start()


def process_broker_neighbors_response(rcv_msg: BrokerNeighborsResponseMessage):
    """
        Processes a broker_neighbors_response message

        It will update the broker_neighbor_table as necessary

        More info about BrokerNeighborsResponseMessage can be found in broker_messages.broker_message_classes.BrokerNeighborsResponseMessage

        Parameters
        ----------
        rcv_msg: BrokerNeighborsResponseMessage
            a dataclass instance representing the received broker_neighbors_response message
    """

    # are the input parameters valid?
    if rcv_msg is not None and isinstance(rcv_msg, BrokerNeighborsResponseMessage):
        # add responder broker if not already in the broker neighbor table
        if broker_neighbors.search_entry(rcv_msg.broker_id) is None:
            broker_neighbors.add_entry(BrokerNeighborInfo(broker_neighbor_id=rcv_msg.broker_id,
                                                          broker_neighbor_address=rcv_msg.broker_address,
                                                          broker_neighbor_port=rcv_msg.broker_port,
                                                          broker_neighbor_last_online_timestamp=broker_internal_clock.current_broker_time()))
        else:
            # if already in table, mark it as online
            broker_neighbors.set_entry_online(rcv_msg.broker_id)

        # insert neighbors in broker_neighbor_table in a similar fashion
        for entry in rcv_msg.broker_neighbors:
            if entry["broker_id"] == this_broker_info["id"]:
                continue
            if broker_neighbors.search_entry(entry["broker_id"]) is None:
                broker_neighbors.add_entry(BrokerNeighborInfo(broker_neighbor_id=entry["broker_id"],
                                                              broker_neighbor_address=entry["broker_address"],
                                                              broker_neighbor_port=entry["broker_port"],
                                                              broker_neighbor_last_online_timestamp=broker_internal_clock.current_broker_time()))
            else:
                broker_neighbors.set_entry_online(entry["broker_id"])

        # broker_logger.debug("broker_neighbors :" + str(broker_neighbors.get_table_as_list_of_dict()))


def process_broker_neighbors_request(rcv_msg: BrokerNeighborsRequestMessage):
    """
           Processes a broker_neighbors_request message

           It will respond with a BrokerNeighborsResponseMessage to the requester broker

           More info about BrokerNeighborsRequestMessage can be found in broker_messages.broker_message_classes.BrokerNeighborsRequestMessage

           Parameters
           ----------
           rcv_msg: BrokerNeighborsRequestMessage
               a dataclass instance representing the received broker_neighbors_request message
       """

    # are the input parameters valid?
    if rcv_msg is not None and isinstance(rcv_msg, BrokerNeighborsRequestMessage):
        # send a response to the requester
        neighbors_response_msg = BrokerNeighborsResponseMessage(
            broker_id=this_broker_info["id"], broker_address=this_broker_info["address"],
            broker_port=this_broker_info["port"], broker_neighbors=broker_neighbors.get_table_as_list_of_dict()
        )

        send_message(neighbors_response_msg.as_json(), this_broker_info["address"],
                     (rcv_msg.broker_address, rcv_msg.broker_port))

        # if the requester broker is already in the neighbor broker, update its online status
        # if not, then insert into the broker neighbor table
        if broker_neighbors.search_entry(rcv_msg.broker_id) is None:
            broker_neighbors.add_entry(BrokerNeighborInfo(broker_neighbor_id=rcv_msg.broker_id,
                                                          broker_neighbor_address=rcv_msg.broker_address,
                                                          broker_neighbor_port=rcv_msg.broker_port,
                                                          broker_neighbor_last_online_timestamp=broker_internal_clock.current_broker_time()))
        else:
            broker_neighbors.set_entry_online(rcv_msg.broker_id)

    # broker_logger.debug("broker_neighbors :" + str(broker_neighbors.get_table_as_list_of_dict()))

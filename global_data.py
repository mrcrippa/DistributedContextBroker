import logging

"""
File with all the data structures that will be shared among all modules
"""
####################################
# a dict with information about this specific broker ({"id": broker unique id, "address": address, "port": port})
this_broker_info = {"id": "", "address": "", "port": 0}
####################################
# the internal broker clock
from utils.broker_clock import BrokerClock
broker_internal_clock = BrokerClock(True)
####################################
# general logger
broker_logger = logging.getLogger()
####################################
# the context info table
from context_management.context_info_table import ContextInfoTable
ctx_table = ContextInfoTable()
####################################
# the subscription table
from subscriptions.subscription_table import SubscriptionTable
sub_table = SubscriptionTable()
####################################
# list of all the presumed online neighbors
# each entry is a dict with keys "broker_id","broker_address" and "broker_port"
from neighbor_management.broker_neighbor_table import BrokerNeighborTable
broker_neighbors = BrokerNeighborTable()


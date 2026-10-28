import logging
from context_management.context_info_table import ContextInfoTable
from subscriptions.subscription_table import SubscriptionTable
from neighbor_management.broker_neighbor_table import BrokerNeighborTable
from utils.broker_clock import BrokerClock

"""
File with all the data structures that will be shared among all modules
"""

####################################
# the internal broker clock
broker_clock = BrokerClock(True)
####################################
# general logger
broker_logger = logging.getLogger()
####################################
# the context info table
ctx_table = ContextInfoTable()
####################################
# the subscription table
sub_table = SubscriptionTable()
####################################
# list of all the presumed online neighbors
# each entry is a dict with keys "broker_id","broker_address" and "broker_port"
broker_neighbors = BrokerNeighborTable()
####################################
# a dict with information about this specific broker ({"id": broker unique id, "address": address, "port": port})
this_broker_info = {"id": "", "address": "", "port": 0}

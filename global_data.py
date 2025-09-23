import logging
from threading import Lock

def is_string_valid_int_bigger_than_zero(value: str) -> bool:
    """check if a string is a valid integer bigger than zero"""
    try:
        if int(value) <= 0:
            return False
    except ValueError:
        return False
    return True

####################################
# value used for correcting the internal broker time, if using NTP, in ms
time_correction = 0
####################################


"file with all the data that will be shared among all modules"




####################################
# general logger variable
logger = logging.getLogger()
####################################
# variables for the context info table
# including the table, a dict with all the timer threads use for the table, and two related locks
from tables.context_info_table import ContextInfoTable
ctx_table_lock = Lock()
ctx_table = ContextInfoTable()
ctx_timer_threads_lock = Lock()
ctx_timer_threads = {}
####################################
# variables for the subscription table
# including the table, a dict with all the timer threads use for the table, and two related locks
from tables.subscription_table import SubscriptionTable
sub_table_lock = Lock()
sub_table = SubscriptionTable()
sub_timer_threads_lock = Lock()
sub_timer_threads = {}
####################################
# list of all the presumed online neighbors
# each element is a dict with keys "broker_id","broker_address" and "broker_port"
from tables.broker_neighbor_table import BrokerNeighborTable
broker_neighbors_lock = Lock()
broker_neighbors = BrokerNeighborTable()

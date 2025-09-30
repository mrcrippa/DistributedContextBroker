import logging
from threading import Lock

"file with all the data that will be shared among all modules"

####################################
# Object representing the internal broker clock
from utils.broker_clock import BrokerClock

broker_clock = BrokerClock(True)
####################################
# general logger variable
broker_logger = logging.getLogger()
####################################
# variables for the context info table
# including the table, a dict with all the timer threads use for the table, and two related locks
from tables.context_info_table import ContextInfoTable

ctx_table = ContextInfoTable()
ctx_timer_threads_lock = Lock()
ctx_timer_threads = {}
####################################
# variables for the subscription table
# including the table, a dict with all the timer threads use for the table, and two related locks
from tables.subscription_table import SubscriptionTable

sub_table = SubscriptionTable()
sub_timer_threads_lock = Lock()
sub_timer_threads = {}
####################################
# list of all the presumed online neighbors
# each element is a dict with keys "broker_id","broker_address" and "broker_port"
from tables.broker_neighbor_table import BrokerNeighborTable

broker_neighbors = BrokerNeighborTable()

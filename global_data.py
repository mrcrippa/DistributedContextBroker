import logging

from tables.context_info_table import ContextInfoTable
from tables.subscription_table import SubscriptionTable
from threading import Lock

"file with all the data that will be shared among all modules"

# info for the broker socket
host_broker_ip = "127.0.0.1"
port_broker_nr = 30000
####################################
# logging location
log_file_name = "log_context_broker.txt"
logger = logging.getLogger()
####################################
# variables for the context info table
# including the table, a dict with all the timer threads use for the table, and two related locks
ctx_table_lock = Lock()
ctx_table = ContextInfoTable()
ctx_timer_threads_lock = Lock()
ctx_timer_threads = {}
####################################
# variables for the subscription table
# including the table, a dict with all the timer threads use for the table, and two related locks
sub_table_lock = Lock()
sub_table = SubscriptionTable()
sub_timer_threads_lock = Lock()
sub_timer_threads = {}

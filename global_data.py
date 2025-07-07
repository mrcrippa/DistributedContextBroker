from queue import Queue

from tables.context_info_table import ContextInfoTable
from tables.subscription_table import SubscriptionTable
from threading import Lock

host_broker_ip = "127.0.0.1"
port_broker_nr = 30000

log_file_name = "log_context_broker.txt"

# message_queue_lock = Lock()
# message_queue = Queue()

####################################
ctx_lock = Lock()
ctx_table = ContextInfoTable()
ctx_timer_threads_lock = Lock()
ctx_timer_threads = {}

####################################
sub_lock = Lock()
sub_table = SubscriptionTable()
sub_timer_threads_lock = Lock()
sub_timer_threads = {}

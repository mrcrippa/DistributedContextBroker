from tables.context_info_table import ContextInfoTable
from tables.subscription_table import SubscriptionTable

host_broker_ip = "127.0.0.1"
port_broker_nr = 30000

log_file_name = "log_context_broker.txt"

message_type_set = {"ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE_ADVERTISEMENT", "CONTEXT_REQUEST", "RESPONSE"}

ctx_table = ContextInfoTable()

sub_table = SubscriptionTable()

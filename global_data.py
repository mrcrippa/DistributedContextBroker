import socket
import threading
import sys
import json
import time

host_broker_ip = "127.0.0.1"
port_broker_nr = 30000

message_type_set = {"ACK", "NACK", "PING", "SUBSCRIBE", "UPDATE_ADVERTISEMENT", "CONTEXT_REQUEST", "RESPONSE"}
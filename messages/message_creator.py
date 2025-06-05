from global_data import *

def create_nack_message_string(operation, requester_address, reason):
    dict_as_string = json.dumps(
        {"TYPE": "NACK", "TIMESTAMP": str(int(round(time.time() * 1000))), "OPERATION": operation,
         "REQUESTER": requester_address, "REASON": reason})

    return dict_as_string

def create_ping_message_string():
    dict_as_string = json.dumps(
        dict(TYPE="PING", TIMESTAMP=str(int(round(time.time() * 1000)))))

    return dict_as_string
import socket
import sys
import threading
from threading import Thread

from global_data import *
from services.subscribe_service import add_subscription
from services.update_service import process_update
from utils.message import *
from utils.message_creator import *


# def send_message_task(msg, sender_address):


def service_task(received_raw_data, sender_addr, broker_sock, broker_lock):
    received_msg = Message(received_raw_data)
    if received_msg.is_well_formed():
        logging.info("well formed message: " + received_msg.print_message() + " from " + str(sender_addr))

        if received_msg.get_message_token("TYPE") == "PING":
            logging.info("PING message received from " + str(sender_addr))
            broker_lock.acquire()
            broker_sock.sendto(str.encode(create_ping_message_string()), sender_addr)
            broker_lock.release()
        elif received_msg.get_message_token("TYPE") == "SUBSCRIBE":
            logging.info("SUBSCRIBE message received from " + str(sender_addr))
            sub = add_subscription(received_msg)

            print("time : " + str(int(round(time.time() * 1000))) + " - thread : " + str(
                threading.get_ident()) + " - sub_table : " + sub_table.print())
            print("time : " + str(int(round(time.time() * 1000))) + " - thread : " + str(
                threading.get_ident()) + " - sub_timer_threads : " + str(sub_timer_threads))

            response = create_ack_message_string("SUBSCRIBE", str(sender_addr),
                                                 "Consumer subscribed to [ " + sub.get_entity_id() + " , " + sub.get_entity_type() + " , " + sub.get_scope() + " ]")
            broker_lock.acquire()
            broker_sock.sendto(str.encode(response), (sub.get_subscriber_address(), sub.get_subscriber_port()))
            broker_lock.release()

            ctx_lock.acquire()
            current_ctx_info = ctx_table.search_entry(sub.get_entity_id(), sub.get_entity_type(), sub.get_scope())
            ctx_lock.release()

            if current_ctx_info is not None:
                response = create_request_response_message_string(current_ctx_info)
                broker_lock.acquire()
                broker_sock.sendto(str.encode(response), (sub.get_subscriber_address(), sub.get_subscriber_port()))
                broker_lock.release()

        elif received_msg.get_message_token("TYPE") == "UPDATE":
            logging.info("UPDATE message received from " + str(sender_addr))

            ctx_data = process_update(received_msg)

            response = create_ack_message_string("UPDATE", str(sender_addr),
                                                 "Update to [ " + ctx_data.get_entity_id() + " , " + ctx_data.get_entity_type() + " , " + ctx_data.get_scope() + " ]")
            broker_lock.acquire()
            broker_sock.sendto(str.encode(response), sender_addr)
            broker_lock.release()

            for sub in sub_table.search_entry(ctx_data.get_entity_id(), ctx_data.get_entity_type(),
                                              ctx_data.get_scope()):
                response = create_request_response_message_string(ctx_data)
                broker_lock.acquire()
                broker_sock.sendto(str.encode(response), (sub.get_subscriber_address(), sub.get_subscriber_port()))
                broker_lock.release()

        elif received_msg.get_message_token("TYPE") == "REQUEST":
            logging.info("REQUEST message received from " + str(sender_addr))

            ctx_lock.acquire()
            current_ctx_info = ctx_table.search_entry(received_msg.get_message_token("ENTITY_ID"),
                                                      received_msg.get_message_token("ENTITY_TYPE"),
                                                      received_msg.get_message_token("SCOPE"))
            ctx_lock.release()

            if current_ctx_info is not None:
                response = create_request_response_message_string(current_ctx_info)
                broker_lock.acquire()
                broker_sock.sendto(str.encode(response), sender_addr)
                broker_lock.release()
            else:
                response = create_nack_message_string("REQUEST", str(sender_addr),
                                                      "No context data found with the given parameters")
                broker_lock.acquire()
                broker_sock.sendto(str.encode(response), sender_addr)
                broker_lock.release()

    else:
        # if not, send NACK
        response = create_nack_message_string("MESSAGE", str(sender_addr), "Received message is not well formed")
        logging.info("received message not well formed from" + str(sender_addr))
        logging.info("sending response: " + response + " to " + str(sender_addr))
        broker_lock.acquire()
        broker_sock.sendto(str.encode(response), sender_addr)
        broker_lock.release()


def main():
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create a new socket using UDP
    broker_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        broker_sock.bind((host_broker_ip, port_broker_nr))
    except socket.error as error_msg:
        logging.critical("Binding Broker socket failed. Message : " + str(error_msg))
        sys.exit()

    broker_lock = threading.Lock()

    while True:
        print("Waiting for messages...")
        received_raw_data, sender_addr = broker_sock.recvfrom(1024)
        logging.info("received message from " + str(sender_addr))

        Thread(target=service_task, args=[received_raw_data, sender_addr, broker_sock, broker_lock]).start()


main()

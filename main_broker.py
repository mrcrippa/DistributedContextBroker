from global_data import *
from messages.message import *
from messages.message_creator import *

def main():
    # TODO: setup logger

    # TODO: setup data structures and semaphores

    # create a new socket using UDP
    broker_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        broker_sock.bind((host_broker_ip, port_broker_nr))
    except socket.error as error_msg:
        # TODO: log error critical
        print("Binding Broker socket failed. Message : " + str(error_msg))
        sys.exit()


    while True:
        print("Waiting for messages...")
        received_raw_data, sender_addr = broker_sock.recvfrom(1024)
        # TODO: log message received trace
        print("received message: " + received_raw_data.decode() + " from " + str(sender_addr))

        # TODO: check if message is properly constructed
        received_msg = Message(received_raw_data)
        if received_msg.is_well_formed():
            #  process message
            print(received_msg.print_message())
            print("Well formed message")

            if received_msg.get_message_token("TYPE") == "PING":
                # TODO: log ping message received trace
                broker_sock.sendto(str.encode(create_ping_message_string()), sender_addr)

        else:
            # if not, send NACK
            response = create_nack_message_string("MESSAGE", str(sender_addr), "Received message is not well formed")
            print("Sending response: " + response)
            # TODO: log message not well formed trace
            broker_sock.sendto(str.encode(response), sender_addr)


main()

import json
import random
import socket
import time


def main():
    # create a new socket using UDP

    consumer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_connected = False

    port_nr = 0
    while not socket_connected:
        port_nr = random.randint(50000, 60000)
        try:
            consumer_sock.bind(("127.0.0.1", port_nr))
        except socket.error:
            continue
        socket_connected = True

    dict_as_string = json.dumps(
        {"TYPE": "SUBSCRIBE",
         "SUBSCRIBER_ADDRESS": "127.0.0.1",
         "SUBSCRIBER_PORT": port_nr,
         "ENTITY_ID": "Porto",
         "ENTITY_TYPE": "location", "SCOPE": "weather_information",
         "DURATION": 60})

    print(dict_as_string)

    consumer_sock.sendto(str.encode(dict_as_string), ("127.0.0.1", 50000))

    dict_as_string = json.dumps(
        {"TYPE": "PING",
         "BROKER_ADDRESS": "127.0.0.1",
         "BROKER_PORT": 50000,
         "BROKER_ID": "test_broker",
         "TIMESTAMP": str(int(time.time() * 1000))})

    print(dict_as_string)

    consumer_sock.sendto(str.encode(dict_as_string), ("127.0.0.1", 50000))

    while True:
        received_raw_data, sender_address = consumer_sock.recvfrom(1024)
        print("received_raw_data : " + str(received_raw_data) + " sender_address : " + str(sender_address))


main()

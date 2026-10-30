import json
import random
import socket
import threading
import time

import openmeteo_requests


def send_info(sock, this_address, this_port_nr, broker_address, broker_port):
    params = {
        "latitude": 49.443,
        "longitude": 7.7716,
        "current": "temperature_2m",
        "timezone": "Europe/Berlin",
        "forecast_days": 1,
    }
    responses = openmeteo_requests.Client().weather_api("https://api.open-meteo.com/v1/forecast", params=params)

    response = responses[0]

    msg_data = f"lat : {response.Latitude():.6f} N\n"
    msg_data += f"lon : {response.Longitude():.6f} E\n"
    msg_data += f"elevation :  {response.Elevation()}m\n"
    msg_data += f"measurement_time : {time.ctime(response.Current().Time())} {response.TimezoneAbbreviation()}\n"
    msg_data += f"measured_temperature_at_2m: {response.Current().Variables(0).Value()}"

    provider_address: str
    provider_port: int

    dict_as_string = json.dumps(
        {"type": "update", "provider_id": "test_provider_kl",
         "entity_id": "Kaiserslautern", "provider_address": "127.0.0.1", "provider_port": this_port_nr,
         "entity_type": "location", "scope": "weather_information",
         "validity_end": int(time.time() * 1000) + 900000, "data": msg_data})

    print(dict_as_string)

    sock.sendto(str.encode(dict_as_string), (broker_address, broker_port))

    threading.Timer(20, send_info,
                    [sock, this_address, this_port_nr, "127.0.0.1", 51000]).start()


def main():
    # create a new socket using UDP

    provider_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_connected = False

    port_nr = 0
    while not socket_connected:
        port_nr = random.randint(50000, 60000)
        try:
            provider_sock.bind(("127.0.0.1", port_nr))
        except socket.error:
            continue
        socket_connected = True

    threading.Timer(1, send_info,
                    [provider_sock, "127.0.0.1", port_nr, "127.0.0.1", 51000]).start()

    while True:
        received_raw_data, sender_address = provider_sock.recvfrom(1024)
        print("received_raw_data : " + str(received_raw_data) + " sender_address : " + str(sender_address))


main()

import threading
import time
import ntplib
import global_data

def time_correction_with_ntp():
    response = ntplib.NTPClient().request('europe.pool.ntp.org', version=3)
    global time_correction
    print(time_correction)
    time_correction = round((response.delay + response.offset) * 1000)
    print(time_correction)
    threading.Timer(10, time_correction_with_ntp,
                    []).start()


def current_broker_time() -> int:
    global time_correction
    return round((time.time() + time_correction) * 1000)

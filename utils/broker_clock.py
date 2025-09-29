import threading
import time

import ntplib


class BrokerClock(object):
    """
        Class representing the internal clock for this broker
    """

    def __init__(self, with_ntp: bool = False):
        self._time_correction = 0
        self._with_ntp = with_ntp

    def time_correction_with_ntp(self):
        if not self._with_ntp:
            return
        response = ntplib.NTPClient().request('europe.pool.ntp.org', version=3)
        self._time_correction = round((response.delay + response.offset) * 1000)
        threading.Timer(10, self.time_correction_with_ntp, []).start()

    def current_broker_time(self) -> int:
        return round((time.time() * 1000) + self._time_correction)

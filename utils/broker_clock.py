"""
This module contains a class representing the internal clock for this broker

Classes:
--------
BrokerClock
"""
import threading
import time

import ntplib


class BrokerClock(object):
    """
        Class representing the internal clock for this broker

        The context brokers in the P2P network do not have any global synchronized clock

        This class uses NTP[1] to sync this broker's internal clock with the NTP system.

        If the NTP system is used, a regular Timer thread will be set to adjust the internal clock

        TIme is given as milliseconds since the Epoch (Unix time)

        [1] https://en.wikipedia.org/wiki/Network_Time_Protocol

        Attributes
        ----------
        _time_correction : int
            time correction value added to the internal clock, in ms
        _with_ntp : bool
            True if the NTP system is to be used, False otherwise
    """

    def __init__(self, with_ntp: bool = False):
        """
        Initializes the instance.

        Parameters
        ----------
        with_ntp : bool
            set to True if the NTP system is to be used, default is False
        """

        self._time_correction = 0
        self._with_ntp = with_ntp

    def time_correction_with_ntp(self):
        """
        If the NTP system is used, this function will be called regularly to adjust the internal clock

        """

        if not self._with_ntp:
            return
        try:
            # uses NTPClient to make a request to the Europe server pool
            response = ntplib.NTPClient().request('europe.pool.ntp.org', version=3)
            # adjust the time correction for this broker clock
            self._time_correction = round((response.delay + response.offset) * 1000)
        except ntplib.NTPException:
            # any problem, leave the correction as 0
            self._time_correction = 0

        # sets up a Timer Thread to regularly adjust the broker clock
        threading.Timer(10, self.time_correction_with_ntp, []).start()

    def current_broker_time(self) -> int:
        """
        Utility function used to get the current time from the internal clock for this broker

        Returns
        -------
        int
            the current time from the internal clock in milliseconds since the Epoch (Unix time)

        """
        return round((time.time() * 1000) + self._time_correction)

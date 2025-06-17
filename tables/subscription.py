def check_integer(s):
    try:
        s = int(s)
        result = True
    except ValueError as e:
        result = False
    return result


class Subscription(object):
    """
        Class representing an entry in the subscription table
    """

    def __init__(self):
        self._entity_id = ""
        self._entity_type = ""
        self._scope = ""
        self._subscriber_address = ""
        self._subscriber_port = 0
        self._duration = 0

    def get_object_id(self):
        return hash(
            self._entity_id + self._entity_type + self._scope + self._subscriber_address + str(self._subscriber_port))

    def get_entity_id(self):
        return self._entity_id

    def set_entity_id(self, entity_id):
        if entity_id is not None:
            self._entity_id = entity_id

    def get_entity_type(self):
        return self._entity_type

    def set_entity_type(self, entity_type):
        if entity_type is not None:
            self._entity_type = entity_type

    def get_scope(self):
        return self._scope

    def set_scope(self, scope):
        if scope is not None:
            self._scope = scope

    def get_subscriber_address(self):
        return self._subscriber_address

    def set_subscriber_address(self, subscriber_address):
        if subscriber_address is not None:
            self._subscriber_address = subscriber_address

    def get_subscriber_port(self):
        return self._subscriber_port

    def set_subscriber_port(self, subscriber_port):
        if subscriber_port is not None and check_integer(subscriber_port) and subscriber_port > 0:
            self._subscriber_port = subscriber_port

    def get_duration(self):
        return self._duration

    def set_duration(self, duration):
        if duration is not None and check_integer(duration) and duration > 0:
            self._duration = duration

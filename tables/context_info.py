def check_integer(s):
    try:
        s = int(s)
        result = True
    except ValueError as e:
        result = False
    return result


class ContextInfo(object):
    """
    Class representing an entry in the context info table
    """

    def __init__(self):
        self._provider_id = ""
        self._entity_id = ""
        self._entity_type = ""
        self._scope = ""
        self._data = ""
        self._validity_begin = 0
        self._validity_end = 0

    def get_object_id(self):
        return hash(self._entity_id + self._entity_type + self._scope)

    def get_provider_id(self):
        return self._provider_id

    def set_provider_id(self, provider_id):
        if provider_id is not None:
            self._provider_id = provider_id

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

    def get_data(self):
        return self._data

    def set_data(self, data):
        if data is not None:
            self._data = data

    def get_validity_begin(self):
        return self._validity_begin

    def set_validity_begin(self, validity_begin):
        if validity_begin is not None and check_integer(validity_begin):
            self._validity_begin = validity_begin

    def get_validity_end(self):
        return self._validity_end

    def set_validity_end(self, validity_end):
        if validity_end is not None and check_integer(validity_end):
            self._validity_end = validity_end

    def print(self):
        contents = {"provider_id": self.get_provider_id(),
                    "entity_id": self.get_entity_id(), "entity_type": self.get_entity_type(), "scope": self.get_scope(),
                    "data": self.get_data(), "validity_begin": self.get_validity_begin(),
                    "validity_end": self.get_validity_end()}

        return str(contents)

from fs_irods import iRODSFS


class iRODSFSBuilder:
    def __init__(self):
        self._host = 'localhost'
        self._port = 1247
        self._user = 'rods'
        self._password = 'rods'
        self._zone = 'tempZone'

    def build(self):
        return iRODSFS(host=self._host, port=self._port, user=self._user, password=self._password, zone=self._zone)
from fs_irods import iRODSFS

from irods.session import iRODSSession
from tests.DelayedSession import DelayedSession

class iRODSFSBuilder:
    def __init__(self):
        self._host = 'localhost'
        self._port = 1247
        self._user = 'rods'
        self._password = 'rods'
        self._zone = 'tempZone'
        self._session = DelayedSession(host=self._host, port=self._port, user=self._user, password=self._password, zone=self._zone)

    def with_host(self, host):
        self._host = host
        return self
    
    def with_port(self, port):
        self._port = port
        return self
    
    def with_user(self, user):
        self._user = user
        return self
    
    def with_password(self, password):
        self._password = password
        return self
    
    def with_zone(self, zone):
        self._zone = zone
        return self
    
    def with_session(self, session):
        self._session = session
        return self
    
    def build(self):
        return iRODSFS(self._session)
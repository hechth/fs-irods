from irods.session import iRODSSession
import time


class DelayedSession(iRODSSession):
    def __exit__(self, exc_type, exc_value, traceback):
        time.sleep(1)
        return super().__exit__(exc_type, exc_value, traceback)
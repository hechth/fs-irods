from io import IOBase
from multiprocessing import RLock
from fs.base import FS
from fs.info import Info
from fs.permissions import Permissions
from fs.errors import DirectoryExists, ResourceNotFound

from irods.session import iRODSSession
from irods.collection import iRODSCollection

from contextlib import contextmanager

from fs_irods.utils import can_create

class iRODSFS(FS):
    def __init__(self, host: str, port:int, user:str, password: str) -> None:
        super().__init__()
        self._lock = RLock()
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        

    @contextmanager
    def _session(self) -> iRODSSession:
        with self._lock:
            with iRODSSession(host=self._host, port=self._port, user=self._user, password=self._password) as session:
                yield session
        

    def getinfo(self, path: str, namespaces: list|None = None) -> Info:
        """Get information about a resource on the filesystem.
        Args:
            path (str): A path to a resource on the filesystem.
            namespaces (list, optional): Info namespaces to query. If
                namespaces is None, then all available namespaces are
                queried. Defaults to None.
        Returns:
            Info: An Info object containing information about the
                resource.
        Raises:
            ResourceNotFound: If the path does not exist.
        """
        raise NotImplementedError()
    
    def listdir(self, path: str) -> list:
        """List a directory on the filesystem.
        Args:
            path (str): A path to a directory on the filesystem.
        Returns:
            list: A list of resources in the directory.
        Raises:
            ResourceNotFound: If the path does not exist.
            DirectoryExpected: If the path is not a directory.
        """
        with self._session() as session:
            coll: iRODSCollection = session.collections.get(path)
            return [item.name for item in coll.items]

    def makedir(self, path: str, permissions: Permissions|None = None, recreate: bool = False):
        """Make a directory on the filesystem.
        Args:
            path (str): A path to a directory on the filesystem.
            permissions (Permissions, optional): A Permissions instance,
                or None to use default permissions. Defaults to None.
            recreate (bool, optional): If False (the default) raise an
                error if the directory already exists, if True do not
                raise an error. Defaults to False.
        Raises:
            DirectoryExists: If the directory already exists and
                recreate is False.
            ResourceNotFound: If the path does not exist.
        """
        with self._session() as session:
            if session.collections.exists(path) and not recreate:
                raise DirectoryExists(path)
            
            session.collections.create(path, recurse=False)
    
    def openbin(self, path: str, mode:str = "r", buffering: int = -1, **options) -> IOBase:
        """Open a binary file-like object on the filesystem.
        Args:
            path (str): A path to a file on the filesystem.
            mode (str, optional): The mode to open the file in, see
                the built-in open() function for details. Defaults to
                "r".
            buffering (int, optional): The buffer size to use for the
                file, see the built-in open() function for details.
                Defaults to -1.
            **options: Additional options to pass to the open() function.
        Returns:
            IO: A file-like object representing the file.
        Raises:
            ResourceNotFound: If the path does not exist and mode does not imply creating the file,
                or if any ancestor of path does not exist.
            FileExpected: If the path is not a file.
            FileExists: If the path exists, and exclusive mode is specified (x in the mode).
        """
        with self._session() as session:
            if not session.data_objects.exists(path) and not can_create(mode):
                raise ResourceNotFound(path)
            
            return session.data_objects.open(path, mode, **options)
    
    def remove(self, path: str):
        """Remove a file from the filesystem.
        Args:
            path (str): A path to a file on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            FileExpected: If the path is not a file.
        """
        raise NotImplementedError()
    
    def removedir(self, path: str):
        """Remove a directory from the filesystem.
        Args:
            path (str): A path to a directory on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            DirectoryExpected: If the path is not a directory.
        """
        raise NotImplementedError()
    
    def setinfo(self, path: str, info: dict) -> None:
        """Set information about a resource on the filesystem.
        Args:
            path (str): A path to a resource on the filesystem.
            info (dict): A dictionary containing the information to set.
        Raises:
            ResourceNotFound: If the path does not exist.
        """
        raise NotImplementedError()

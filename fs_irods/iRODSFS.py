from io import IOBase
import os

from multiprocessing import RLock
from typing import Text
from fs.base import FS
from fs.info import Info
from fs.permissions import Permissions
from fs.errors import DirectoryExists, ResourceNotFound, RemoveRootError, DirectoryExpected, FileExpected, FileExists, DirectoryNotEmpty

from irods.session import iRODSSession
from irods.collection import iRODSCollection
from irods.path import iRODSPath



from contextlib import contextmanager

from fs_irods.utils import can_create

class iRODSFS(FS):
    def __init__(self, host: str, port:int, user:str, password: str, zone: str) -> None:
        super().__init__()
        self._lock = RLock()
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._zone = zone

    def _wrap(self, path: str) -> str:
        if path.startswith(f"/{self._zone}"):
            return path
        return str(iRODSPath(self._zone, path))
        

    @contextmanager
    def _session(self) -> iRODSSession:
        with self._lock:
            with iRODSSession(host=self._host, port=self._port, user=self._user, password=self._password, zone=self._zone) as session:
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
        self._check_exists(path)

        with self._session() as session:
            raw_info = {"basic": {"name": path}}
            if session.data_objects.exists(self._wrap(path)):
                raw_info["basic"]["is_dir"] = False
                raw_info["details"] = {"type": "file"}
            elif session.collections.exists(self._wrap(path)):
                raw_info["basic"]["is_dir"] = True
                raw_info["details"] = {"type": "directory"}
          
            return Info(raw_info)
    
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
        self._check_exists(path)
        with self._session() as session:
            coll: iRODSCollection = session.collections.get(self._wrap(path))
            return [item.path for item in coll.data_objects + coll.subcollections]

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
        if self.isdir(path) and not recreate:
            raise DirectoryExists(path)
        
        if not self.isdir(os.path.dirname(path)):
            raise ResourceNotFound(path)
        
        with self._session() as session:           
            session.collections.create(self._wrap(path), recurse=False)
    
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
        self._check_isfile(path)

        with self._session() as session:
            if not session.data_objects.exists(self._wrap(path)):
                if not can_create(mode):
                    raise ResourceNotFound(path)
                session.data_objects.create(self._wrap(path))
            
            mode = mode.replace("b", "")          
            return session.data_objects.open(self._wrap(path), mode, **options)
    
    def remove(self, path: str):
        """Remove a file from the filesystem.
        Args:
            path (str): A path to a file on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            FileExpected: If the path is not a file.
        """
        self._check_exists(path)
        self._check_isfile(path)
        
        with self._session() as session:            
            session.data_objects.unlink(self._wrap(path))

    def _check_isfile(self, path: str):
        """Check if a path points to a file and raise an FileExpected error if not.
        Args:
            path (str): A path to a file on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            FileExpected: If the path is not a file.
        """
        if not self.isfile(path):
            raise FileExpected(path)
    
    def removedir(self, path: str):
        """Remove a directory from the filesystem.
        Args:
            path (str): A path to a directory on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            DirectoryExpected: If the path is not a directory.
            RemoveRootError: If the path is the root directory.
            DirectoryNotEmpty: If the directory is not empty.
        """
        self._check_exists(path)
        self._check_isdir(path)

        if self._is_root(path):
            raise RemoveRootError()
        if not self.isempty(path):
            raise DirectoryNotEmpty(path)

        with self._session() as session:
            session.collections.remove(self._wrap(path), recurse=False)

    def _is_root(self, path: str) -> bool:
        """Check if path points to root of the filesystem.

        Args:
            path (str): Path to a directory.

        Returns:
            bool: True if path points to root.
        """
        return path in ["/", "", self._zone]

    def removetree(self, path: str):
        """Recursively remove a directory and all its contents. 
        This method is similar to removedir, but will remove the contents of the directory if it is not empty.

        Args:
            path (str):  A path to a directory on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            DirectoryExpected: If the path is not a directory.
        """
        self._check_exists(path)
        self._check_isdir(path)

        with self._session() as session:
            if self._is_root(path):
                root: iRODSCollection = session.collections.get(self._wrap(path))
                for item in root.data_objects:
                    item.unlink()
                for item in root.subcollections:
                    item.remove()
            else:
                session.collections.remove(self._wrap(path), recurse=True)

    def _check_isdir(self, path: str):
        """Check if a path is a directory.
        Args:
            path (str): A path to a resource on the filesystem.
        Raises:
            DirectoryExpected: If the path is not a directory.
        """
        if not self.isdir(path):
            raise DirectoryExpected(path)
    
    def setinfo(self, path: str, info: dict) -> None:
        """Set information about a resource on the filesystem.
        Args:
            path (str): A path to a resource on the filesystem.
            info (dict): A dictionary containing the information to set.
        Raises:
            ResourceNotFound: If the path does not exist.
        """
        self._check_exists(path)           
        raise NotImplementedError()

    def _check_exists(self, path:str):
        """Check if a resource exists.
        Args:
            path (str): A path to a resource on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
        """
        with self._session() as session:
            path = self._wrap(path)
            if not session.data_objects.exists(path) and not session.collections.exists(path):
                raise ResourceNotFound(path)
    
    def isfile(self, path: str) -> bool:
        """Check if a path is a file.
        Args:
            path (str): A path to a resource on the filesystem.
        Returns:
            bool: True if the path is a file, False otherwise.
        """       
        with self._session() as session:
            return session.data_objects.exists(self._wrap(path))
        
    def isdir(self, path: str) -> bool:
        """Check if a path is a directory.
        Args:
            path (str): A path to a resource on the filesystem.
        Returns:
            bool: True if the path is a directory, False otherwise.
        """
        with self._session() as session:
            return session.collections.exists(self._wrap(path))

    def create(self, path:str):
        """Create a file on the filesystem.
        Args:
            path (str): A path to a file on the filesystem.
        Raises:
            ResourceNotFound: If any ancestor of path does not exist.
            FileExists: If the path exists.
        """
        if not self.isdir(os.path.dirname(path)):
            raise ResourceNotFound(path)

        if self.isfile(path):
            raise FileExists(path)

        with self._session() as session:
            session.data_objects.create(self._wrap(path))

    def exists(self, path: str) -> bool:
        """Check if a resource exists.
        Args:
            path (str): A path to a resource on the filesystem.
        Returns:
            bool: True if the path exists, False otherwise.
        """
        with self._session() as session:
            path = self._wrap(path)
            return session.data_objects.exists(path) or session.collections.exists(path)

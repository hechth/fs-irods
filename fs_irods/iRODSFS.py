from io import BufferedIOBase, BufferedRandom, IOBase
import os

from multiprocessing import RLock
from fs.base import FS
from fs.info import Info
from fs.permissions import Permissions
from fs.errors import DirectoryExists, ResourceNotFound, RemoveRootError, DirectoryExpected, FileExpected, FileExists, DirectoryNotEmpty

from irods.session import iRODSSession
from irods.collection import iRODSCollection
from irods.path import iRODSPath
from irods.data_object import iRODSDataObject


from contextlib import contextmanager

from fs_irods.utils import can_create

class iRODSFS(FS):
    def __init__(self, session: iRODSSession) -> None:
        super().__init__()
        self._lock = RLock()
        self._host = session.host
        self._port = session.port
        self._zone = session.zone

        self._session = session

    def wrap(self, path: str) -> str:
        if path.startswith(f"/{self._zone}"):
            return path
        return str(iRODSPath(self._zone, path))
        

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

        with self._lock:
            raw_info: dict = {"basic": {}, "details": {}, "access": {}}
            path = self.wrap(path)
            data_object: iRODSDataObject|iRODSCollection = None

            if self._session.data_objects.exists(path):
                data_object = self._session.data_objects.get(path)
                raw_info["basic"]["is_dir"] = False
                raw_info["details"] = {"type": data_object.type}
                raw_info["details"]["size"] = data_object.size
            elif self._session.collections.exists(path):
                data_object = self._session.collections.get(path)
                raw_info["basic"]["is_dir"] = True
                raw_info["details"] = {"type": "directory"}

            raw_info["basic"]["name"] = data_object.name
            raw_info["access"]["user"] = data_object.owner_name

            raw_info["details"]["modified"] = data_object.modify_time.timestamp()
            raw_info["details"]["created"] = data_object.create_time.timestamp()
          
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
        with self._lock:
            coll: iRODSCollection = self._session.collections.get(self.wrap(path))
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
        
        with self._lock:
            self._session.collections.create(self.wrap(path), recurse=False)
    
    def openbin(self, path: str, mode:str = "r", buffering: int = -1, **options) -> BufferedRandom:
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
        
        create = can_create(mode)
        if not self.exists(path):
            if not create:
                raise ResourceNotFound(path)
            self.create(path)

        self._check_isfile(path)

        with self._lock:
            mode = mode.replace("b", "")
            file = self._session.data_objects.open(
                    self.wrap(path),
                    mode,
                    create,
                    allow_redirect=False,
                    auto_close=False,
                    **options
                )
            return file
    
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
        
        with self._lock:
            self._session.data_objects.unlink(self.wrap(path))

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

        with self._lock:
            self._session.collections.remove(self.wrap(path), recurse=False)

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

        with self._lock:
            if self._is_root(path):
                root: iRODSCollection = self._session.collections.get(self.wrap(path))
                for item in root.data_objects:
                    item.unlink()
                for item in root.subcollections:
                    if item.name == "trash":
                        continue
                    item.remove()
                    item.unregister()
            else:
                self._session.collections.remove(self.wrap(path), recurse=True)

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
        with self._lock:
            path = self.wrap(path)
            if not self._session.data_objects.exists(path) and not self._session.collections.exists(path):
                raise ResourceNotFound(path)
    
    def isfile(self, path: str) -> bool:
        """Check if a path is a file.
        Args:
            path (str): A path to a resource on the filesystem.
        Returns:
            bool: True if the path is a file, False otherwise.
        """       
        with self._lock:
            return self._session.data_objects.exists(self.wrap(path))
        
    def isdir(self, path: str) -> bool:
        """Check if a path is a directory.
        Args:
            path (str): A path to a resource on the filesystem.
        Returns:
            bool: True if the path is a directory, False otherwise.
        """
        with self._lock:
            return self._session.collections.exists(self.wrap(path))

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

        with self._lock:
            self._session.data_objects.create(self.wrap(path))

    def exists(self, path: str) -> bool:
        """Check if a resource exists.
        Args:
            path (str): A path to a resource on the filesystem.
        Returns:
            bool: True if the path exists, False otherwise.
        """
        with self._lock:
            path = self.wrap(path)
            return self._session.data_objects.exists(path) or self._session.collections.exists(path)

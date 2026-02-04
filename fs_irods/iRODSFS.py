import datetime
from io import BufferedRandom
import io
import os

from multiprocessing import RLock
from typing import Text
from fs.base import FS
from fs.errors import DirectoryExists, ResourceNotFound, RemoveRootError, DirectoryExpected, FileExpected, FileExists, DirectoryNotEmpty, DestinationExists
from fs.info import Info
from fs.permissions import Permissions
from fs.walk import Walker

from irods.session import iRODSSession
from irods.collection import iRODSCollection
from irods.path import iRODSPath
from irods.data_object import iRODSDataObject


from fs_irods.utils import can_create

# metadata keys used to store preserved timestamps
_PRESERVE_MOD_KEY = "fs_irods_preserved_modified"
_PRESERVE_CRE_KEY = "fs_irods_preserved_created"

_utc=datetime.timezone(datetime.timedelta(0))

class iRODSFS(FS):
    def __init__(self, session: iRODSSession, root: str|None = None) -> None:
        super().__init__()
        self._lock = RLock()
        self._host = session.host
        self._port = session.port
        self._zone = session.zone

        self._session = session
        self._root = root if root else self._zone

    def wrap(self, path: str) -> str:
        return str(iRODSPath(self._root, path))
    
    def parent(self, path: str):
        return os.path.dirname(path)
    
        
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

            raw_info["details"]["modified"] = data_object.modify_time.replace(tzinfo=_utc).timestamp()
            raw_info["details"]["created"] = data_object.create_time.replace(tzinfo=_utc).timestamp()

            # if preserved timestamps are stored as metadata, prefer those
            try:
                meta = data_object.metadata
            except Exception:
                meta = None

            if meta is not None:
                if _PRESERVE_MOD_KEY in meta:
                    try:
                        raw_info["details"]["modified"] = float(meta.get_one(_PRESERVE_MOD_KEY).value)
                    except Exception:
                        pass
                if _PRESERVE_CRE_KEY in meta:
                    try:
                        raw_info["details"]["created"] = float(meta.get_one(_PRESERVE_CRE_KEY).value)
                    except Exception:
                        pass

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
            if 'a' in mode:
                file.seek(0, io.SEEK_END)
            return file
    
    def remove(self, path: str):
        """Remove a file from the filesystem.
        Args:
            path (str): A path to a file on the filesystem.
        Raises:
            ResourceNotFound: If the path does not exist.
            FileExpected: If the path is not a file.
        """
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
        self._check_exists(path)
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
        self._check_isdir(path)

        with self._lock:
            if self._is_root(path):
                root: iRODSCollection = self._session.collections.get(self.wrap(path))
                for item in root.data_objects:
                    item.unlink()
                for item in root.subcollections:
                    if item.name in ["trash", "home"]:
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
            ResourceNotFound: If the path does not exist.
            DirectoryExpected: If the path is not a directory.
        """
        self._check_exists(path)
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
        self._check_points_into_collection(path)

        if self.isfile(path):
            raise FileExists(path)

        with self._lock:
            self._session.data_objects.create(self.wrap(path))

    def _check_points_into_collection(self, path: str):
        """Check if a path points to a location inside a collection.

        Args:
            path (str): Path to check.

        Raises:
            ResourceNotFound: If the path does not point to a location inside a collection.
        """
        if not self.points_into_collection(path):
            raise ResourceNotFound(path)

    def points_into_collection(self, path: str) -> bool:
        """Return true if the path is located inside a collection, aka the parent is a collection.

        Args:
            path (str): Path to check

        Returns:
            bool: True if the parent of path is a collection.
        """
        return self.isdir(os.path.dirname(path))

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

    def move(self, src_path: str, dst_path: str, overwrite: bool = False, preserve_time: bool = False) -> None:
        """Move a file to the specified location

        Args:
            src_path (str): Path to the current location of the file
            dst_path (str): Path to the target location of the file
            overwrite (bool, optional): Set to True to overwrite an existing destination file. Defaults to False.
            preserve_time (bool, optional): _description_. Defaults to False.
        Raises:
            ResourceNotFound: If the path does not exist.
            FileExpected: If the source path is not a file.
            DestinationExists: If destination path exists and overwrite is False.
        """
        self._check_exists(src_path)
        self._check_isfile(src_path)

        if self.exists(dst_path) and not overwrite:
            raise DestinationExists(dst_path)
        with self._lock:
            self._session.data_objects.move(self.wrap(src_path), self.wrap(dst_path))
    
    def copy(self, src_path: str, dst_path: str, overwrite: bool = False, preserve_time: bool = False):
        """Copy a file from ``src_path`` to ``dst_path``.

        Args:
            src_path (str): Path to source file to copy.
            dst_path (str): Destination path (file path or directory).
            overwrite (bool, optional): If True, overwrite an existing destination file. Defaults to False.
            preserve_time (bool, optional): If True, attempt to preserve the source file's
                modification and creation times. The preserved timestamps are stored as AVU
                metadata on the destination object using the keys
                ``fs_irods_preserved_modified`` and ``fs_irods_preserved_created``. Defaults to False.

        Raises:
            DestinationExists: If ``dst_path`` exists and ``overwrite`` is ``False``.
            ResourceNotFound: If a parent directory of ``dst_path`` does not exist.
            FileExpected: If ``src_path`` is not a file.
            NotImplementedError: If ``preserve_time`` is True but the backend or permissions
                do not allow storing preserved timestamps as metadata.
        """
        self._check_isfile(src_path)

        if self.exists(dst_path):
            if self.isdir(dst_path):
                dst_path = os.path.join(dst_path, os.path.basename(src_path))

            if self.isfile(dst_path):
                if overwrite == False:
                    raise DestinationExists(dst_path)
                self.remove(dst_path)
        else:
            self._check_points_into_collection(dst_path)
        
        with self._lock:
            self._session.data_objects.copy(self.wrap(src_path), self.wrap(dst_path))

            if preserve_time:
                    # store preserved timestamps on the destination data object as metadata
                    try:
                        src_info = self.getinfo(src_path)
                        dest_obj = self._session.data_objects.get(self.wrap(dst_path))
                        # store raw epoch timestamps (floats) as strings
                        src_mod = src_info.get('details', 'modified')
                        src_cre = src_info.get('details', 'created')
                        dest_obj.metadata.set(_PRESERVE_MOD_KEY, str(src_mod))
                        dest_obj.metadata.set(_PRESERVE_CRE_KEY, str(src_cre))
                    except Exception:
                        # if backend doesn't support metadata or something goes wrong, raise to notify caller
                        raise NotImplementedError("preserve_time not supported by backend")
                    
    def copydir(self, src_path: str, dst_path: str, create: bool = False, preserve_time: bool = False):
        """Copy the contents of the folder ``src_path`` to ``dst_path``.

        The result of the operation is that a directory named like the source basename
        will be created under ``dst_path``. For example, copying ``/a/b`` into
        ``/x`` will create ``/x/b`` and populate it with the contents of ``/a/b``.

        Args:
            src_path (str): Source directory to copy. Must be a directory.
            dst_path (str): Destination directory under which the source-subtree will be created.
            create (bool, optional): If ``True``, create ``dst_path`` when it does not exist.
                Defaults to False.
            preserve_time (bool, optional): If ``True``, attempt to preserve modification and
                creation times for both directories and files by storing epoch timestamps
                as AVU metadata on destination objects. The metadata keys used are
                ``fs_irods_preserved_modified`` and ``fs_irods_preserved_created``. Defaults to False.

        Behavior:
            - Directories under ``src_path`` are recreated under ``dst_path/basename(src_path)``.
            - Files are copied using :py:meth:`copy` with ``overwrite=True``, so existing files
              in the destination are overwritten.
            - When ``preserve_time`` is requested, timestamps are recorded as AVU metadata
              on destination objects and ``getinfo`` will prefer these preserved values when present.

        Raises:
            ResourceNotFound: If ``dst_path`` does not exist and ``create`` is ``False``.
            DirectoryExpected: If ``src_path`` is not a directory.
            NotImplementedError: If ``preserve_time`` is True but the backend or permissions
                do not allow storing preserved timestamps as metadata.
        """
        self._check_isdir(src_path)
        if create and self.isdir(dst_path) is False:
            self.makedirs(dst_path)
        self._check_isdir(dst_path)

        src_basename = os.path.basename(src_path)
        dst = os.path.join(dst_path, src_basename)

        # ensure destination root exists
        self.makedirs(dst, recreate=True)

        # preserve timestamps for root directory if requested
        if preserve_time:
            try:
                src_root_info = self.getinfo(src_path)
                coll_root = self._session.collections.get(self.wrap(dst))
                coll_root.metadata.set(_PRESERVE_MOD_KEY, str(src_root_info.get('details','modified')))
                coll_root.metadata.set(_PRESERVE_CRE_KEY, str(src_root_info.get('details','created')))
            except Exception:
                raise NotImplementedError("preserve_time not supported by backend for root directory")

        walker = Walker(self)

        for path, dirs, files in walker.walk(self, path=src_path, namespaces=["details"]):
            # compute path relative to the copy root
            rel = os.path.relpath(path, src_path)
            if rel == ".":
                rel = ""

            if rel:
                target_dir = os.path.join(dst, rel)
            else:
                target_dir = dst

            # create directories found by walker
            for dir_info in dirs:
                dir_name = dir_info.name
                dst_dir = os.path.join(target_dir, dir_name)
                self.makedirs(dst_dir, recreate=True)
                # preserve directory timestamps if requested
                if preserve_time:
                    try:
                        src_dir_info = self.getinfo(os.path.join(path, dir_name))
                        coll = self._session.collections.get(self.wrap(dst_dir))
                        # store raw epoch timestamps (floats) as strings
                        src_mod = src_dir_info.get('details', 'modified')
                        src_cre = src_dir_info.get('details', 'created')
                        coll.metadata.set(_PRESERVE_MOD_KEY, str(src_mod))
                        coll.metadata.set(_PRESERVE_CRE_KEY, str(src_cre))
                    except Exception:
                        raise NotImplementedError("preserve_time not supported by backend for directories")

            # copy files found by walker
            for file in files:
                src_file = os.path.join(path, file.name)
                dst_file = os.path.join(target_dir, file.name)
                # overwrite any existing files in destination
                self.copy(src_file, dst_file, overwrite=True, preserve_time=preserve_time)
    
    def upload(self, path: str, file: io.IOBase | str, chunk_size: int|None = None, **options):
        """Set a file to the contents of a binary file object.

        This method copies bytes from an open binary file to a file on
        the filesystem. If the destination exists, it will first be
        truncated.

        Arguments:
            path (str): A path on the filesystem.
            file (io.IOBase or str): a file object open for reading in
                binary mode or a path to a local file to upload.
            chunk_size (int, optional): Number of bytes to read at a
                time, if a simple copy is used, or `None` to use
                sensible default.
            **options: Implementation specific options required to open
                the source file.

        Raises:
            ResourceNotFound: If a parent directory of ``path`` does not exist.

        Note that the file object ``file`` will *not* be closed by this
        method. Take care to close it after this method completes
        (ideally with a context manager).

        Example:
            >>> with open('~/movies/starwars.mov', 'rb') as read_file:
            ...     my_fs.upload('starwars.mov', read_file)

        """
        if isinstance(file, io.IOBase):
            super().upload(path, file, chunk_size, **options)
        elif isinstance(file, str):
            self._check_points_into_collection(path)
            with self._lock:
                self._session.data_objects.put(
                    file,
                    self.wrap(path),
                    allow_redirect=False,
                    auto_close=False
                )
        else:
            raise NotImplementedError()
    
    def download(self, path: str, file: io.IOBase | str, chunk_size=None, **options):
        """Copy a file from the filesystem to a file-like object.

        This may be more efficient that opening and copying files
        manually if the filesystem supplies an optimized method.

        Note that the file object ``file`` will *not* be closed by this
        method. Take care to close it after this method completes
        (ideally with a context manager).

        Arguments:
            path (str): Path to a resource.
            file (file-like): A file-like object open for writing in
                binary mode.
            chunk_size (int, optional): Number of bytes to read at a
                time, if a simple copy is used, or `None` to use
                sensible default.
            **options: Implementation specific options required to open
                the source file.

        Example:
            >>> with open('starwars.mov', 'wb') as write_file:
            ...     my_fs.download('/Videos/starwars.mov', write_file)

        Raises:
            ResourceNotFound: if ``path`` does not exist.
        """
        if isinstance(file, io.IOBase):
            super().download(path, file, chunk_size=chunk_size, **options)
        elif(isinstance(file, str)):
            with self._lock:
                self._check_exists(path)
                self._session.data_objects.get(
                    self.wrap(path),
                    file,
                    allow_redirect=False,
                    auto_close=False
                )
        else:
            raise NotImplementedError()

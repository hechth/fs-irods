import datetime
import io
import logging
import os
from io import BufferedRandom
from multiprocessing import RLock
from weakref import WeakKeyDictionary
from fs.base import FS
from fs.errors import DestinationExists
from fs.errors import DirectoryExists
from fs.errors import DirectoryExpected
from fs.errors import DirectoryNotEmpty
from fs.errors import FileExists
from fs.errors import FileExpected
from fs.errors import RemoveRootError
from fs.errors import ResourceNotFound
from fs.info import Info
from fs.permissions import Permissions
from fs.walk import Walker
from irods.at_client_exit import register_for_execution_before_prc_cleanup
from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.path import iRODSPath
from irods.session import iRODSSession
from fs_irods.utils import can_create

fses = WeakKeyDictionary()
_logger = logging.getLogger(__name__)


# Close out dangling file handles.
def finalize():
    for fs in list(fses):
        fs._finalize_files()


register_for_execution_before_prc_cleanup(finalize)

_utc = datetime.timezone(datetime.timedelta(0))


class iRODSFS(FS):
    def __init__(self, session: iRODSSession, root: str | None = None) -> None:
        super().__init__()
        self._lock = RLock()
        self._host = session.host
        self._port = session.port
        self._zone = session.zone
        self._session = session
        self._finalizing = False
        self.files = WeakKeyDictionary()
        fses[self] = None
        self._root = root if root else self._zone

    def wrap(self, path: str) -> str:
        return str(iRODSPath(self._root, path))

    def parent(self, path: str):
        return os.path.dirname(path)

    def getinfo(self, path: str, namespaces: list | None = None) -> Info:
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
            data_object: iRODSDataObject | iRODSCollection = None

            if self._session.data_objects.exists(path):
                data_object = self._session.data_objects.get(path)
                raw_info["basic"]["is_dir"] = False
                raw_info["details"] = {"type": data_object.type}
                raw_info["details"]["size"] = data_object.size
                raw_info["details"]["checksum"] = data_object.checksum
                raw_info["details"]["comments"] = data_object.comments
                raw_info["details"]["expiry"] = data_object.expiry  # datatype: string
            elif self._session.collections.exists(path):
                data_object = self._session.collections.get(path)
                raw_info["basic"]["is_dir"] = True
                raw_info["details"] = {"type": "directory"}

            raw_info["basic"]["name"] = data_object.name
            raw_info["access"]["user"] = data_object.owner_name

            raw_info["details"]["modified"] = data_object.modify_time.replace(tzinfo=_utc).timestamp()
            raw_info["details"]["created"] = data_object.create_time.replace(tzinfo=_utc).timestamp()

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

    def makedir(self, path: str, permissions: Permissions | None = None, recreate: bool = False):
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

    def _finalize_files(self):
        self._finalizing = True
        l = list(self.files)
        while l:
            f = l.pop()
            if not f.closed:
                f.close()

    def __del__(self):
        if not self._finalizing:
            self._finalize_files()

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str = "",
        **options,
    ):
        """Open a file.

        Stores weak references to open file handles that maintain a hard reference to the iRODSFS object.
        In this way, the iRODSFS can only be destructed once these file handles are gone.

        Arguments:
            path (str): A path to a file on the filesystem.
            mode (str): Mode to open the file object with
                (defaults to *r*).
            buffering (int): Buffering policy (-1 to use
                default buffering, 0 to disable buffering, 1 to select
                line buffering, of any positive integer to indicate
                a buffer size).
            encoding (str): Encoding for text files (defaults to
                ``utf-8``)
            errors (str, optional): What to do with unicode decode errors
                (see `codecs` module for more information).
            newline (str): Newline parameter.
            **options: keyword arguments for any additional information
                required by the filesystem (if any).

        Returns:
            io.IOBase: a *file-like* object.

        Raises:
            fs.errors.FileExpected: If the path is not a file.
            fs.errors.FileExists: If the file exists, and *exclusive mode*
                is specified (``x`` in the mode).
            fs.errors.ResourceNotFound: If the path does not exist.
        """
        fd = super().open(path, mode, buffering, encoding, errors, newline, **options)
        self.files[fd] = self
        return fd

    def openbin(self, path: str, mode: str = "r", buffering: int = -1, **options) -> BufferedRandom:
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
                self.wrap(path), mode, create, allow_redirect=False, auto_close=False, **options
            )
            if "a" in mode:
                file.seek(0, io.SEEK_END)

            self.files[file] = self
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
        """Set metadata for a file or directory.
        Args:
            path (str): Path to a file or directory on the filesystem.
            info (dict): Dictionary with metadata. Format: 
            {"details": {"modified": <int>, "created": <int>, 
                    "expiry": <str>, "comments": <str>}}
        Raises:
            ResourceNotFound: If the path does not exist.
            ValueError: If field values are invalid.
        """
        self._check_exists(path)

        wrapped_path = self.wrap(path)
        meta_dict = {}

        if "details" in info:
            details = info["details"]
            if "modified" in details:
                meta_dict["dataModify"] = self._validate_and_format_timestamp(details["modified"], "modified")
            if "created" in details:
                meta_dict["dataCreate"] = self._validate_and_format_timestamp(details["created"], "created")
            if "expiry" in details:
                meta_dict["dataExpiry"] = str(self._validate_and_format_timestamp(details["expiry"], "expiry"))
            if "comments" in details:
                comments = details["comments"]
                if not isinstance(comments, str):
                    raise ValueError("'comments' must be a string")
                meta_dict["dataComments"] = comments

        # If there are no fields to set, return early
        if not meta_dict:
            return

        with self._lock:
            # Use modDataObjMeta for files and touch for collections (directories)
            if self.isfile(path):
                self._session.data_objects.modDataObjMeta(
                    {"objPath": wrapped_path},
                    meta_dict
                )
            elif self.isdir(path):
                # For collections, use touch to update modification time
                if "dataModify" in meta_dict:
                    self._session.collections.touch(
                        wrapped_path,
                        seconds_since_epoch=meta_dict["dataModify"]
                    )

    def _validate_and_format_timestamp(self, value, field_name: str) -> int:
        """Validate that `value` can be parsed as a non-negative int timestamp.
        Args:
            value (dict): The value to validate and format.
            field_name (str): The name of the field being validated (for error messages).
        Returns:
            int: The integer timestamp.
        Raises:
            ValueError: If `value` is not an integer or is negative.
        """
        try:
            ts = int(value)
        except Exception:
            raise ValueError(f"'{field_name}' must be an integer timestamp") from Exception
        if ts < 0:
            raise ValueError(f"'{field_name}' timestamp must be >= 0")
        return ts

    def _check_exists(self, path: str):
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

    def create(self, path: str):
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
        """Move a file to the specified location.

        Args:
            src_path (str): Path to the current location of the file
            dst_path (str): Path to the target location of the file
            overwrite (bool, optional): Set to True to overwrite an existing destination file. Defaults to False.
            preserve_time (bool, optional): Set to True to preserve the original modification time. Defaults to False.

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
            if preserve_time:
                self._preserve_modified_time(src_path, dst_path)

    def _preserve_modified_time(self, src_path: str, dst_path: str) -> None:
        """
        Copy the modified time field from src to dst if present
        
        Args:
            src_path (str): Source path to copy modified time from
            dst_path (str): Destination path to copy modified time to
        """
        src_info = self.getinfo(src_path, namespaces=["details"])
        modified_time = src_info.raw.get("details", {}).get("modified")
        if modified_time is not None:
            self.setinfo(dst_path, {"details": {"modified": int(modified_time)}})

    def movedir(self, src_path: str, dst_path: str, overwrite: bool = False, preserve_time: bool = False) -> None:
        """Move a directory to the specified location

        Args:
            src_path (str): Path to the current location of the directory
            dst_path (str): Path to the target location of the directory
            overwrite (bool, optional): Set to True to overwrite an existing destination directory. Defaults to False.
            preserve_time (bool, optional): Set to True to preserve the original modification time. Defaults to False.
        Raises:
            ResourceNotFound: If the source path does not exist.
            DirectoryExpected: If the source path is not a directory.
            DestinationExists: If destination path exists and overwrite is False.
        """
        self._check_exists(src_path)
        self._check_isdir(src_path)

        dest_exists = self.exists(dst_path)
        if dest_exists and not overwrite:
            raise DestinationExists(dst_path)

        metadata = {}
        if preserve_time:
            metadata = self._collect_directory_tree_metadata(src_path)

        with self._lock:
            if dest_exists and overwrite:
                self._session.collections.remove(self.wrap(dst_path), recurse=True)
            self._session.collections.move(self.wrap(src_path), self.wrap(dst_path))

        if metadata is not None:
            self._apply_directory_tree_metadata(dst_path, metadata)

    def _collect_directory_tree_metadata(self, src_path: str) -> dict:
        """Recursively collect modification time metadata for all files and directories in a directory tree.

        Args:
            src_path (str): Root directory to collect metadata from
            
        Returns:
            dict: Dictionary mapping relative paths to their modification times
        """
        metadata = {}
        walker = Walker(self)

        for path, dirs, files in walker.walk(self, path=src_path, namespaces=["details"]):
            rel = os.path.relpath(path, src_path)
            if rel == ".":
                rel = ""
            # Collect metadata for directories
            for dir_entry in dirs:
                self._collect_entry_metadata(path, rel, dir_entry, metadata)
            # Collect metadata for files
            for file_entry in files:
                self._collect_entry_metadata(path, rel, file_entry, metadata)
        # Capture metadata for the root directory itself so its modification time can be restored
        root_info = self.getinfo(src_path, namespaces=["details"])
        root_modified = root_info.raw.get("details", {}).get("modified")
        if root_modified is not None:
            metadata[""] = root_modified
        return metadata

    def _collect_entry_metadata(self, path: str, rel: str, entry, metadata: dict) -> None:
        """Collect metadata for a file or directory entry.
        
        Args:
            path (str): Current path during traversal
            rel (str): Path relative to source root
            entry: Entry object (file or directory)
            metadata (dict): Dictionary to store collected metadata
        """
        entry_name = getattr(entry, "name", entry)
        entry_name = str(entry_name)

        src_entry = os.path.join(path, entry_name)
        rel_entry = os.path.join(rel, entry_name) if rel else entry_name

        info = self.getinfo(src_entry, namespaces=["details"])
        modified_time = info.raw.get("details", {}).get("modified")
        if modified_time is not None:
            metadata[rel_entry] = modified_time

    def _apply_directory_tree_metadata(self, dst_path: str, metadata: dict) -> None:
        """Apply collected metadata to a directory tree at the destination.
        
        Args:
            dst_path (str): Root destination path where metadata should be applied
            metadata (dict): Dictionary mapping relative paths to their modification times
        """
        for rel_path, modified_time in metadata.items():
            path = os.path.join(dst_path, rel_path)
            self.setinfo(path, {"details": {"modified": int(modified_time)}})

    def copy(self, src_path: str, dst_path: str, overwrite: bool = False, preserve_time: bool = False):
        """Copy a file from one position to another.

        Args:
            src_path (str): Path to source file to copy
            dst_path (str): Destination
            overwrite (bool, optional): Whether to overwrite if the destination exists. Defaults to False.
            preserve_time (bool, optional): Whether to preserve the original modification time. Defaults to False.

        Raises:
            DestinationExists: If ``dst_path`` exists and ``overwrite`` is `False`.
            ResourceNotFound: If a parent directory of ``dst_path`` does not exist.
            FileExpected: If ``src_path`` is not a file.
        """
        self._check_isfile(src_path)

        if self.exists(dst_path):
            if self.isdir(dst_path):
                dst_path = os.path.join(dst_path, os.path.basename(src_path))
            if self.isfile(dst_path):
                if overwrite is False:
                    raise DestinationExists(dst_path)
                self.remove(dst_path)
        else:
            self._check_points_into_collection(dst_path)

        with self._lock:
            self._session.data_objects.copy(self.wrap(src_path), self.wrap(dst_path))
            if preserve_time:
                self._preserve_modified_time(src_path, dst_path)

    def copydir(self, src_path: str, dst_path: str, create: bool = False, preserve_time: bool = False):
        """Copy the contents of the folder src_path to dst_path.

        Args:
            src_path (str): Source directory to copy.
            dst_path (str): Where to copy the folder to.
            create (bool, optional): Create the target directory if it does not exist. Defaults to False.
            preserve_time (bool, optional): Preserve the modification time. Defaults to False.
        Raises:
            ResourceNotFound: If the ``dst_path`` does not exist, and ``create`` is not `True`.
            DirectoryExpected: If ``src_path`` is not a directory.
        """
        self._check_isdir(src_path)

        src_basename = os.path.basename(src_path)
        dst = os.path.join(dst_path, src_basename)

        if not self.isdir(dst):
            if create or self.isdir(dst_path):
                self.makedirs(dst, recreate=True)
            else:
                raise ResourceNotFound(dst_path)

        walker = Walker(self)

        for path, dirs, files in walker.walk(self, path=src_path, namespaces=["details"]):

            rel = os.path.relpath(path, src_path)
            if rel == ".":
                rel = ""

            target_dir = os.path.join(dst, rel) if rel else dst
            for dir_entry in dirs:
                dir_name = getattr(dir_entry, "name", dir_entry)
                dir_name = str(dir_name)
                dst_dir = os.path.join(target_dir, dir_name)
                self.makedirs(dst_dir, recreate=True)
                if preserve_time:
                    src_dir = os.path.join(path, dir_name)
                    self._preserve_modified_time(src_dir, dst_dir)
            for file_entry in files:
                file_name = getattr(file_entry, "name", file_entry)
                file_name = str(file_name)
                src_file = os.path.join(path, file_name)
                dst_file = os.path.join(target_dir, file_name)
                self.copy(src_file, dst_file, overwrite=True, preserve_time=preserve_time)

    def upload(self, path: str, file: io.IOBase | str, chunk_size: int | None = None, **options):
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
                self._session.data_objects.put(file, self.wrap(path), allow_redirect=False, auto_close=False)
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
        elif isinstance(file, str):
            with self._lock:
                self._check_exists(path)
                self._session.data_objects.get(self.wrap(path), file, allow_redirect=False, auto_close=False)
        else:
            raise NotImplementedError()

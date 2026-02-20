import time
from typing import Generator, List
import pytest
import os

from fs_irods import iRODSFS
from unittest.mock import patch
from tests.DelayedSession import DelayedSession
from tests.iRODSFSBuilder import iRODSFSBuilder

from fs.errors import *
from fs.walk import Walker
import six

from tests.test_utils import WalkResult

def assert_bytes(fs: iRODSFS, path: str, contents: bytes):
    """Assert a file contains the given bytes.
    Arguments:
        path (str): A path on the filesystem.
        contents (bytes): Bytes to compare.
    """
    assert isinstance(contents, bytes)
    data = fs.readbytes(path)
    assert data == contents
    assert type(data) == bytes


@pytest.fixture
def fs():
    builder: iRODSFSBuilder = iRODSFSBuilder().with_root("/")
    sut = builder.build()

    if not sut.exists("/tempZone/existing_file.txt"):
        sut.create("/tempZone/existing_file.txt")
    if not sut.exists("/tempZone/existing_collection"):
        sut.makedir("/tempZone/existing_collection")
    if not sut.exists("/tempZone/existing_collection/existing_file.txt"):
        sut.create("/tempZone/existing_collection/existing_file.txt")
        sut.writetext("/tempZone/existing_collection/existing_file.txt", "content")

    yield sut

    if sut.exists("/tempZone/existing_collection"):
        sut.removetree("/tempZone/existing_collection")
    if sut.exists("/tempZone/existing_file.txt"):
        sut.remove("/tempZone/existing_file.txt")
    if sut.exists("/tempZone/new_collection"):
        sut.removetree("/tempZone/new_collection")
    
    del(sut)
    builder._session.cleanup()


# When other tests have run before it -> fail
def test_default_state():
    builder: iRODSFSBuilder = iRODSFSBuilder().with_root("/")
    sut = builder.build()

    assert len(list(sut.scandir("/tempZone"))) == 2

    del(sut)
    builder._session.cleanup()


@pytest.mark.parametrize("path, expected", [
    ["/", 1], ["/tempZone", 4]
])
def test_scandir(fs: iRODSFS, path: str, expected: int):
    actual = list(fs.scandir(path))
    assert len(actual) == expected
    

@pytest.mark.parametrize("path, expected", [
    ["/tempZone/home", True],
    ["/tempZone/home/rods", True],
    ["/tempZone/existing_file.txt", False],
    ["/tempZone/i_dont_exist", False]
])
def test_isdir(fs: iRODSFS, path: str, expected: bool):
    assert fs.isdir(path) == expected


@pytest.mark.parametrize("path, expected", [
    ["/tempZone/existing_file.txt", True],
    ["/tempZone/existing_collection/existing_file.txt", True],
    ["/tempZone/i_dont_exist", False],
    ["/tempZone/new_collection", False],
    ["/tempZone", False],
])
def test_isfile(fs: iRODSFS, path:str, expected: bool):
    assert fs.isfile(path) == expected


@pytest.mark.parametrize("path", [
    "/tempZone/test", "/tempZone/home/rods/test"
])
def test_makedir(fs: iRODSFS, path:str):
    fs.makedir(path)
    assert fs.isdir(path) == True
    fs.removedir(path)
    assert fs.isdir(path) == False


@pytest.mark.parametrize("path, exception", [
    ["/tempZone/home", DirectoryExists],
    ["/tempZone/test/subcollection", ResourceNotFound]
])
def test_makedir_exceptions(fs:iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.makedir(path)


@pytest.mark.parametrize("path", [
    "/tempZone/test.txt", "/tempZone/home/rods/test.txt"
])
def test_create_remove(fs: iRODSFS, path):
    fs.create(path)
    assert fs.isfile(path) == True
    fs.remove(path)
    assert fs.isfile(path) == False


@pytest.mark.parametrize("path, exception", [
    ["/tempZone/missing_collection/file.txt", ResourceNotFound],
    ["/tempZone/existing_file.txt", FileExists]
])
def test_create_exceptions(fs: iRODSFS, path: str, exception: Exception):
    with pytest.raises(exception):
        fs.create(path)


@pytest.mark.parametrize("path, is_dir", [
    ["/tempZone/home", True],
    ["/tempZone/existing_file.txt", False],
])
def test_get_info(fs: iRODSFS, path: str, is_dir: bool):
    info = fs.getinfo(path)
    
    assert info.name == os.path.basename(path)
    assert info.is_dir == is_dir
    assert info.is_file != is_dir
    
    assert info.modified is not None
    assert info.created is not None
    assert info.accessed is None
    
    if is_dir is False:
        assert info.raw["details"]["type"] is not None
        assert "size" in info.raw["details"]
        assert "checksum" in info.raw["details"]
        assert "comments" in info.raw["details"]
        assert "expiry" in info.raw["details"]
    
    assert info.raw["access"]["user"] is not None


@pytest.mark.parametrize("path, expected", [
    ["/tempZone/home", True],
    ["/tempZone/home/rods", True],
    ["/tempZone/fakedir", False],
    ["/tempZone/home/other_user", False],
    ["/tempZone/existing_file.txt", True],
    ["/tempZone/existing_collection/existing_file.txt", True],
    ["/tempZone/existing_collection/bad_file.txt", False]
])
def test_exists(fs: iRODSFS, path: str, expected: bool):
    assert fs.exists(path) == expected
    assert fs.exists(path) == expected


@pytest.mark.parametrize("path", [
    "/tempZone/foo", "/tempZone/home/rods/test"
])
def test_removedir(fs: iRODSFS, path: str):
    fs.makedir(path)
    assert fs.isdir(path) == True
    fs.removedir(path)
    assert fs.isdir(path) == False


@pytest.mark.parametrize("path, exception", [
    ["", RemoveRootError],
    ["/", RemoveRootError],
    ["/tempZone/existing_file.txt", DirectoryExpected],
    ["/tempZone/home/something", ResourceNotFound],
    ["/tempZone/existing_collection", DirectoryNotEmpty]
])
def test_removedir_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.removedir(path)


@pytest.mark.parametrize("src_path, dst_path, create, preserve_time", [
    ["/tempZone/existing_collection", "/tempZone/home", False, False],
    ["/tempZone/existing_collection", "/tempZone/non_existing_collection", True, False],
    ["/tempZone/existing_collection", "/tempZone/home/existing_collection", True, False],
    ["/tempZone/existing_collection", "/tempZone/new_collection_preserve", True, True],
])
def test_copydir(fs:iRODSFS, src_path: str, dst_path: str, create: bool, preserve_time: bool):
    # Record original modified times if preserve_time is True
    original_modified = None
    if preserve_time:
        src_file = os.path.join(src_path, "existing_file.txt")
        original_info = fs.getinfo(src_file, namespaces=["details"])
        original_modified = original_info.raw["details"]["modified"]
    
    fs.copydir(src_path, dst_path, create, preserve_time=preserve_time)
    result_path = os.path.join(dst_path, os.path.basename(src_path))

    assert fs.isdir(result_path)

    src_entries = list(fs.scandir(src_path))
    dst_entries = list(fs.scandir(result_path))

    # compare names
    assert [e.name for e in src_entries] == [e.name for e in dst_entries]

    # ensure file contents were copied
    for entry in src_entries:
        if entry.is_file:
            src_file = os.path.join(src_path, entry.name)
            dst_file = os.path.join(result_path, entry.name)
            assert fs.readbytes(src_file) == fs.readbytes(dst_file)
            
            # Check preserve_time if enabled
            if preserve_time:
                dst_info = fs.getinfo(dst_file, namespaces=["details"])
                assert dst_info.raw["details"]["modified"] == original_modified
    
    # Clean up result_path and parent if it was created
    fs.removetree(result_path)
    if create and fs.exists(dst_path):
        fs.removetree(dst_path)


@pytest.mark.parametrize("src_path, dst_path, create, exception", [
    ["/tempZone/existing_file.txt", "/", False, DirectoryExpected],
    ["/tempZone/existing_collection", "/tempZone/fakeFolder", False, ResourceNotFound],
    ["/tempZone/fakeFolder", "/tempZone/existing_collection", False, ResourceNotFound]
])
def test_copydir_exceptions(fs: iRODSFS, src_path: str, dst_path: str, create:bool, exception: Exception):
    with pytest.raises(exception):
        fs.copydir(src_path, dst_path, create=create)


def test_copydir_empty_directory(fs: iRODSFS):
    src_empty = "/tempZone/empty_collection_for_copy"
    dst_parent = "/tempZone"

    # create empty source
    if fs.exists(src_empty):
        fs.removetree(src_empty)
    fs.makedirs(src_empty)

    try:
        fs.copydir(src_empty, dst_parent, create=False)
        result_path = os.path.join(dst_parent, os.path.basename(src_empty))
        assert fs.isdir(result_path)
        # copied directory should be empty
        assert fs.isempty(result_path)
    finally:
        if fs.exists(src_empty):
            fs.removetree(src_empty)
        if fs.exists(os.path.join(dst_parent, os.path.basename(src_empty))):
            fs.removetree(os.path.join(dst_parent, os.path.basename(src_empty)))


def test_copydir_overwrite_behavior(fs: iRODSFS):
    src = "/tempZone/existing_collection"
    dst_parent = "/tempZone/copy_dst"
    try:
        fs.makedirs(dst_parent)
        dst_existing = os.path.join(dst_parent, os.path.basename(src))

        # ensure destination exists and contains a differing file
        if not fs.isdir(dst_existing):
            fs.makedirs(dst_existing)
        fs.writetext(os.path.join(dst_existing, "existing_file.txt"), "OLD")

        fs.copydir(src, dst_parent, create=False)
        # copydir uses overwrite=True when copying files
        dst_file = os.path.join(dst_existing, "existing_file.txt")
        assert fs.readtext(dst_file) == "content"
    finally:
        if fs.exists(dst_parent):
            fs.removetree(dst_parent)


def test_copydir_nested_structure(fs: iRODSFS):
    src = "/tempZone/testsrc_nested"
    dst_parent = "/tempZone/nested_dst"
    
    # clean up any existing
    if fs.exists(src):
        fs.removetree(src)
    if fs.exists(dst_parent):
        fs.removetree(dst_parent)

    # build nested structure
    fs.makedirs(os.path.join(src, "a/b"))
    fs.writetext(os.path.join(src, "a", "file1.txt"), "one")
    fs.writetext(os.path.join(src, "a", "b", "file2.txt"), "two")

    try:
        fs.makedirs(dst_parent, recreate=True)
        fs.copydir(src, dst_parent, create=True)
        result = os.path.join(dst_parent, os.path.basename(src))
        assert fs.isdir(os.path.join(result, "a"))
        assert fs.isdir(os.path.join(result, "a", "b"))
        assert fs.readtext(os.path.join(result, "a", "file1.txt")) == "one"
        assert fs.readtext(os.path.join(result, "a", "b", "file2.txt")) == "two"
    finally:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst_parent):
            fs.removetree(dst_parent)


@pytest.mark.parametrize("path, exception", [
    ["/tempZone/home", FileExpected],
    ["/tempZone/some_file.txt", ResourceNotFound]
])
def test_remove_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.remove(path)
    

@pytest.mark.parametrize("path, expected", [
    ["/tempZone", ["/tempZone/existing_file.txt",  "/tempZone/existing_collection", "/tempZone/home", "/tempZone/trash"]],
    ["", ["/tempZone"]],
    ["/tempZone/home", ["/tempZone/home/public", "/tempZone/home/rods"]]
])
def test_listdir(fs: iRODSFS, path: str, expected: list[str]):
    actual = fs.listdir(path)
    assert actual == expected


@pytest.mark.parametrize("path, expected", [
    ["/tempZone/home", False],
    ["/tempZone/home/rods", True]
])
def test_isempty(fs: iRODSFS, path: str, expected: bool):
    assert fs.isempty(path) == expected


@pytest.mark.parametrize("path", [
    "/tempZone/test/subdir"
])
def test_makedirs(fs:iRODSFS, path: str):
    fs.makedirs(path)
    assert fs.isdir(path)
    fs.removedir(path)
    fs.removedir(os.path.dirname(path))
    assert fs.isdir(path) == False
    assert fs.isdir(os.path.dirname(path)) == False


@pytest.mark.parametrize("path, recreate, exception", [
    ["/tempZone/home", False, DirectoryExists],
    ["/tempZone/existing_collection/existing_file.txt/subfolder", False, DirectoryExpected]
])
def test_makedirs_exception(fs: iRODSFS, path:str, recreate: bool, exception: Exception):
    with pytest.raises(exception):
        fs.makedirs(path, recreate = recreate)


def test_removetree(fs: iRODSFS):
    fs.makedirs("/tempZone/test/subdir")
    fs.create("/tempZone/test/subdir/file.txt")
    assert fs.isfile("/tempZone/test/subdir/file.txt")

    fs.removetree("/tempZone/test")
    assert fs.exists("/tempZone/test/subdir/file.txt") == False
    assert fs.exists("/tempZone/test/subdir") == False
    assert fs.exists("/tempZone/test") == False


@pytest.mark.skip
def test_removetree_root(fs: iRODSFS):
    fs.removetree("")
    assert fs.listdir("") == ["/tempZone/trash"]


@pytest.mark.parametrize("path, expected", [
    ["home", "/home"],
    ["", "/"],
    ["/", "/"],
    ["/tempZone/home", "/tempZone/home"],
    ["/tempZone", "/tempZone"]
])
def test_wrap(fs: iRODSFS, path: str, expected:str):
    assert fs.wrap(path) == expected


def test_openbin(fs: iRODSFS):
    f = fs.openbin("/tempZone/home/rods/existing_file.txt", mode="w")
    assert f.writable()
    assert f.closed == False
    f.write("test".encode())
    f.close()
    assert f.closed == True

    f = fs.openbin("/tempZone/home/rods/existing_file.txt", mode="r")
    assert f.readable()
    assert f.readlines() == [b"test"]
    f.close()
    assert f.closed == True

    fs.remove("/tempZone/home/rods/existing_file.txt")
    
    
@pytest.mark.parametrize("path, content, expected", [
    ["/tempZone/empty", b"", 0],
    ["/tempZone/one", b"a", 1],
    ["/tempZone/onethousand", ("b" * 1000).encode("ascii"), 1000]
])
def test_getsize(fs: iRODSFS, path: str, content: bytes, expected: int):
    fs.writebytes(path, content)
    assert fs.getsize(path) == expected    
    fs.remove(path)


def test_getsize_exception(fs: iRODSFS):
    with pytest.raises(ResourceNotFound):
        fs.getsize("doesnotexist")


def test_root_dir(fs:iRODSFS):
    with pytest.raises(FileExpected):
        fs.open("/")
    with pytest.raises(FileExpected):
        fs.openbin("/")


def test_appendbytes(fs: iRODSFS):
    try:
        fs.appendbytes("/tempZone/foo", b"bar")
        assert_bytes(fs, "/tempZone/foo", b"bar")

        fs.appendbytes("/tempZone/foo", b"baz")
        assert_bytes(fs, "/tempZone/foo", b"barbaz")
    finally:
        fs.remove("/tempZone/foo")


def test_appendbytes_typeerror(fs: iRODSFS):
    with pytest.raises(TypeError):
        fs.appendbytes("/tempZone/foo", "bar")


def test_basic(fs: iRODSFS):
    #  Check str and repr don't break
    repr(fs)
    assert isinstance(six.text_type(fs), six.text_type)


def test_getmeta(fs: iRODSFS):
    meta = fs.getmeta()
    assert meta == fs.getmeta(namespace="standard")
    assert isinstance(meta, dict)

    no_meta = fs.getmeta("__nosuchnamespace__")
    assert isinstance(no_meta, dict)
    assert not no_meta


def test_move(fs: iRODSFS):
    fs.writetext("/tempZone/existing_file.txt", "test")
    fs.move("/tempZone/existing_file.txt", "/tempZone/new_file_location.txt")
    
    assert fs.isfile("/tempZone/new_file_location.txt")
    assert fs.readtext("/tempZone/new_file_location.txt") == "test"
    fs.remove("/tempZone/new_file_location.txt")


@pytest.mark.parametrize("source, dest, overwrite, exception", [
    ["/tempZone/non_existing_file", "/tempZone/new_location", False, ResourceNotFound],
    ["/tempZone/home", "/tempZone/somewhere", False, FileExpected],
    ["/tempZone/existing_file.txt", "/tempZone//existing_collection/existing_file.txt", False, DestinationExists]
])
def test_move_exceptions(fs:iRODSFS, source: str, dest: str, overwrite:bool, exception: type):
    with pytest.raises(exception):
        fs.move(source, dest, overwrite=overwrite)

@pytest.mark.parametrize("src_path, dst_path, create_dst, expected_result_path", [
    ["/tempZone/testdir", "/tempZone/newdir", False, "/tempZone/newdir/file.txt"],
    ["/tempZone/testdir", "/tempZone/newdir", True, "/tempZone/newdir/testdir/file.txt"],
])
def test_movedir(fs: iRODSFS, src_path: str, dst_path: str, create_dst: bool, expected_result_path: str):
    if fs.exists(src_path):
        fs.removetree(src_path)
    if fs.exists(dst_path):
        fs.removetree(dst_path)
    
    fs.makedirs(src_path)
    fs.writetext(f"{src_path}/file.txt", "test")
    
    # optionally create destination directory first
    if create_dst:
        fs.makedirs(dst_path)
    
    fs.movedir(src_path, dst_path, overwrite=True)
    
    assert fs.isdir(os.path.dirname(expected_result_path))
    assert fs.isfile(expected_result_path)
    assert fs.readtext(expected_result_path) == "test"
    fs.removetree(dst_path)

@pytest.mark.parametrize("path, content",[
    ["/tempZone/existing_file.txt", "test"]
])
def test_writetext_readtext(fs:iRODSFS, path: str, content: str):
    fs.writetext(path, content)
    assert fs.readtext(path) == content


def test_upload(fs: iRODSFS):
    testfile = os.path.join(os.path.curdir, "tests", "test-data", "test.txt")
    with open(testfile, mode='rb') as file:
        fs.upload("/tempZone/uploaded_file.txt", file)
    assert fs.readtext("/tempZone/uploaded_file.txt") == "Hello World!"
    fs.remove("/tempZone/uploaded_file.txt")


def test_upload_put(fs:iRODSFS):
    testfile = os.path.join(os.path.curdir, "tests", "test-data", "test.txt")
    dst_path = "/tempZone/home/rods/uploaded_file.txt"

    fs.upload(dst_path, testfile)
    assert fs.readtext(dst_path) == "Hello World!"
    fs.remove(dst_path)


def test_download(fs:iRODSFS, tmp_path):
    tmp_file = os.path.join(tmp_path, "downloads.txt")
    with open(tmp_file, mode='wb') as file:
        fs.download("/tempZone/existing_collection/existing_file.txt", file)
    with(open(tmp_file)) as file:
        assert file.read() == "content"


def test_download_get(fs:iRODSFS, tmp_path):
    tmp_file = os.path.join(tmp_path, "downloads.txt")
    fs.download("/tempZone/existing_collection/existing_file.txt", tmp_file)
    with(open(tmp_file)) as file:
        assert file.read() == "content"


@pytest.mark.parametrize("dst_path, result_path, overwrite", [
    ["/tempZone/existing_file_copy.txt", "/tempZone/existing_file_copy.txt", False],
    ["/tempZone/home", "/tempZone/home/existing_file.txt", False],
    ["/tempZone/existing_collection", "/tempZone/existing_collection/existing_file.txt", True],
    ["/tempZone/existing_collection/existing_file.txt", "/tempZone/existing_collection/existing_file.txt", True],
])
def test_copy(fs: iRODSFS, dst_path: str, result_path: str, overwrite: bool):
    src_path = "/tempZone/existing_file.txt"
    fs.copy(src_path, dst_path, overwrite)

    assert fs.exists(result_path)
    assert fs.readbytes(src_path) == fs.readbytes(result_path)

    fs.remove(result_path)


@pytest.mark.parametrize("dst_path, result_path, overwrite, preserve_time", [
    ["/tempZone/existing_file_copy.txt", "/tempZone/existing_file_copy.txt", False, True],
    ["/tempZone/home", "/tempZone/home/existing_file.txt", False, True],
    ["/tempZone/existing_collection", "/tempZone/existing_collection/existing_file.txt", True, True],
    ["/tempZone/existing_collection/existing_file.txt", "/tempZone/existing_collection/existing_file.txt", True, True],
])
def test_copy_preserve_time(fs: iRODSFS, dst_path: str, result_path: str, overwrite: bool, preserve_time: bool):
    src_path = "/tempZone/existing_file.txt"
    fs.copy(src_path, dst_path, overwrite, preserve_time=preserve_time)

    # Get original info
    original_info = fs.getinfo(src_path, namespaces=["details"])
    original_modified = original_info.raw["details"]["modified"]

    # Verify the modification time was updated
    updated_info = fs.getinfo(result_path, namespaces=["details"])
    assert updated_info.raw["details"]["modified"] == original_modified

    fs.remove(result_path)


@pytest.mark.parametrize("src_path, dst_path, overwrite, exception", [
    ["/tempZone/existing_file.txt", "/tempZone/existing_collection", False, DestinationExists],
    ["not_existing.txt", "/tempZone/", False, ResourceNotFound],
    ["/tempZone/existing_collection", "/tempZone/", False, FileExpected],
    ["/tempZone/existing_file.txt", "/tempZone/fakeFolder/existing_file.txt", False, ResourceNotFound],
    ["/tempZone/existing_file.txt", "/tempZone/fakeFolder/test", False, ResourceNotFound]
])
def test_copy_exceptions(fs: iRODSFS, src_path: str, dst_path: str, overwrite: bool, exception: Exception):
    with pytest.raises(exception):
        fs.copy(src_path, dst_path, overwrite)


@pytest.mark.parametrize("path, expected", [
    ["/tempZone/", True],
    ["/", True],
    ["/tempZone/existing_file.txt", True],
    ["existing_file.txt", True],
    ["/tempZone/fakeFolder", True],
    ["/tempZone/fakeFolder/test", False]
])
def test_points_into_collection(fs: iRODSFS, path: str, expected: bool):
    assert fs.points_into_collection(path) == expected


def test_walk(fs: iRODSFS):
    walker = Walker(fs)
    actual: List[WalkResult] = []

    for path, dirs, files in walker.walk(fs, path="/tempZone/home", namespaces=["details"]):
        actual.append(WalkResult(path, dirs, files))

    assert len(actual) == 3
    assert len(actual[0].dirs) == 2


@pytest.mark.parametrize("field, time_offset", [
    ["modified", -600],  # 10 minutes earlier
    ["created", -86400],  # 1 day earlier
])
def test_setinfo_time_fields(fs: iRODSFS, field: str, time_offset: int):
    """Test setting modification and creation times of a file."""
    path = "/tempZone/existing_file.txt"
    
    # Get original info
    original_info = fs.getinfo(path, namespaces=["details"])
    original_time = original_info.raw["details"][field]
    
    # Set a new time
    new_time = original_time + time_offset
    fs.setinfo(path, {"details": {field: new_time}})
    
    # Verify the time was updated
    updated_info = fs.getinfo(path, namespaces=["details"])
    assert updated_info.raw["details"][field] == new_time


@pytest.mark.parametrize("path, exception, field, value", [
    ["/tempZone/nonexistent_file.txt", ResourceNotFound, "modified", 1000000000],
    ["/tempZone/existing_collection", FileExpected, "modified", 1000000000],
])
def test_setinfo_exceptions(fs: iRODSFS, path: str, exception: Exception, field: str, value):
    """Test that setinfo raises appropriate exceptions for invalid inputs."""
    with pytest.raises(exception):
        fs.setinfo(path, {"details": {field: value}})


@pytest.mark.parametrize("field, value", [
    ["modified", -1],  # negative timestamp
    ["created", "not-a-timestamp"],  # non-numeric timestamp
    ["comments", 12345],  # non-string comments
    ["expiry", -1],  # negative expiry
])
def test_setinfo_invalid_values(fs: iRODSFS, field: str, value):
    """Test that setinfo raises ValueError for invalid field values."""
    with pytest.raises(ValueError):
        fs.setinfo("/tempZone/existing_file.txt", {"details": {field: value}})


@pytest.mark.parametrize("field, get_value", [
    ["comments", lambda: "This is a test comment"],
    ["expiry", lambda: int(time.time()) + (30 * 24 * 60 * 60)],  # 30 days from now
])
def test_setinfo_catalog_fields(fs: iRODSFS, field: str, get_value):
    """Test setting catalog fields (comments, expiry)."""
    path = "/tempZone/existing_file.txt"
    value = get_value()
    
    # Set field
    fs.setinfo(path, {"details": {field: value}})
    
    # Verify field was set correctly
    updated_info = fs.getinfo(path, namespaces=["details"])
    if field == "expiry":
        assert int(updated_info.raw["details"][field]) == value
    else:
        assert updated_info.raw["details"][field] == value


def test_setinfo_all_fields(fs: iRODSFS):
    """Test setting multiple catalog fields at once."""
    path = "/tempZone/existing_file.txt"
    
    current_time = int(time.time())
    
    # Set all supported fields
    fs.setinfo(path, {
        "details": {
            "modified": current_time - 600,  # 10 minutes ago
            "created": current_time - 86400,  # 1 day ago
            "comments": "Test file with all catalog",
            "expiry": current_time + (90 * 24 * 60 * 60)  # 90 days from now
        }
    })
    
    # Verify all fields were updated
    updated_info = fs.getinfo(path, namespaces=["details"])
    assert updated_info.raw["details"]["modified"] == current_time - 600
    assert updated_info.raw["details"]["created"] == current_time - 86400
    assert updated_info.raw["details"]["comments"] == "Test file with all catalog"
    assert int(updated_info.raw["details"]["expiry"]) == current_time + (90 * 24 * 60 * 60)


import os
import time
from typing import List
import pytest
import six
from fs.errors import DestinationExists
from fs.errors import DirectoryExists
from fs.errors import DirectoryExpected
from fs.errors import DirectoryNotEmpty
from fs.errors import FileExists
from fs.errors import FileExpected
from fs.errors import RemoveRootError
from fs.errors import ResourceNotFound
from fs.walk import Walker
from fs_irods import iRODSFS
from tests.iRODSFSBuilder import iRODSFSBuilder
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
    assert isinstance(data, bytes)


@pytest.fixture
def fs():
    builder: iRODSFSBuilder = iRODSFSBuilder()
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

    del sut
    builder._session.cleanup()


def test_default_state():
    builder: iRODSFSBuilder = iRODSFSBuilder().with_root("/")
    sut = builder.build()

    assert len(list(sut.scandir("/tempZone"))) == 2

    del sut
    builder._session.cleanup()


@pytest.mark.parametrize("path, expected", [["/", 1], ["/tempZone", 4]])
def test_scandir(fs: iRODSFS, path: str, expected: int):
    actual = list(fs.scandir(path))
    assert len(actual) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ["/tempZone/home", True],
        ["/tempZone/home/rods", True],
        ["/tempZone/existing_file.txt", False],
        ["/tempZone/i_dont_exist", False],
    ],
)
def test_isdir(fs: iRODSFS, path: str, expected: bool):
    assert fs.isdir(path) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ["/tempZone/existing_file.txt", True],
        ["/tempZone/existing_collection/existing_file.txt", True],
        ["/tempZone/i_dont_exist", False],
        ["/tempZone/new_collection", False],
        ["/tempZone", False],
    ],
)
def test_isfile(fs: iRODSFS, path: str, expected: bool):
    assert fs.isfile(path) == expected


@pytest.mark.parametrize("path", ["/tempZone/test", "/tempZone/home/rods/test"])
def test_makedir(fs: iRODSFS, path: str):
    fs.makedir(path)
    assert fs.isdir(path) is True
    fs.removedir(path)
    assert fs.isdir(path) is False


@pytest.mark.parametrize(
    "path, exception", [["/tempZone/home", DirectoryExists], ["/tempZone/test/subcollection", ResourceNotFound]]
)
def test_makedir_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.makedir(path)


@pytest.mark.parametrize("path", ["/tempZone/test.txt", "/tempZone/home/rods/test.txt"])
def test_create_remove(fs: iRODSFS, path):
    fs.create(path)
    assert fs.isfile(path) is True
    fs.remove(path)
    assert fs.isfile(path) is False


@pytest.mark.parametrize(
    "path, exception",
    [["/tempZone/missing_collection/file.txt", ResourceNotFound], ["/tempZone/existing_file.txt", FileExists]],
)
def test_create_exceptions(fs: iRODSFS, path: str, exception: Exception):
    with pytest.raises(exception):
        fs.create(path)


@pytest.mark.parametrize(
    "path, is_dir",
    [
        ["/tempZone/home", True],
        ["/tempZone/existing_file.txt", False],
    ],
)
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


@pytest.mark.parametrize(
    "path, expected",
    [
        ["/tempZone/home", True],
        ["/tempZone/home/rods", True],
        ["/tempZone/fakedir", False],
        ["/tempZone/home/other_user", False],
        ["/tempZone/existing_file.txt", True],
        ["/tempZone/existing_collection/existing_file.txt", True],
        ["/tempZone/existing_collection/bad_file.txt", False],
    ],
)
def test_exists(fs: iRODSFS, path: str, expected: bool):
    assert fs.exists(path) == expected
    assert fs.exists(path) == expected


@pytest.mark.parametrize("path", ["/tempZone/foo", "/tempZone/home/rods/test"])
def test_removedir(fs: iRODSFS, path: str):
    fs.makedir(path)
    assert fs.isdir(path) is True
    fs.removedir(path)
    assert fs.isdir(path) is False


@pytest.mark.parametrize(
    "path, exception",
    [
        ["", RemoveRootError],
        ["/", RemoveRootError],
        ["/tempZone/existing_file.txt", DirectoryExpected],
        ["/tempZone/home/something", ResourceNotFound],
        ["/tempZone/existing_collection", DirectoryNotEmpty],
    ],
)
def test_removedir_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.removedir(path)


@pytest.mark.parametrize(
    "src_path, dst_path, create, preserve_time",
    [
        ["/tempZone/existing_collection", "/tempZone/home", False, False],
        ["/tempZone/existing_collection", "/tempZone/non_existing_collection", True, False],
        ["/tempZone/existing_collection", "/tempZone/home/existing_collection", True, False],
        ["/tempZone/existing_collection", "/tempZone/new_collection_preserve", True, True],
    ],
)
def test_copydir(fs: iRODSFS, src_path: str, dst_path: str, create: bool, preserve_time: bool):
    fs.copydir(src_path, dst_path, create, preserve_time=preserve_time)
    result_path = os.path.join(dst_path, os.path.basename(src_path))
    assert fs.isdir(result_path)

    src_entries = list(fs.scandir(src_path))
    dst_entries = list(fs.scandir(result_path))
    assert sorted(e.name for e in src_entries) == sorted(e.name for e in dst_entries)

    for entry in src_entries:
        if entry.is_file:
            src_file = os.path.join(src_path, entry.name)
            dst_file = os.path.join(result_path, entry.name)
            assert fs.readbytes(src_file) == fs.readbytes(dst_file)
            if preserve_time:
                _assert_preserve_time(fs, src_file, dst_file)
        elif entry.is_dir:
            src_dir = os.path.join(src_path, entry.name)
            dst_dir = os.path.join(result_path, entry.name)
            if preserve_time:
                _assert_preserve_time(fs, src_dir, dst_dir)

    if preserve_time:
        _assert_preserve_time(fs, src_path, result_path)

    fs.removetree(result_path)
    if create and fs.exists(dst_path):
        fs.removetree(dst_path)


def _assert_preserve_time(fs: iRODSFS, src_path: str, dst_path: str):
    src_info = fs.getinfo(src_path, namespaces=["details"])
    dst_info = fs.getinfo(dst_path, namespaces=["details"])
    assert dst_info.raw["details"]["modified"] == src_info.raw["details"]["modified"]


@pytest.mark.parametrize(
    "src_path, dst_path, create, exception",
    [
        ["/tempZone/existing_file.txt", "/", False, DirectoryExpected],
        ["/tempZone/existing_collection", "/tempZone/fakeFolder", False, ResourceNotFound],
        ["/tempZone/fakeFolder", "/tempZone/existing_collection", False, ResourceNotFound],
    ],
)
def test_copydir_exceptions(fs: iRODSFS, src_path: str, dst_path: str, create: bool, exception: Exception):
    with pytest.raises(exception):
        fs.copydir(src_path, dst_path, create=create)


def test_copydir_empty_directory(fs: iRODSFS):
    src_empty = "/tempZone/empty_collection_for_copy"
    dst_parent = "/tempZone"

    if fs.exists(src_empty):
        fs.removetree(src_empty)
    fs.makedirs(src_empty)

    try:
        fs.copydir(src_empty, dst_parent, create=False)
        result_path = os.path.join(dst_parent, os.path.basename(src_empty))
        assert fs.isdir(result_path)
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

        if not fs.isdir(dst_existing):
            fs.makedirs(dst_existing)
        fs.writetext(os.path.join(dst_existing, "existing_file.txt"), "old content")

        fs.copydir(src, dst_parent, create=False)

        dst_file = os.path.join(dst_existing, "existing_file.txt")
        assert fs.readtext(dst_file) == "content"
    finally:
        if fs.exists(dst_parent):
            fs.removetree(dst_parent)


@pytest.mark.parametrize(
    "src_path, dst_parent, create, preserve_time",
    [
        ["/tempZone/testsrc_nested", "/tempZone/nested_dst", True, False],
        ["/tempZone/testsrc_nested", "/tempZone/nested_dst", True, True],
    ],
)
def test_copydir_nested_structure(fs: iRODSFS, src_path: str, dst_parent: str, create: bool, preserve_time: bool):
    if fs.exists(src_path):
        fs.removetree(src_path)
    if fs.exists(dst_parent):
        fs.removetree(dst_parent)

    fs.makedirs(os.path.join(src_path, "a/b"))
    fs.writetext(os.path.join(src_path, "a", "file1.txt"), "one")
    fs.writetext(os.path.join(src_path, "a", "b", "file2.txt"), "two")

    try:
        fs.copydir(src_path, dst_parent, create=create, preserve_time=preserve_time)
        result = os.path.join(dst_parent, os.path.basename(src_path))
        assert fs.isdir(os.path.join(result, "a"))
        assert fs.isdir(os.path.join(result, "a", "b"))
        assert fs.readtext(os.path.join(result, "a", "file1.txt")) == "one"
        assert fs.readtext(os.path.join(result, "a", "b", "file2.txt")) == "two"
        if preserve_time:
            _assert_preserve_time(fs, src_path, result)
            _assert_preserve_time(fs, os.path.join(src_path, "a", "file1.txt"), os.path.join(result, "a", "file1.txt"))
            _assert_preserve_time(fs, os.path.join(src_path, "a", "b"), os.path.join(result, "a", "b"))
            _assert_preserve_time(
                fs, os.path.join(src_path, "a", "b", "file2.txt"), os.path.join(result, "a", "b", "file2.txt")
            )
    finally:
        if fs.exists(src_path):
            fs.removetree(src_path)
        if fs.exists(dst_parent):
            fs.removetree(dst_parent)


@pytest.mark.parametrize(
    "path, exception", [["/tempZone/home", FileExpected], ["/tempZone/some_file.txt", ResourceNotFound]]
)
def test_remove_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.remove(path)


@pytest.mark.parametrize(
    "path, expected",
    [
        [
            "/tempZone",
            ["/tempZone/existing_file.txt", "/tempZone/existing_collection", "/tempZone/home", "/tempZone/trash"],
        ],
        ["", ["/tempZone"]],
        ["/tempZone/home", ["/tempZone/home/public", "/tempZone/home/rods"]],
    ],
)
def test_listdir(fs: iRODSFS, path: str, expected: list[str]):
    actual = fs.listdir(path)
    assert actual == expected


@pytest.mark.parametrize("path, expected", [["/tempZone/home", False], ["/tempZone/home/rods", True]])
def test_isempty(fs: iRODSFS, path: str, expected: bool):
    assert fs.isempty(path) == expected


@pytest.mark.parametrize("path", ["/tempZone/test/subdir"])
def test_makedirs(fs: iRODSFS, path: str):
    fs.makedirs(path)
    assert fs.isdir(path)
    fs.removedir(path)
    fs.removedir(os.path.dirname(path))
    assert fs.isdir(path) is False
    assert fs.isdir(os.path.dirname(path)) is False


@pytest.mark.parametrize(
    "path, recreate, exception",
    [
        ["/tempZone/home", False, DirectoryExists],
        ["/tempZone/existing_collection/existing_file.txt/subfolder", False, DirectoryExpected],
    ],
)
def test_makedirs_exception(fs: iRODSFS, path: str, recreate: bool, exception: Exception):
    with pytest.raises(exception):
        fs.makedirs(path, recreate=recreate)


def test_removetree(fs: iRODSFS):
    fs.makedirs("/tempZone/test/subdir")
    fs.create("/tempZone/test/subdir/file.txt")
    assert fs.isfile("/tempZone/test/subdir/file.txt")

    fs.removetree("/tempZone/test")
    assert fs.exists("/tempZone/test/subdir/file.txt") is False
    assert fs.exists("/tempZone/test/subdir") is False
    assert fs.exists("/tempZone/test") is False


@pytest.mark.skip
def test_removetree_root(fs: iRODSFS):
    fs.removetree("")
    assert fs.listdir("") == ["/tempZone/trash"]


@pytest.mark.parametrize(
    "path, expected",
    [["home", "/home"], ["", "/"], ["/", "/"], ["/tempZone/home", "/tempZone/home"], ["/tempZone", "/tempZone"]],
)
def test_wrap(fs: iRODSFS, path: str, expected: str):
    assert fs.wrap(path) == expected


def test_openbin(fs: iRODSFS):
    f = fs.openbin("/tempZone/home/rods/existing_file.txt", mode="w")
    assert f.writable()
    assert f.closed is False
    f.write("test".encode())
    f.close()
    assert f.closed is True

    f = fs.openbin("/tempZone/home/rods/existing_file.txt", mode="r")
    assert f.readable()
    assert f.readlines() == [b"test"]
    f.close()
    assert f.closed is True

    fs.remove("/tempZone/home/rods/existing_file.txt")


@pytest.mark.parametrize(
    "path, content, expected",
    [
        ["/tempZone/empty", b"", 0],
        ["/tempZone/one", b"a", 1],
        ["/tempZone/onethousand", ("b" * 1000).encode("ascii"), 1000],
    ],
)
def test_getsize(fs: iRODSFS, path: str, content: bytes, expected: int):
    fs.writebytes(path, content)
    assert fs.getsize(path) == expected
    fs.remove(path)


def test_getsize_exception(fs: iRODSFS):
    with pytest.raises(ResourceNotFound):
        fs.getsize("doesnotexist")


def test_root_dir(fs: iRODSFS):
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


@pytest.mark.parametrize(
    "source, dest, overwrite, exception",
    [
        ["/tempZone/non_existing_file", "/tempZone/new_location", False, ResourceNotFound],
        ["/tempZone/home", "/tempZone/somewhere", False, FileExpected],
        ["/tempZone/existing_file.txt", "/tempZone//existing_collection/existing_file.txt", False, DestinationExists],
    ],
)
def test_move_exceptions(fs: iRODSFS, source: str, dest: str, overwrite: bool, exception: type):
    with pytest.raises(exception):
        fs.move(source, dest, overwrite=overwrite)


@pytest.mark.parametrize(
    "src_path, dst_path, overwrite, preserve_time",
    [
        ["/tempZone/movedir_basic_src1", "/tempZone/movedir_basic_dst1", False, False],
        ["/tempZone/movedir_basic_src2", "/tempZone/movedir_basic_dst2/subdir", False, False],
    ],
)
def test_movedir_basic(fs: iRODSFS, src_path: str, dst_path: str, overwrite: bool, preserve_time: bool):
    try:
        if fs.exists(src_path):
            fs.removetree(src_path)
        if fs.exists(dst_path):
            fs.removetree(dst_path)

        fs.makedirs(src_path)
        fs.writetext(f"{src_path}/file.txt", "test content")
        if not fs.isdir(os.path.dirname(dst_path)):
            fs.makedirs(os.path.dirname(dst_path))

        fs.movedir(src_path, dst_path, overwrite=overwrite, preserve_time=preserve_time)

        assert not fs.exists(src_path), "Source directory should be removed after move"
        assert fs.isdir(dst_path), "Destination directory should exist"
        assert fs.isfile(f"{dst_path}/file.txt"), "File should exist in destination"
        assert fs.readtext(f"{dst_path}/file.txt") == "test content"
    finally:
        if fs.exists(dst_path):
            fs.removetree(dst_path)
        if fs.exists(src_path):
            fs.removetree(src_path)

        # Clean up any created parent directories
        dst_parent = os.path.dirname(dst_path)
        if dst_parent != "/tempZone" and fs.exists(dst_parent):
            fs.removedir(dst_parent)


def test_movedir_nested_structure(fs: iRODSFS):
    src = "/tempZone/movedir_src_nested"
    dst = "/tempZone/movedir_dst_nested"

    try:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst):
            fs.removetree(dst)
        fs.makedirs(os.path.join(src, "a", "b", "c"))
        fs.writetext(os.path.join(src, "file1.txt"), "nested file 1")
        fs.writetext(os.path.join(src, "a", "file2.txt"), "nested file 2")
        fs.writetext(os.path.join(src, "a", "b", "file3.txt"), "nested file 3")
        fs.writetext(os.path.join(src, "a", "b", "c", "file4.txt"), "nested file 4")

        fs.movedir(src, dst, overwrite=True)

        assert not fs.exists(src), "Source should be removed"
        assert fs.isdir(dst)
        assert fs.isdir(os.path.join(dst, "a", "b", "c"))
        assert fs.readtext(os.path.join(dst, "file1.txt")) == "nested file 1"
        assert fs.readtext(os.path.join(dst, "a", "file2.txt")) == "nested file 2"
        assert fs.readtext(os.path.join(dst, "a", "b", "file3.txt")) == "nested file 3"
        assert fs.readtext(os.path.join(dst, "a", "b", "c", "file4.txt")) == "nested file 4"
    finally:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst):
            fs.removetree(dst)


def test_movedir_overwrite_existing(fs: iRODSFS):
    src = "/tempZone/movedir_src_overwrite"
    dst = "/tempZone/movedir_dst_overwrite"

    try:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst):
            fs.removetree(dst)

        fs.makedirs(src)
        fs.writetext(os.path.join(src, "new_file.txt"), "new content")
        fs.makedirs(dst)
        fs.writetext(os.path.join(dst, "old_file.txt"), "old content")
        fs.movedir(src, dst, overwrite=True)

        assert not fs.exists(src)
        assert fs.isdir(dst)
        assert fs.isfile(os.path.join(dst, "new_file.txt"))
        assert fs.readtext(os.path.join(dst, "new_file.txt")) == "new content"
    finally:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst):
            fs.removetree(dst)


@pytest.mark.parametrize(
    "src_path, dst_path, overwrite, exception",
    [
        ["/tempZone/existing_file.txt", "/tempZone/somewhere", False, DirectoryExpected],
        ["/tempZone/nonexistent_dir", "/tempZone/somewhere", False, ResourceNotFound],
        ["/tempZone/existing_collection", "/tempZone/home", False, DestinationExists],
    ],
)
def test_movedir_exceptions(fs: iRODSFS, src_path: str, dst_path: str, overwrite: bool, exception: type):
    with pytest.raises(exception):
        fs.movedir(src_path, dst_path, overwrite=overwrite)


def test_movedir_preserve_time_nested(fs: iRODSFS):
    src = "/tempZone/movedir_preserve_nested"
    dst = "/tempZone/movedir_preserve_nested_dst"

    try:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst):
            fs.removetree(dst)

        fs.makedirs(os.path.join(src, "subdir"))
        fs.writetext(os.path.join(src, "file.txt"), "content")
        fs.writetext(os.path.join(src, "subdir", "nested_file.txt"), "nested content")

        src_info = fs.getinfo(src, namespaces=["details"])
        src_root_modified = src_info.raw["details"]["modified"]
        file_info = fs.getinfo(os.path.join(src, "file.txt"), namespaces=["details"])
        file_modified = file_info.raw["details"]["modified"]
        subdir_info = fs.getinfo(os.path.join(src, "subdir"), namespaces=["details"])
        subdir_modified = subdir_info.raw["details"]["modified"]
        nested_file_info = fs.getinfo(os.path.join(src, "subdir", "nested_file.txt"), namespaces=["details"])
        nested_file_modified = nested_file_info.raw["details"]["modified"]

        fs.movedir(src, dst, overwrite=True, preserve_time=True)

        dst_info = fs.getinfo(dst, namespaces=["details"])
        assert (
            dst_info.raw["details"]["modified"] == src_root_modified
        ), "Root directory modification time not preserved"
        dst_file_info = fs.getinfo(os.path.join(dst, "file.txt"), namespaces=["details"])
        assert dst_file_info.raw["details"]["modified"] == file_modified, "File modification time not preserved"
        dst_subdir_info = fs.getinfo(os.path.join(dst, "subdir"), namespaces=["details"])
        assert (
            dst_subdir_info.raw["details"]["modified"] == subdir_modified
        ), "Nested directory modification time not preserved"
        dst_nested_file_info = fs.getinfo(os.path.join(dst, "subdir", "nested_file.txt"), namespaces=["details"])
        assert (
            dst_nested_file_info.raw["details"]["modified"] == nested_file_modified
        ), "Nested file modification time not preserved"
    finally:
        if fs.exists(src):
            fs.removetree(src)
        if fs.exists(dst):
            fs.removetree(dst)


@pytest.mark.parametrize("path, content", [["/tempZone/existing_file.txt", "test"]])
def test_writetext_readtext(fs: iRODSFS, path: str, content: str):
    fs.writetext(path, content)
    assert fs.readtext(path) == content


def test_upload(fs: iRODSFS):
    testfile = os.path.join(os.path.curdir, "tests", "test-data", "test.txt")
    with open(testfile, mode="rb") as file:
        fs.upload("/tempZone/uploaded_file.txt", file)
    assert fs.readtext("/tempZone/uploaded_file.txt") == "Hello World!"
    fs.remove("/tempZone/uploaded_file.txt")


def test_upload_put(fs: iRODSFS):
    testfile = os.path.join(os.path.curdir, "tests", "test-data", "test.txt")
    dst_path = "/tempZone/home/rods/uploaded_file.txt"

    fs.upload(dst_path, testfile)
    assert fs.readtext(dst_path) == "Hello World!"
    fs.remove(dst_path)


def test_download(fs: iRODSFS, tmp_path):
    tmp_file = os.path.join(tmp_path, "downloads.txt")
    with open(tmp_file, mode="wb") as file:
        fs.download("/tempZone/existing_collection/existing_file.txt", file)
    with open(tmp_file) as file:
        assert file.read() == "content"


def test_download_get(fs: iRODSFS, tmp_path):
    tmp_file = os.path.join(tmp_path, "downloads.txt")
    fs.download("/tempZone/existing_collection/existing_file.txt", tmp_file)
    with open(tmp_file) as file:
        assert file.read() == "content"


@pytest.mark.parametrize(
    "dst_path, result_path, overwrite",
    [
        ["/tempZone/existing_file_copy.txt", "/tempZone/existing_file_copy.txt", False],
        ["/tempZone/home", "/tempZone/home/existing_file.txt", False],
        ["/tempZone/existing_collection", "/tempZone/existing_collection/existing_file.txt", True],
        ["/tempZone/existing_collection/existing_file.txt", "/tempZone/existing_collection/existing_file.txt", True],
    ],
)
def test_copy(fs: iRODSFS, dst_path: str, result_path: str, overwrite: bool):
    src_path = "/tempZone/existing_file.txt"
    fs.copy(src_path, dst_path, overwrite)

    assert fs.exists(result_path)
    assert fs.readbytes(src_path) == fs.readbytes(result_path)

    fs.remove(result_path)


@pytest.mark.parametrize(
    "dst_path, result_path, overwrite, preserve_time",
    [
        ["/tempZone/existing_file_copy.txt", "/tempZone/existing_file_copy.txt", False, True],
        ["/tempZone/home", "/tempZone/home/existing_file.txt", False, True],
        ["/tempZone/existing_collection", "/tempZone/existing_collection/existing_file.txt", True, True],
        [
            "/tempZone/existing_collection/existing_file.txt",
            "/tempZone/existing_collection/existing_file.txt",
            True,
            True,
        ],
    ],
)
def test_copy_preserve_time(fs: iRODSFS, dst_path: str, result_path: str, overwrite: bool, preserve_time: bool):
    src_path = "/tempZone/existing_file.txt"
    fs.copy(src_path, dst_path, overwrite, preserve_time=preserve_time)
    _assert_preserve_time(fs, src_path, result_path)
    fs.remove(result_path)


@pytest.mark.parametrize(
    "src_path, dst_path, overwrite, exception",
    [
        ["/tempZone/existing_file.txt", "/tempZone/existing_collection", False, DestinationExists],
        ["not_existing.txt", "/tempZone/", False, ResourceNotFound],
        ["/tempZone/existing_collection", "/tempZone/", False, FileExpected],
        ["/tempZone/existing_file.txt", "/tempZone/fakeFolder/existing_file.txt", False, ResourceNotFound],
        ["/tempZone/existing_file.txt", "/tempZone/fakeFolder/test", False, ResourceNotFound],
    ],
)
def test_copy_exceptions(fs: iRODSFS, src_path: str, dst_path: str, overwrite: bool, exception: Exception):
    with pytest.raises(exception):
        fs.copy(src_path, dst_path, overwrite)


@pytest.mark.parametrize(
    "path, expected",
    [
        ["/tempZone/", True],
        ["/", True],
        ["/tempZone/existing_file.txt", True],
        ["existing_file.txt", True],
        ["/tempZone/fakeFolder", True],
        ["/tempZone/fakeFolder/test", False],
    ],
)
def test_points_into_collection(fs: iRODSFS, path: str, expected: bool):
    assert fs.points_into_collection(path) == expected


def test_walk(fs: iRODSFS):
    walker = Walker(fs)
    actual: List[WalkResult] = []

    for path, dirs, files in walker.walk(fs, path="/tempZone/home", namespaces=["details"]):
        actual.append(WalkResult(path, dirs, files))

    assert len(actual) == 3
    assert len(actual[0].dirs) == 2


@pytest.mark.parametrize(
    "field, time_offset",
    [
        ["modified", -600],
        ["created", -86400],
    ],
)
def test_setinfo_time_fields(fs: iRODSFS, field: str, time_offset: int):
    """Test setting modification and creation times of a file."""
    path = "/tempZone/existing_file.txt"

    original_info = fs.getinfo(path, namespaces=["details"])
    original_time = original_info.raw["details"][field]

    new_time = original_time + time_offset
    fs.setinfo(path, {"details": {field: new_time}})

    updated_info = fs.getinfo(path, namespaces=["details"])
    assert updated_info.raw["details"][field] == new_time


@pytest.mark.parametrize(
    "path, exception, field, value",
    [
        ["/tempZone/nonexistent_file.txt", ResourceNotFound, "modified", 1000000000],
    ],
)
def test_setinfo_exceptions(fs: iRODSFS, path: str, exception: Exception, field: str, value):
    """Test that setinfo raises appropriate exceptions for invalid inputs."""
    with pytest.raises(exception):
        fs.setinfo(path, {"details": {field: value}})


@pytest.mark.parametrize(
    "field, value",
    [
        ["modified", -1],  # negative timestamp
        ["created", "not-a-timestamp"],  # non-numeric timestamp
        ["comments", 12345],  # non-string comments
        ["expiry", -1],  # negative expiry
    ],
)
def test_setinfo_invalid_values(fs: iRODSFS, field: str, value):
    """Test that setinfo raises ValueError for invalid field values."""
    with pytest.raises(ValueError):
        fs.setinfo("/tempZone/existing_file.txt", {"details": {field: value}})


@pytest.mark.parametrize(
    "field, get_value",
    [
        ["comments", lambda: "This is a test comment"],
        ["expiry", lambda: int(time.time()) + (30 * 24 * 60 * 60)],  # 30 days from now
    ],
)
def test_setinfo_catalog_fields(fs: iRODSFS, field: str, get_value):
    """Test setting catalog fields (comments, expiry)."""
    path = "/tempZone/existing_file.txt"
    value = get_value()

    fs.setinfo(path, {"details": {field: value}})

    updated_info = fs.getinfo(path, namespaces=["details"])
    if field == "expiry":
        assert int(updated_info.raw["details"][field]) == value
    else:
        assert updated_info.raw["details"][field] == value


def test_setinfo_all_fields(fs: iRODSFS):
    """Test setting multiple catalog fields at once."""
    path = "/tempZone/existing_file.txt"

    current_time = int(time.time())

    fs.setinfo(
        path,
        {
            "details": {
                "modified": current_time - 600,
                "created": current_time - 86400,
                "comments": "Test file with all catalog",
                "expiry": current_time + (90 * 24 * 60 * 60),  # 90 days
            }
        },
    )

    updated_info = fs.getinfo(path, namespaces=["details"])
    assert updated_info.raw["details"]["modified"] == current_time - 600
    assert updated_info.raw["details"]["created"] == current_time - 86400
    assert updated_info.raw["details"]["comments"] == "Test file with all catalog"
    assert int(updated_info.raw["details"]["expiry"]) == current_time + (90 * 24 * 60 * 60)

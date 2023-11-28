import time
import pytest
import os

from fs_irods import iRODSFS
from unittest.mock import patch
from tests.DelayedSession import DelayedSession
from tests.iRODSFSBuilder import iRODSFSBuilder

from fs.errors import *
import six

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
def fs() -> iRODSFS:
    builder: iRODSFSBuilder = iRODSFSBuilder()
    sut = builder.build()

    if not sut.exists("existing_file.txt"):
        sut.create("existing_file.txt")
    if not sut.exists("existing_collection"):
        sut.makedir("existing_collection")
    if not sut.exists("existing_collection/existing_file.txt"):
        sut.create("existing_collection/existing_file.txt")

    yield sut

    sut.removetree("existing_collection")
    sut.remove("existing_file.txt")
    del(sut)
    builder._session.cleanup()


@pytest.mark.parametrize("path, expected", [
    ["home", True],
    ["home/rods", True],
    ["existing_file.txt", False],
    ["i_dont_exist", False]
])
def test_isdir(fs: iRODSFS, path: str, expected: bool):
    assert fs.isdir(path) == expected


@pytest.mark.parametrize("path", [
    "test", "home/rods/test"
])
def test_makedir(fs: iRODSFS, path:str):
    fs.makedir(path)
    assert fs.isdir(path) == True
    fs.removedir(path)
    assert fs.isdir(path) == False

@pytest.mark.parametrize("path, exception", [
    ["home", DirectoryExists],
    ["test/subcollection", ResourceNotFound]
])
def test_makedir_exceptions(fs:iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.makedir(path)


@pytest.mark.parametrize("path", [
    "test.txt", "home/rods/test.txt"
])
def test_create_remove(fs: iRODSFS, path):
    fs.create(path)
    assert fs.isfile(path) == True
    fs.remove(path)
    assert fs.isfile(path) == False


@pytest.mark.parametrize("path", [
    "test.txt", "/home/rods/test.txt"
])
def test_create_fileexists(fs: iRODSFS, path: str):
    fs.create(path)
    with pytest.raises(FileExists) as e:
        fs.create(path)
    fs.remove(path)


@pytest.mark.parametrize("path", [
    "/missing_collection/file.txt"
])
def test_create_resourcenotfound(fs: iRODSFS, path: str):
    with pytest.raises(ResourceNotFound):
        fs.create(path)


def test_get_info(fs: iRODSFS):
    info = fs.getinfo("home")
    assert info.name == "home"
    assert info.is_dir == True
    assert info.is_file == False
    assert info.modified is not None
    assert info.created is not None
    assert info.accessed  is None


@pytest.mark.parametrize("path, expected", [
    ["home", True],
    ["home/rods", True],
    ["fakedir", False],
    ["home/other_user", False],
    ["existing_file.txt", True],
    ["existing_collection/existing_file.txt", True],
    ["existing_collection/bad_file.txt", False]
])
def test_exists(fs: iRODSFS, path: str, expected: bool):
    assert fs.exists(path) == expected


@pytest.mark.parametrize("path", [
    "foo", "home/rods/test"
])
def test_removedir(fs: iRODSFS, path: str):
    fs.makedir(path)
    assert fs.isdir(path) == True
    fs.removedir(path)
    assert fs.isdir(path) == False


@pytest.mark.parametrize("path, exception", [
    ["", RemoveRootError],
    ["/", RemoveRootError],
    ["existing_file.txt", DirectoryExpected],
    ["home/something", ResourceNotFound],
    ["existing_collection", DirectoryNotEmpty]
])
def test_removedir_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.removedir(path)


@pytest.mark.parametrize("path, exception", [
    ["home", FileExpected],
    ["some_file.txt", ResourceNotFound]
])
def test_remove_exceptions(fs: iRODSFS, path: str, exception: type):
    with pytest.raises(exception):
        fs.remove(path)
    

@pytest.mark.parametrize("path, expected", [
    ["/", ["/tempZone/existing_file.txt",  "/tempZone/existing_collection", "/tempZone/home", "/tempZone/trash", ]],
    ["", ["/tempZone/existing_file.txt",  "/tempZone/existing_collection", "/tempZone/home", "/tempZone/trash"]],
    ["home", ["/tempZone/home/public", "/tempZone/home/rods"]]
])
def test_listdir(fs: iRODSFS, path: str, expected: list[str]):
    actual = fs.listdir(path)
    assert actual == expected

@pytest.mark.parametrize("path, expected", [
    ["home", False],
    ["home/rods", True]
])
def test_isempty(fs: iRODSFS, path: str, expected: bool):
    assert fs.isempty(path) == expected

@pytest.mark.parametrize("path", [
    "test/subdir"
])
def test_makedirs(fs:iRODSFS, path: str):
    fs.makedirs(path)
    assert fs.isdir(path)
    fs.removedir(path)
    fs.removedir(os.path.dirname(path))
    assert fs.isdir(path) == False
    assert fs.isdir(os.path.dirname(path)) == False


def test_removetree(fs: iRODSFS):
    fs.makedirs("test/subdir")
    fs.create("test/subdir/file.txt")
    assert fs.isfile("test/subdir/file.txt")

    fs.removetree("test")
    assert fs.exists("test/subdir/file.txt") == False
    assert fs.exists("test/subdir") == False
    assert fs.exists("test") == False

@pytest.mark.skip
def test_removetree_root(fs: iRODSFS):
    fs.removetree("")
    assert fs.listdir("") == ["/tempZone/trash"]


@pytest.mark.parametrize("path, expected", [
    ["home", "/tempZone/home"],
    ["", "/tempZone"],
    ["/", "/tempZone"],
    ["/tempZone/home", "/tempZone/home"],
    ["/tempZone", "/tempZone"]
])
def test_wrap(fs: iRODSFS, path: str, expected:str):
    assert fs.wrap(path) == expected


def test_openbin(fs: iRODSFS):
    f = fs.openbin("/home/rods/existing_file.txt", mode="w")
    assert f.writable()
    assert f.closed == False
    f.write("test".encode())
    f.close()
    assert f.closed == True

    f = fs.openbin("/home/rods/existing_file.txt", mode="r")
    assert f.readable()
    assert f.readlines() == [b"test"]
    f.close()
    assert f.closed == True

    fs.remove("/home/rods/existing_file.txt")
    

def test_getsize(fs: iRODSFS):
    fs.writebytes("empty", b"")
    fs.writebytes("one", b"a")
    fs.writebytes("onethousand", ("b" * 1000).encode("ascii"))
    assert fs.getsize("empty") == 0
    assert fs.getsize("one") == 1
    assert fs.getsize("onethousand") == 1000

    with pytest.raises(ResourceNotFound):
        fs.getsize("doesnotexist")
    
    fs.remove("empty")
    fs.remove("one")
    fs.remove("onethousand")


def test_root_dir(fs:iRODSFS):
    with pytest.raises(FileExpected):
        fs.open("/")
    with pytest.raises(FileExpected):
        fs.openbin("/")

def test_appendbytes(fs: iRODSFS):
    try:
        fs.appendbytes("foo", b"bar")
        assert_bytes(fs, "foo", b"bar")

        fs.appendbytes("foo", b"baz")
        assert_bytes(fs, "foo", b"barbaz")
    finally:
        fs.remove("foo")

def test_appendbytes_typeerror(fs: iRODSFS):
    with pytest.raises(TypeError):
        fs.appendbytes("foo", "bar")

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
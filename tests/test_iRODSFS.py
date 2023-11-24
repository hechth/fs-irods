import time
import pytest

from fs_irods import iRODSFS
from unittest.mock import patch
from tests.DelayedSession import DelayedSession
from tests.builder_iRODSFS import iRODSFSBuilder

from fs.errors import DirectoryExists, ResourceNotFound, RemoveRootError, DirectoryExpected, FileExpected, FileExists


@patch("fs_irods.iRODSFS.iRODSSession")
def test_enters_session(mocksession):
    sut = iRODSFSBuilder().build()
    sut.listdir("/")
    mocksession.assert_called()


@patch("fs_irods.iRODSFS.iRODSSession", new=DelayedSession)
def test_delayed_session():
    sut = iRODSFSBuilder().build()
    now = time.time()
    sut.listdir("/")
    later = time.time()
    assert later - now > 1


@pytest.fixture
def fs() -> iRODSFS:
    sut = iRODSFSBuilder().build()
    return sut


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
def test_create(fs: iRODSFS, path):
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
    assert info.modified is None
    assert info.created is None
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
    "foo", "foo/bar"
])
def test_removedir(fs: iRODSFS, path: str):
    fs.makedir(path)
    assert fs.isdir(path) == True
    fs.removedir(path)
    assert fs.isdir(path) == False


@pytest.mark.parametrize("path, expected", [
    ["/", ["/tempZone/existing_file.txt",  "/tempZone/existing_collection", "/tempZone/home", "/tempZone/trash", ]],
    ["", ["/tempZone/existing_file.txt",  "/tempZone/existing_collection", "/tempZone/home", "/tempZone/trash"]],
    ["home", ["/tempZone/home/public", "/tempZone/home/rods"]]
])
def test_listdir(fs: iRODSFS, path: str, expected: list[str]):
    actual = fs.listdir(path)
    assert actual == expected

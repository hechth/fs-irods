import time
import pytest

from fs_irods import iRODSFS
from unittest.mock import patch
from tests.DelayedSession import DelayedSession
from tests.builder_iRODSFS import iRODSFSBuilder


@patch("fs_irods.iRODSFS.iRODSSession")
def test_enters_session(mocksession):
    sut = iRODSFSBuilder().build()
    sut.listdir("/")
    mocksession.assert_called()


@pytest.fixture
@patch("fs_irods.iRODSFS.iRODSSession", new=DelayedSession)
def fs() -> iRODSFS:
    sut = iRODSFSBuilder().build()
    return sut



def test_is_dir(fs: iRODSFS):
    assert fs.isdir("home") == True
    assert fs.isdir("birthday.txt") == False
    assert fs.isdir("i_dont_exist") == False


def test_create_dir(fs: iRODSFS):
    fs.makedir("test")
    assert fs.isdir("test") == True
    fs.removedir("test")
    assert fs.isdir("test") == False


def test_create_file(fs: iRODSFS):
    fs.create("test.txt")
    assert fs.isfile("test.txt") == True
    fs.remove("test.txt")
    assert fs.isfile("test.txt") == False

def test_get_info(fs: iRODSFS):
    info = fs.getinfo("home")
    assert info.name == "home"
    assert info.is_dir == True
    assert info.is_file == False
    assert info.modified is None
    assert info.created is None
    assert info.accessed  is None

def test_exists(fs: iRODSFS):
    assert fs.exists("home") == True
    assert fs.exists("i_dont_exist") == False

def test_clean(fs: iRODSFS):
    fs.makedir("test")
    fs.clean()
    assert fs.exists("test") == False
    assert fs.exists("home") == True

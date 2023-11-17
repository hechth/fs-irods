from fs_irods import iRODSFS
from unittest.mock import patch
from tests.builder_iRODSFS import iRODSFSBuilder

def test_default():
    sut = iRODSFS()
    assert sut is not None


@patch("fs_irods.iRODSFS.iRODSSession")
def test_enters_session(mocksession):
    sut = iRODSFSBuilder().build()
    sut.listdir("/")
    mocksession.assert_called_once()
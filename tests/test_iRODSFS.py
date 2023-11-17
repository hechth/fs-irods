from fs_irods import iRODSFS
from unittest.mock import patch
from tests.builder_iRODSFS import iRODSFSBuilder


@patch("fs_irods.iRODSFS.iRODSSession")
def test_enters_session(mocksession):
    sut = iRODSFSBuilder().build()
    sut.listdir("/")
    mocksession.assert_called_once()
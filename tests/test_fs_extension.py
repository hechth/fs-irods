import unittest
import pytest
from fs.test import FSTestCases
from fs_irods import iRODSFS

from tests.iRODSFSBuilder import iRODSFSBuilder


@pytest.mark.skip
class TestMyFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        sut = iRODSFSBuilder().build()
        return sut
    
    def destroy_fs(self, fs: iRODSFS):
        fs.removetree("")

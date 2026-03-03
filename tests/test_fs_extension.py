import time
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
        # fs.removetree("/")
        if fs.exists("foo"):
            if fs.isfile("foo"):
                fs.remove("foo")
            elif fs.isdir("foo"):
                fs.removedir("foo")
        if fs.exists("bar"):
            if fs.isfile("bar"):
                fs.remove("bar")
            elif fs.isdir("bar"):
                fs.removedir("bar")
        time.sleep(0.1)

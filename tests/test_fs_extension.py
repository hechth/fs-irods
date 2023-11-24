import time
import unittest
from unittest.mock import patch
from fs.test import FSTestCases
from tests.DelayedSession import DelayedSession

from tests.builder_iRODSFS import iRODSFSBuilder


class TestMyFS(FSTestCases, unittest.TestCase):

    @patch("fs_irods.iRODSFS.iRODSSession", new=DelayedSession)
    def make_fs(self):
        sut = iRODSFSBuilder().build()
        return sut

    def destroy_fs(self, fs):
        fs.clean()
        self.fs.clean()

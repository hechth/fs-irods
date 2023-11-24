import time
import unittest
from unittest.mock import patch
from fs.test import FSTestCases
from tests.DelayedSession import DelayedSession

from tests.iRODSFSBuilder import iRODSFSBuilder


class TestMyFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        sut = iRODSFSBuilder().build()
        return sut
    
    def destroy_fs(self, fs):
        fs.removetree("")

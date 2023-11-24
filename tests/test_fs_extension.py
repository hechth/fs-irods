import time
import unittest
from unittest.mock import patch
from fs.test import FSTestCases
from tests.DelayedSession import DelayedSession

from tests.builder_iRODSFS import iRODSFSBuilder


class TestMyFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        sut = iRODSFSBuilder().build()
        return sut

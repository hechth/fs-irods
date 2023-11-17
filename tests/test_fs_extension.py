import unittest
from unittest.mock import patch
from fs.test import FSTestCases

from fs_irods import iRODSFS
from tests.builder_iRODSFS import iRODSFSBuilder

from irods.session import iRODSSession

class DummySession(iRODSSession):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

class TestMyFS(FSTestCases, unittest.TestCase):
    def make_fs(self):
        sut = iRODSFSBuilder().build()
        with patch.object(sut, "_session", wraps=DummySession.__enter__):
            return sut
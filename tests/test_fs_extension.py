import unittest
from fs.test import FSTestCases

from fs_irods import iRODSFS

class TestMyFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        # Return an instance of your FS object here
        return iRODSFS()
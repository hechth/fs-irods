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
        """Clean up test artifacts without removing root.
        
        Dynamically removes all items in the zone root that are not system
        paths, so no test artifact is ever left behind regardless of what
        individual tests create.
        """
        SYSTEM_PATHS = {"/tempZone/home", "/tempZone/trash"}
        if not fs.isclosed():
            try:
                for item_path in fs.listdir(""):
                    if item_path in SYSTEM_PATHS:
                        continue
                    try:
                        if fs.isdir(item_path):
                            fs.removetree(item_path)
                        else:
                            fs.remove(item_path)
                    except Exception as e:
                        print(f"WARNING: Failed to remove '{item_path}': {type(e).__name__}: {e}")
            except Exception as e:
                print(f"WARNING: Zone root cleanup failed: {type(e).__name__}: {e}")

        fs.close()

import os
import pytest

from tests.iRODSFSBuilder import iRODSFSBuilder
from fs_irods import iRODSFS


@pytest.fixture
def fs():
    builder = iRODSFSBuilder().with_root("/")
    sut = builder.build()
    yield sut
    # cleanup (leaving deletions to existing test fixtures is fine)
    builder._session.cleanup()


@pytest.mark.parametrize("dst, expected_path", [
    ("/tempZone/preserve_test_file_copied.txt", "/tempZone/preserve_test_file_copied.txt"),
    ("/tempZone/home", "/tempZone/home/preserve_test_file.txt"),
])
def test_copy_preserve_time_file_param(fs: iRODSFS, dst: str, expected_path: str):
    src = "/tempZone/preserve_test_file.txt"

    # ensure source exists
    if not fs.exists(src):
        fs.create(src)
    fs.writetext(src, "content")

    src_info = fs.getinfo(src)

    # precreate dest when dest is a file to test overwrite
    if expected_path != dst and fs.exists(expected_path):
        fs.remove(expected_path)

    fs.copy(src, dst, overwrite=True, preserve_time=True)

    assert fs.exists(expected_path)
    dst_info = fs.getinfo(expected_path)

    assert dst_info.modified == src_info.modified
    assert dst_info.created == src_info.created

    # cleanup
    if fs.exists(expected_path):
        fs.remove(expected_path)
    if fs.exists(src):
        fs.remove(src)


@pytest.mark.parametrize("src_root, files", [
    ("/tempZone/preserve_dir", ["sub/file.txt"]),
    ("/tempZone/preserve_dir_multi", ["a/f1.txt", "a/b/f2.txt", "empty/"])
])
def test_copydir_preserve_time_param(fs: iRODSFS, src_root: str, files: list[str]):
    # setup tree
    for rel in files:
        path = os.path.join(src_root, rel)
        parent = os.path.dirname(path.rstrip('/'))
        if parent and not fs.exists(parent):
            fs.makedirs(parent)

        if rel.endswith("/"):
            # empty directory
            dir_path = path.rstrip('/')
            if not fs.exists(dir_path):
                fs.makedirs(dir_path)
        else:
            if not fs.exists(path):
                fs.create(path)
            fs.writetext(path, "content")

    # record infos
    src_root_info = fs.getinfo(src_root)
    src_file_infos = {rel: fs.getinfo(os.path.join(src_root, rel)) for rel in files if not rel.endswith("/")}

    # copy with preserve_time
    fs.copydir(src_root, "/tempZone/home", create=False, preserve_time=True)

    dst_root = os.path.join("/tempZone/home", os.path.basename(src_root))

    # verify root timestamps
    dst_root_info = fs.getinfo(dst_root)
    assert dst_root_info.modified == src_root_info.modified
    assert dst_root_info.created == src_root_info.created

    # verify file timestamps
    for rel, src_info in src_file_infos.items():
        dst_path = os.path.join(dst_root, rel)
        dst_info = fs.getinfo(dst_path)
        assert dst_info.modified == src_info.modified
        assert dst_info.created == src_info.created

    # cleanup
    if fs.exists(dst_root):
        fs.removetree(dst_root)
    if fs.exists(src_root):
        fs.removetree(src_root)

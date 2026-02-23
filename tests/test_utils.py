import os
from typing import Any, List
import pytest
from fs_irods import can_create

from dataclasses import dataclass
from fs.info import Info

@pytest.mark.parametrize("mode, expected", [
    ["r", False],
    ["w", True],
    ["a", True],
    ["r+", False],
    ["w+", True],
    ["a+", True],
    ["rb", False],
    ["wb", True],
    ["ab", True],
    ["r+b", False],
    ["w+b", True],
    ["a+b", True]
])
def test_can_create(mode: str, expected: bool):
    assert can_create(mode) == expected


@dataclass
class WalkResult:
    path: str
    dirs: List[Info]
    files: List[Info]


def make_destination(src: str,  sub: str, dst: str) -> str:
    """Get the destination path for an object 'sub' in 'src' when copying to 'dst'.

    Args:
        src (str): Source directory to copy.
        sub (str): Path to sub-folder or file somewhere in src.
        dst (str): Path to destination to copy 'src' to.

    Returns:
        str: Path where 'sub' should be located in 'dst' after copying.
    """
    src_basename = os.path.basename(src)
    dst = os.path.join(dst, src_basename)
    rel = os.path.relpath(sub, src)
    return os.path.join(dst, rel)


@pytest.mark.parametrize("src_path, dst_path, file_path, expected", [
    ["/bar", "/foo", "/bar/baz.txt", "/foo/bar/baz.txt"],
    ["/bar", "/foo", "/bar/bar1/baz.txt", "/foo/bar/bar1/baz.txt"],
    ["/bar", "/foo/foo1", "/bar/bar1/baz.txt", "/foo/foo1/bar/bar1/baz.txt"],
    ["/bar/bar1", "/foo/foo1", "/bar/bar1/baz.txt", "/foo/foo1/bar1/baz.txt"],
    ["/foo", "/foo/bar", "/foo/bar/baz.txt", "/foo/bar/foo/bar/baz.txt"],
    ["/foo", "/bar", "/foo/foo1", "/bar/foo/foo1"]
])
def test_get_destination(src_path: str, dst_path: str, file_path: str, expected: str):
    
    actual = make_destination(src_path, file_path, dst_path)
    assert actual == expected
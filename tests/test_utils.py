import pytest
from fs_irods import can_create


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
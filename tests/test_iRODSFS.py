from fs_irods import iRODSFS


def test_default():
    sut = iRODSFS()
    assert sut is not None


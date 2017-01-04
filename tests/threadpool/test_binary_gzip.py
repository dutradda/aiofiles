"""Tests for binary files."""
import io
from os.path import dirname, join
from aiofiles.threadpool import gzip_open as aiogzip_open
import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_iteration(compresslevel):
    """Test iterating over lines from a file."""
    filename = join(dirname(__file__), '..', 'resources', 'multiline_file.txt')

    file = yield from aiogzip_open(filename, mode='rb', compresslevel=compresslevel)

    # Append mode needs us to seek.
    yield from file.seek(0)

    counter = 1

    # The recommended iteration pattern:
    while True:
        line = yield from file.readline()
        if not line:
            break
        assert line.strip() == b'line ' + str(counter).encode()
        counter += 1

    yield from file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_readlines(compresslevel):
    """Test the readlines functionality."""
    filename = join(dirname(__file__), '..', 'resources', 'multiline_file.txt')

    with open(filename, mode='rb') as f:
        expected = f.readlines()

    file = yield from aiogzip_open(filename, mode='rb', compresslevel=compresslevel)

    # Append mode needs us to seek.
    yield from file.seek(0)

    actual = yield from file.readlines()

    yield from file.close()

    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['wb', 'ab'])
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_flush(mode, compresslevel, tmpdir):
    """Test flushing to a file."""
    filename = 'file.bin'

    full_file = tmpdir.join(filename)

    if 'r' in mode:
        full_file.ensure()  # Read modes want it to already exist.

    file = yield from aiogzip_open(str(full_file), mode=mode,
                              compresslevel=compresslevel)

    yield from file.write(b'0')  # Shouldn't flush.

    if compresslevel == -1:
        assert b'' == full_file.read_binary()
    else:
        assert b'0' == full_file.read_binary()

    yield from file.flush()

    assert b'0' == full_file.read_binary()

    yield from file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_read(compresslevel):
    """Just read some bytes from a test file."""
    filename = join(dirname(__file__), '..', 'resources', 'multiline_file.txt')
    file = yield from aiogzip_open(filename, mode='rb', compresslevel=compresslevel)

    yield from file.seek(0)  # Needed for the append mode.

    actual = yield from file.read()

    assert b'' == (yield from file.read())
    assert actual == open(filename, mode='rb').read()

    yield from file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_staggered_read(compresslevel):
    """Read bytes repeatedly."""
    filename = join(dirname(__file__), '..', 'resources', 'multiline_file.txt')
    file = yield from aiogzip_open(filename, mode='rb', compresslevel=compresslevel)

    yield from file.seek(0)  # Needed for the append mode.

    actual = []
    while True:
        byte = yield from file.read(1)
        if byte:
            actual.append(byte)
        else:
            break

    assert b'' == (yield from file.read())

    expected = []
    with open(filename, mode='rb') as f:
        while True:
            byte = f.read(1)
            if byte:
                expected.append(byte)
            else:
                break

    assert actual == expected

    yield from file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_seek(compresslevel, tmpdir):
    """Test seeking and then reading."""
    filename = 'bigfile.bin'
    content = b'0123456789' * 4 * io.DEFAULT_BUFFER_SIZE

    full_file = tmpdir.join(filename)
    full_file.write_binary(content)

    file = yield from aiogzip_open(str(full_file), mode='rb',
                              compresslevel=compresslevel)

    yield from file.seek(4)

    assert b'4' == (yield from file.read(1))

    yield from file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['wb', 'rb', 'ab'])
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_close(mode, compresslevel, tmpdir):
    """Open a file, read a byte, and close it."""
    filename = 'bigfile.bin'
    content = b'0' * 4 * io.DEFAULT_BUFFER_SIZE

    full_file = tmpdir.join(filename)
    full_file.write_binary(content)

    file = yield from aiogzip_open(str(full_file), mode=mode,
                              compresslevel=compresslevel)

    assert not file.closed
    assert not file._file.closed

    yield from file.close()

    assert file.closed
    assert file._file.closed


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_readinto(compresslevel):
    """Test the readinto functionality."""
    filename = join(dirname(__file__), '..', 'resources', 'multiline_file.txt')
    file = yield from aiogzip_open(filename, mode='rb', compresslevel=compresslevel)

    yield from file.seek(0)  # Needed for the append mode.

    array = bytearray(4)
    bytes_read = yield from file.readinto(array)

    assert bytes_read == 4
    assert array == open(filename, mode='rb').read(4)

    yield from file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_truncate(mode, compresslevel, tmpdir):
    """Test truncating files."""
    filename = 'bigfile.bin'
    content = b'0123456789' * 4 * io.DEFAULT_BUFFER_SIZE

    full_file = tmpdir.join(filename)
    full_file.write_binary(content)

    file = yield from aiogzip_open(str(full_file), mode='wb',
                              compresslevel=compresslevel)

    # The append modes want us to seek first.
    yield from file.seek(0)

    if 'w' in mode:
        # We've just erased the entire file.
        yield from file.write(content)
        yield from file.flush()
        yield from file.seek(0)

    yield from file.truncate()

    yield from file.close()

    assert b'' == full_file.read_binary()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['wb', 'ab'])
@pytest.mark.parametrize('compresslevel', [9, 0])
def test_simple_write(mode, compresslevel, tmpdir):
    """Test writing into a file."""
    filename = 'bigfile.bin'
    content = b'0' * 4 * io.DEFAULT_BUFFER_SIZE

    full_file = tmpdir.join(filename)

    if 'r' in mode:
        full_file.ensure()  # Read modes want it to already exist.

    file = yield from aiogzip_open(str(full_file), mode=mode,
                              compresslevel=compresslevel)
    bytes_written = yield from file.write(content)
    yield from file.close()

    assert bytes_written == len(content)
    assert content == full_file.read_binary()


@pytest.mark.asyncio
def test_simple_detach(tmpdir):
    """Test detaching for buffered streams."""
    filename = 'file.bin'

    full_file = tmpdir.join(filename)
    full_file.write_binary(b'0123456789')

    file = yield from aiogzip_open(str(full_file), mode='rb')

    raw_file = file.detach()

    assert raw_file

    with pytest.raises(ValueError):
        yield from file.read()

    assert b'0123456789' == raw_file.read(10)


@pytest.mark.asyncio
def test_simple_readall(tmpdir):
    """Test the readall function by reading a large file in.

    Only RawIOBase supports readall().
    """
    filename = 'bigfile.bin'
    content = b'0' * 4 * io.DEFAULT_BUFFER_SIZE  # Hopefully several reads.

    sync_file = tmpdir.join(filename)
    sync_file.write_binary(content)

    file = yield from aiogzip_open(str(sync_file), mode='rb', compresslevel=0)

    actual = yield from file.readall()

    assert actual == content

    yield from file.close()



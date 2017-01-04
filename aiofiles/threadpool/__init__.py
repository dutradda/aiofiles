"""Handle files using a thread pool executor."""
import asyncio

from io import (FileIO, TextIOBase, BufferedReader, BufferedWriter,
                BufferedRandom)
from functools import partial
from gzip import open as gzip_open, GzipFile
from zipfile import ZipFile, ZipExtFile, ZIP_STORED

from .binary import AsyncBufferedIOBase, AsyncBufferedReader, AsyncFileIO
from .text import AsyncTextIOWrapper
from ..base import AiofilesContextManager
from .._compat import singledispatch, PY_35

_sync_open = open
_sync_gzip_open = gzip_open

def _sync_zip_open(filename, mode, compression, allowZip64):
    zfile = ZipFile(filename, mode, compression, allowZip64)
    return zfile.open(zfile.namelist()[0])


__all__ = ('open', 'gzip_open', 'zip_open')


def open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None,
          closefd=True, opener=None, *, loop=None, executor=None):
    return AiofilesContextManager(_open(file, mode=mode, buffering=buffering,
                                        encoding=encoding, errors=errors,
                                        newline=newline, closefd=closefd,
                                        opener=opener, loop=loop,
                                        executor=executor))


@asyncio.coroutine
def _open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None,
          closefd=True, opener=None, *, loop=None, executor=None):
    """Open an asyncio file."""
    if loop is None:
        loop = asyncio.get_event_loop()
    cb = partial(_sync_open, file, mode=mode, buffering=buffering,
                 encoding=encoding, errors=errors, newline=newline,
                 closefd=closefd, opener=opener)
    f = yield from loop.run_in_executor(executor, cb)

    return wrap(f, loop=loop, executor=executor)


def gzip_open(filename, mode='rb', compresslevel=9, encoding=None,
          errors=None, newline=None, *, loop=None, executor=None):
    return AiofilesContextManager(_gzip_open(filename, mode=mode,
                                         compresslevel=compresslevel,
                                         encoding=encoding, errors=errors,
                                         newline=newline))


@asyncio.coroutine
def _gzip_open(filename, mode='rb', compresslevel=9, encoding=None,
           errors=None, newline=None, *, loop=None, executor=None):
    """Open an asyncio gzip file."""
    if loop is None:
        loop = asyncio.get_event_loop()

    cb = partial(_sync_gzip_open, filename, mode=mode, compresslevel=compresslevel,
                 encoding=encoding, errors=errors, newline=newline)
    f = yield from loop.run_in_executor(executor, cb)

    return wrap(f, loop=loop, executor=executor)


def zip_open(filename, mode='r', compression=ZIP_STORED, allowZip64=True, *,
             loop=None, executor=None):
    return AiofilesContextManager(
        _zip_open(
            filename, mode=mode, compression=compression,
            allowZip64=allowZip64,
            loop=loop, executor=executor
        )
    )


@asyncio.coroutine
def _zip_open(filename, mode='r', compression=ZIP_STORED, allowZip64=True, *,
              loop=None, executor=None):
    """Open an asyncio zip file."""
    if loop is None:
        loop = asyncio.get_event_loop()

    cb = partial(_sync_zip_open, filename, mode=mode,
                 compression=compression, allowZip64=allowZip64)
    f = yield from loop.run_in_executor(executor, cb)

    return wrap(f, loop=loop, executor=executor)


@singledispatch
def wrap(file, *, loop=None, executor=None):
    raise TypeError('Unsupported io type: {}.'.format(file))


@wrap.register(TextIOBase)
def _(file, *, loop=None, executor=None):
    return AsyncTextIOWrapper(file, loop=loop, executor=executor)


@wrap.register(BufferedWriter)
def _(file, *, loop=None, executor=None):
    return AsyncBufferedIOBase(file, loop=loop, executor=executor)


@wrap.register(BufferedReader)
@wrap.register(BufferedRandom)
@wrap.register(GzipFile)
@wrap.register(ZipExtFile)
def _(file, *, loop=None, executor=None):
    return AsyncBufferedReader(file, loop=loop, executor=executor)


@wrap.register(FileIO)
def _(file, *, loop=None, executor=None):
    return AsyncFileIO(file, loop, executor)


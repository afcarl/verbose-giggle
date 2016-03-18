#!/usr/bin/env python
import sys
import re
import os
import io
import struct
from tarfile import BLOCKSIZE, RECORDSIZE
from tarfile import TarInfo
from tarfile import TarFile

NAME = re.compile(r'^[ax](\d+)\.tar')
def name2idx(name):
    m = NAME.match(name)
    if m:
        return int(m.group(1))
    else:
        raise ValueError(name)

def idx2namea(idx):
    return ('a%05d.tar' % idx)

def idx2namex(idx):
    return ('x%05d.tar' % idx)

def rec2bytes(idx, offset):
    return struct.pack('>II', idx, offset)

def bytes2rec(v):
    return struct.unpack('>II', v)


##  TarDB
##
class TarDB:
    
    CATREC_SIZE = 4+4
    
    def __init__(self, basedir, maxsize=10*1024*1024):
        self.basedir = basedir
        self.maxsize = maxsize
        self._catalog = None
        self._files = None
        self._wtar = None
        self._widx = None
        self._rtar = None
        self._ridx = None
        return

    def create(self):
        os.makedirs(self.basedir)
        return

    def open(self):
        path = os.path.join(self.basedir, 'catalog')
        self._catalog = open(path, 'a+b')
        self._files = {}
        for name in os.listdir(self.basedir):
            try:
                idx = name2idx(name)
                self._files[idx] = name
            except ValueError:
                pass
        return

    def close(self):
        self.flush()
        self._catalog.close()
        if self._rtar is not None:
            self._rtar.close()
            self._rtar = None
        self._ridx = None
        if self._wtar is not None:
            self._wtar.close()
            self._wtar = None
        self._widx = None
        return

    def flush(self):
        self._catalog.flush()
        if self._wtar is not None:
            self._wtar.flush()
        return

    def next_recno(self):
        assert self._catalog.tell() % self.CATREC_SIZE == 0
        return self._catalog.tell() // self.CATREC_SIZE

    def _add_catent(self, idx, offset):
        recno = self.next_recno()
        self._catalog.seek(0, io.SEEK_END)
        self._catalog.write(rec2bytes(idx, offset))
        return recno

    def _get_catent(self, recno):
        self._catalog.seek(recno * self.CATREC_SIZE)
        return bytes2rec(self._catalog.read(self.CATREC_SIZE))

    def _open_wtar(self):
        assert self._wtar is None
        if self._files:
            self._widx = max(self._files.keys())
            if not self._files[self._widx].startswith('a'):
                self._widx += 1
                assert self._widx not in self._files
        else:
            self._widx = 0
            assert self._widx not in self._files
        name = idx2namea(self._widx)
        self._files[self._widx] = name
        path = os.path.join(self.basedir, name)
        self._wtar = open(path, 'ab')
        return

    def _close_wtar(self):
        assert self._wtar is not None
        n = self._wtar.tell() % RECORDSIZE
        self._wtar.write(b'\x00'*(RECORDSIZE-n))
        self._wtar.close()
        self._wtar = None
        namea = self._files[self._widx]
        namex = idx2namex(self._widx)
        self._files[self._widx] = namex
        os.rename(os.path.join(self.basedir, namea),
                  os.path.join(self.basedir, namex))
        self._widx = None
        return

    def add_record(self, info, data):
        if self._wtar is None:
            self._open_wtar()
        assert self._wtar is not None
        offset = self._wtar.tell()
        self._wtar.write(info.tobuf())
        self._wtar.write(data)
        n = len(data) % BLOCKSIZE
        if 0 < n:
            self._wtar.write(b'\x00'*(BLOCKSIZE-n))
        recno = self._add_catent(self._widx, offset)
        if self.maxsize < self._wtar.tell():
            self._close_wtar()
        return recno

    def iter_info(self, data=False):
        for recno in range(self.next_recno()):
            yield (recno, self.get_recinfo(recno, data=data))
        return

    def get_recinfo(self, recno, data=False):
        (idx, offset) = self._get_catent(recno)
        if self._ridx != idx:
            if self._rtar is not None:
                self._rtar.close()
            assert idx in self._files
            path = os.path.join(self.basedir, self._files[idx])
            self._rtar = open(path, mode='rb')
            self._ridx = idx
        self._rtar.seek(offset)
        buf = self._rtar.read(BLOCKSIZE)
        info = TarInfo.frombuf(buf, 'utf-8', 'ignore')
        if data:
            b = self._rtar.read(info.size)
        else:
            b = None
        return (info, b)

    def set_recinfo(self, recno, info):
        (idx, offset) = self._get_catent(recno)
        path = os.path.join(self.basedir, self._files[idx])
        with open(path, 'r+b') as tar:
            tar.seek(offset)
            tar.write(info.tobuf())
        return


def main(argv):
    import getopt
    def usage():
        print('usage: %s [-b basedir] cmd [arg ...]' % argv[0])
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'db:')
    except getopt.GetoptError:
        return usage()
    debug = 0
    basedir = 'tar'
    for (k, v) in opts:
        if k == '-d': debug += 1
        elif k == '-b': basedir = v
    tardb = TarDB(basedir)
    if not args: return usage()
    cmd = args.pop(0)
    if cmd == 'create':
        tardb.create()
    elif cmd == 'import':
        tardb.open()
        for path in args:
            tar = TarFile(path)
            while True:
                info = tar.next()
                if info is None: break
                fp = tar.fileobj
                fp.seek(info.offset+BLOCKSIZE)
                data = fp.read(info.size)
                tardb.add_record(info, data)
            tardb.flush()
        tardb.close()
    elif cmd == 'add':
        tardb.open()
        for path in args:
            name = os.path.basename(path)
            info = TarInfo(name)
            with open(path, 'rb') as fp:
                data = fp.read()
            recno = tardb.add_record(info, data)
            print(recno)
        tardb.close()
    elif cmd == 'get':
        tardb.open()
        for recno in args:
            recno = int(recno)
            (_, data) = tardb.get_recinfo(recno, True)
            sys.stdout.buffer.write(data)
        tardb.close()
    elif cmd == 'getinfo':
        tardb.open()
        for recno in args:
            recno = int(recno)
            (info, _) = tardb.get_recinfo(recno, False)
            print(info)
        tardb.close()
    else:
        return usage()
    return 0

if __name__ == '__main__': sys.exit(main(sys.argv))

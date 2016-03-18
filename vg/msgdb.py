#!/usr/bin/env python
import sys
import os
from email import message_from_bytes
from tarfile import BLOCKSIZE
from tarfile import TarInfo
from tarfile import TarFile
from tardb import TarDB
from textdb import TextDB
from utils import gzip2bytes
from utils import bytes2gzip
from utils import html2text
from utils import msg2str
from utils import msg2tags
from utils import cutoff
from utils import rmsp


##  MessageDB
##
class MessageDB:

    MAX_TEXT_SIZE = 100000
    
    def __init__(self, basedir):
        self.basedir = basedir
        self._tar = TarDB(os.path.join(basedir, 'tar'))
        self._text = TextDB(os.path.join(basedir, 'text'))
        return

    def create(self):
        os.makedirs(self.basedir)
        self._tar.create()
        self._text.create()
        return

    def open(self):
        self._tar.open()
        self._text.open()
        return

    def close(self):
        self._tar.close()
        self._text.close()
        return
    
    def flush(self):
        self._tar.flush()
        self._text.flush()
        return

    def add_file(self, data):
        recno = self._tar.next_recno()
        info = TarInfo('%08d' % recno)
        self._tar.add_record(info, bytes2gzip(data))
        msg = message_from_bytes(data)
        text = cutoff(msg2str(msg), self.MAX_TEXT_SIZE)
        self._text.add_text(recno, text)
        for tag in msg2tags(msg):
            self._text.add_tag(recno, tag)
        return recno

    def search_tag(self, tags):
        result = None
        for tag in tags:
            recs = set(self._text.search_tag(tag))
            if result is None:
                result = recs
            else:
                result.update_intersection(recs)
        for recno in sorted(result, reverse=True):
            yield self._text.get_text(recno)
        return
    
    def search_text(self, qs):
        result = None
        for q in qs:
            recs = set(self._text.search_text(q))
            if result is None:
                result = recs
            else:
                result.update_intersection(recs)
        for recno in sorted(result, reverse=True):
            yield self._text.get_text(recno)
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
    basedir = 'msg'
    for (k, v) in opts:
        if k == '-d': debug += 1
        elif k == '-b': basedir = v
    if not args: return usage()
    cmd = args.pop(0)
    msgdb = MessageDB(basedir)
    if cmd == 'create':
        msgdb.create()
    elif cmd == 'import':
        msgdb.open()
        for path in args:
            tar = TarFile(path)
            while True:
                info = tar.next()
                if info is None: break
                fp = tar.fileobj
                fp.seek(info.offset+BLOCKSIZE)
                data = fp.read(info.size)
                recno = msgdb.add_file(gzip2bytes(data))
                print(recno)
            msgdb.flush()
        msgdb.close()
    elif cmd == 'add':
        msgdb.open()
        for path in args:
            with open(path, 'r') as fp:
                data = fp.read()
            recno = msgdb.add_file(data)
            print(recno)
        msgdb.close()
    elif cmd == 'search':
        msgdb.open()
        for data in msgdb.search_text(args):
            print(rmsp(data)[:80])
        msgdb.close()
    else:
        return usage()
    return 0

if __name__ == '__main__': sys.exit(main(sys.argv))

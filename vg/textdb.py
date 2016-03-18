#!/usr/bin/env python
import sys
import os
import sqlite3


##  TextDB
##
class TextDB:
    
    def __init__(self, basedir):
        self.basedir = basedir
        self._conn = None
        self._addcur = None
        return

    def create(self):
        os.makedirs(self.basedir)
        path = os.path.join(self.basedir, 'catalog.sqlite')
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute('CREATE VIRTUAL TABLE message USING fts5(text);')
        cur.execute('CREATE TABLE record (recno INTEGER PRIMARY KEY, msgid INTEGER);')
        cur.execute('CREATE INDEX record_idx on record(msgid);')
        cur.execute('CREATE TABLE tag (recno INTEGER, tag TEXT);')
        cur.execute('CREATE INDEX tag_idx1 on tag(recno);')
        cur.execute('CREATE INDEX tag_idx2 on tag(tag);')
        conn.commit()
        return

    def open(self):
        path = os.path.join(self.basedir, 'catalog.sqlite')
        self._conn = sqlite3.connect(path)
        self._addcur = self._conn.cursor()
        return

    def close(self):
        self.flush()
        self._addcur = None
        self._conn.close()
        self._conn = None
        return

    def flush(self):
        self._conn.commit()
        return

    def add_text(self, recno, data):
        self._addcur.execute('INSERT INTO message VALUES (?);', (data,))
        msgid = self._addcur.lastrowid
        self._addcur.execute('INSERT INTO record VALUES (?, ?);', (recno, msgid))
        return

    def add_tag(self, recno, tag):
        self._addcur.execute('INSERT INTO tag VALUES (?, ?);', (recno, tag))
        return

    def del_tag(self, recno, tag):
        self._addcur.execute('DELETE FROM tag WHERE recno=? AND tag=?;', (recno, tag))
        return

    def get_tags(self, recno):
        cur = self._conn.cursor()
        cur.execute('SELECT tag FROM tag WHERE recno=?;', (recno,))
        for (tag,) in cur.fetchall():
            yield tag
        return

    def get_text(self, recno):
        cur = self._conn.cursor()
        cur.execute('SELECT msgid FROM record WHERE recno=?;', (recno,))
        v = cur.fetchone()
        if v is None: raise KeyError(recno)
        (msgid,) = v
        cur.execute('SELECT text FROM message WHERE rowid=?;', (msgid,))
        (data,) = cur.fetchone()
        return data

    def search_tag(self, tag):
        cur = self._conn.cursor()
        cur.execute('SELECT recno FROM tag WHERE tag=?;', (tag,))
        for (recno,) in cur.fetchall():
            yield recno
        return
    
    def search_text(self, q):
        cur = self._conn.cursor()
        cur.execute('SELECT rowid FROM message WHERE message MATCH ?;', (q,))
        for (msgid,) in cur.fetchall():
            cur.execute('SELECT recno FROM record WHERE msgid=?;', (msgid,))
            (recno,) = cur.fetchone()
            yield recno
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
    basedir = 'text'
    for (k, v) in opts:
        if k == '-d': debug += 1
        elif k == '-b': basedir = v
    if not args: return usage()
    cmd = args.pop(0)
    txtdb = TextDB(basedir)
    if cmd == 'create':
        txtdb.create()
    elif cmd == 'add':
        txtdb.open()
        recno = 1
        for path in args:
            with open(path, 'r') as fp:
                data = fp.read()
            txtdb.add_text(recno, data)
            print(recno)
        txtdb.close()
    elif cmd == 'get':
        txtdb.open()
        for recno in args:
            recno = int(recno)
            data = txtdb.get_text(recno)
            sys.stdout.write(data)
        txtdb.close()
    elif cmd == 'search':
        txtdb.open()
        q = ' '.join(args)
        for recno in txtdb.search_text(q):
            print(recno)
        txtdb.close()
    else:
        return usage()
    return 0

if __name__ == '__main__': sys.exit(main(sys.argv))

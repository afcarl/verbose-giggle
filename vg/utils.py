#!/usr/bin/env python
import io
import re
import gzip
from email.header import decode_header
from email.header import make_header
from email.utils import getaddresses
from html.parser import HTMLParser
from tarfile import BLOCKSIZE
from tarfile import TarInfo
from tarfile import TarFile
from tardb import TarDB
from textdb import TextDB


def gzip2bytes(data):
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as fp:
        data = fp.read()
    return data

def bytes2gzip(data):
    out = io.BytesIO()
    with gzip.GzipFile(mode='w', fileobj=out) as fp:
        fp.write(data)
    return out.getvalue()

class HTMLRipper(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.text = ''
        self._ok = True
        return
    def handle_data(self, data):
        if self._ok:
            self.text += data
        return
    def handle_starttag(self, tag, attrs):
        if tag in ('script','style'):
            self._ok = False
        return
    def handle_endtag(self, tag):
        if tag in ('script','style'):
            self._ok = True
        return
    
def html2text(text):
    p = HTMLRipper()
    p.feed(text)
    return p.text

HEADERS = ['From','To','Cc','Subject','Content-Disposition']
def msg2str(msg, level=2):
    for k in HEADERS:
        for v in msg.get_all(k, []):
            h = make_header(decode_header(v))
            yield '%s: %s' % (k,h)
    if 0 < level:
        if msg.is_multipart():
            if msg.preamble is not None:
                yield msg.preamble
            for part in msg.get_payload():
                for z in msg2str(part, level=level-1):
                    yield z
            if msg.preamble is not None:
                yield msg.preamble
        elif msg.get_content_maintype() in (None,'text','message'):
            t = msg.get_content_subtype()
            charset = msg.get_charset() or 'iso-8859-1'
            data = msg.get_payload(decode=True)
            text = data.decode(charset)
            if t == 'plain':
                yield text
            elif t == 'html':
                yield html2text(text)

def msg2tags(msg):
    values = msg.get_all('From', [])
    for (_,a) in getaddresses(map(str, values)):
        yield 'F:'+a
    values = msg.get_all('To', []) + msg.get_all('Cc', [])
    for (_,a) in getaddresses(map(str, values)):
        yield 'T:'+a
    return

def cutoff(texts, maxlen):
    text = ''
    for s in texts:
        text += s+' '
        if maxlen < len(text): break
    return text[:maxlen]

RMSP = re.compile(r'\s+', re.U)
def rmsp(s):
    return RMSP.sub(' ', s).strip()

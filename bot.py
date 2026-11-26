#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hindi Translate Bot for Telegram  (EPUB + TXT, Chinese or English -> Hindi)
OFFLINE-TRANSLATION edition (v2.0.0)

* The text of your files is translated by a LOCAL model (Argos Translate)
  running on the bot's own machine. Nothing is sent to Google, Bing, Yandex,
  Lingva, MyMemory or LibreTranslate any more.
* After start-up the bot may only talk to api.telegram.org (a network
  allow-list is installed). Set OFFLINE_LOCK=0 to switch that lock off.
* Telegram itself still sees your files, because the bot works through Telegram.
* Needs one setting: BOT_TOKEN (from @BotFather), and:  pip install argostranslate
* The language models (en->hi, zh->en) are downloaded once at start-up.
* Files up to 15 MB. Send a .epub or .txt, get the Hindi file back.
* Live progress bar, queue (/queue), cancel button, final check of every line.
"""
from __future__ import annotations

import collections
import gzip
import html
import http.client
import http.server
import json
import logging
import os
import posixpath
import re
import shutil
import signal
import socket
import ssl
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
import uuid
import zipfile
import zlib

VERSION = "2.0.0-offline"

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
MAX_FILE_BYTES = 15 * 1024 * 1024          # 15 MB input limit
TG_UPLOAD_LIMIT = 49 * 1000 * 1000         # Telegram bots can upload 50 MB
PIECE_MAX = 1000                           # longest text sent to the model at once
MAX_JOBS = max(1, int(os.environ.get("MAX_JOBS", "2")))
MAX_QUEUE = max(1, int(os.environ.get("MAX_QUEUE", "20")))          # total waiting files
MAX_PER_CHAT = max(1, int(os.environ.get("MAX_PER_CHAT", "3")))     # files per user
ZIP_MAX_ENTRIES = 30000
ZIP_MAX_UNPACKED = 600 * 1024 * 1024
TARGET = "hi"
UA = ("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36")

log = logging.getLogger("hindibot")
STOP = threading.Event()
FATAL = threading.Event()          # set when the token is rejected: exit code 1, no auto-restart
MAX_RUNTIME_MIN = float(os.environ.get("MAX_RUNTIME_MIN", "0") or 0)   # 0 = run forever
DRAIN_MIN = float(os.environ.get("DRAIN_MIN", "12") or 12)

# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


class UserError(Exception):
    """Problem with the user's file. Message is shown to the user."""


class Cancelled(Exception):
    pass


class HttpError(Exception):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status


class TelegramError(Exception):
    def __init__(self, code, description, retry_after=None):
        super().__init__(f"{code}: {description}")
        self.code = code
        self.description = description
        self.retry_after = retry_after


# --------------------------------------------------------------------------
# Small HTTP layer (keep-alive, one connection per thread and host).
# Only used to talk to Telegram.
# --------------------------------------------------------------------------
_SSL_CTX = ssl.create_default_context()
_tls = threading.local()


def _conn_for(scheme, host, port, timeout):
    cache = getattr(_tls, "conns", None)
    if cache is None:
        cache = _tls.conns = {}
    key = (scheme, host, port, timeout)
    conn = cache.get(key)
    if conn is None:
        if scheme == "https":
            conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=_SSL_CTX)
        else:
            conn = http.client.HTTPConnection(host, port, timeout=timeout)
        cache[key] = conn
    return conn


def _drop_conn(scheme, host, port, timeout):
    cache = getattr(_tls, "conns", None)
    if not cache:
        return
    conn = cache.pop((scheme, host, port, timeout), None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def close_thread_connections():
    cache = getattr(_tls, "conns", None)
    if cache:
        for conn in list(cache.values()):
            try:
                conn.close()
            except Exception:
                pass
        cache.clear()


def http_request(method, url, headers=None, body=None, timeout=25.0, redirects=3):
    """Returns (status, headers_lowercase, body_bytes). Raises HttpError."""
    for _ in range(redirects + 1):
        sp = urllib.parse.urlsplit(url)
        scheme = (sp.scheme or "").lower()
        if scheme not in ("http", "https") or not sp.hostname:
            raise HttpError("bad url")
        port = sp.port or (443 if scheme == "https" else 80)
        path = sp.path or "/"
        if sp.query:
            path += "?" + sp.query
        hdrs = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip"}
        if headers:
            hdrs.update(headers)
        if isinstance(body, str):
            body = body.encode("utf-8")
        status = None
        rh = {}
        data = b""
        for attempt in range(2):
            conn = _conn_for(scheme, sp.hostname, port, timeout)
            try:
                conn.request(method, path, body=body, headers=hdrs)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
                rh = {k.lower(): v for k, v in resp.getheaders()}
                if resp.will_close:
                    _drop_conn(scheme, sp.hostname, port, timeout)
                break
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError,
                    http.client.CannotSendRequest, http.client.ResponseNotReady,
                    http.client.CannotSendHeader) as exc:
                _drop_conn(scheme, sp.hostname, port, timeout)
                if attempt == 0:
                    continue
                raise HttpError(f"connection lost ({type(exc).__name__})")
            except (socket.timeout, TimeoutError):
                _drop_conn(scheme, sp.hostname, port, timeout)
                raise HttpError("timeout")
            except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                _drop_conn(scheme, sp.hostname, port, timeout)
                raise HttpError(f"{type(exc).__name__}: {exc}")
        if status is None:
            raise HttpError("no response")
        enc = rh.get("content-encoding", "").lower()
        try:
            if enc == "gzip":
                data = gzip.decompress(data)
            elif enc == "deflate":
                data = zlib.decompress(data)
        except (OSError, zlib.error, EOFError):
            raise HttpError("bad compressed body")
        if status in (301, 302, 303, 307, 308) and rh.get("location"):
            url = urllib.parse.urljoin(url, rh["location"])
            if status == 303:
                method, body = "GET", None
            continue
        return status, rh, data
    raise HttpError("too many redirects")


def http_download(url, dest, timeout=120.0, max_bytes=None):
    sp = urllib.parse.urlsplit(url)
    scheme = sp.scheme.lower()
    port = sp.port or (443 if scheme == "https" else 80)
    path = sp.path or "/"
    if sp.query:
        path += "?" + sp.query
    if scheme == "https":
        conn = http.client.HTTPSConnection(sp.hostname, port, timeout=timeout, context=_SSL_CTX)
    else:
        conn = http.client.HTTPConnection(sp.hostname, port, timeout=timeout)
    try:
        conn.request("GET", path, headers={"User-Agent": UA})
        resp = conn.getresponse()
        if resp.status != 200:
            resp.read()
            raise HttpError(f"HTTP {resp.status}", resp.status)
        total = 0
        with open(dest, "wb") as fh:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise HttpError("file too big")
                fh.write(chunk)
        return total
    except HttpError:
        raise
    except (socket.timeout, TimeoutError):
        raise HttpError("timeout")
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        raise HttpError(f"{type(exc).__name__}: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Text helpers
# --------------------------------------------------------------------------
WS_RE = re.compile(r"\s+")
NL_RE = re.compile(r"\r\n|\r|\n")
CJK_RE = re.compile("[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002fa1f]")
DEV_RE = re.compile("[\u0900-\u097f]")
DEV_WORD_RE = re.compile("[\u0900-\u097f]+")
LATIN_WORD_RE = re.compile(r"[A-Za-z]{2,}")
LATIN_LETTER_RE = re.compile(r"[A-Za-z]")
BAD_XML_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\ufffe\uffff]")
SENT_SPLIT_RE = re.compile(r"(?<=[。！？!?；;…])\s*|(?<=[.])\s+")


def split_core(s):
    core = s.strip()
    if not core:
        return s, "", ""
    lead = s[: len(s) - len(s.lstrip())]
    trail = s[len(s.rstrip()):]
    return lead, core, trail


def needs_translation(core):
    letters = sum(1 for ch in core if ch.isalpha())
    if letters == 0:
        return False
    return len(DEV_RE.findall(core)) < 0.5 * letters


def split_pieces(text, maxlen=PIECE_MAX):
    """Cut long text into pieces <= maxlen at sentence ends (or commas / spaces)."""
    if len(text) <= maxlen:
        return [text]
    sents = [x for x in SENT_SPLIT_RE.split(text) if x]
    pieces = []
    cur = ""
    for s in sents:
        while len(s) > maxlen:
            window = s[:maxlen]
            cut = max(window.rfind(" "), window.rfind("，"), window.rfind("、"),
                      window.rfind(","), window.rfind("："), window.rfind(":"))
            cut = cut + 1 if cut >= maxlen // 2 else maxlen
            head, s = s[:cut], s[cut:]
            if cur:
                pieces.append(cur)
                cur = ""
            pieces.append(head.strip() or head)
        if not s:
            continue
        if not cur:
            cur = s
        elif len(cur) + 1 + len(s) <= maxlen:
            cur = cur + " " + s
        else:
            pieces.append(cur)
            cur = s
    if cur:
        pieces.append(cur)
    return [p for p in pieces if p.strip()] or [text]


def clean_result(s):
    s = BAD_XML_RE.sub("", s)
    return WS_RE.sub(" ", s).strip()


def leftover_score(src, out):
    """0 = looks translated. Bigger = worse."""
    out = out.strip()
    if not out:
        return 1.0
    s = src.strip()
    if (len(s) <= 20 and not CJK_RE.search(s)
            and len(LATIN_WORD_RE.findall(s)) <= 2 and out.lower() == s.lower()):
        return 0.0
    score = 0.0
    s_cjk = len(CJK_RE.findall(src))
    o_cjk = len(CJK_RE.findall(out))
    if o_cjk and s_cjk:
        score = max(score, 0.5 + 0.5 * min(1.0, o_cjk / s_cjk))
    if not DEV_RE.search(out):
        score = max(score, 0.45)
    else:
        lw = len(LATIN_WORD_RE.findall(out))
        if lw >= 4:
            dw = len(DEV_WORD_RE.findall(out))
            if lw > dw:
                score = max(score, 0.3 + 0.2 * lw / (lw + dw))
    return score


def final_flag(core):
    """None = fine, 'kept' = short name/word kept, 'problem' = looks untranslated."""
    if not any(ch.isalpha() for ch in core):
        return None
    if CJK_RE.search(core):
        return "problem"
    if not DEV_RE.search(core):
        words = len(LATIN_WORD_RE.findall(core))
        return "kept" if (len(core) <= 30 and words <= 4) else "problem"
    lw = len(LATIN_WORD_RE.findall(core))
    if lw >= 4 and lw > len(DEV_WORD_RE.findall(core)):
        return "problem"
    return None


def detect_source(texts, limit=400000):
    cjk = lat = seen = 0
    for t in texts:
        cjk += len(CJK_RE.findall(t))
        lat += len(LATIN_LETTER_RE.findall(t))
        seen += len(t)
        if seen > limit:
            break
    if cjk == 0 and lat == 0:
        return "auto"
    return "zh" if cjk * 4 >= lat else "en"


def decode_text(raw):
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8", "replace")
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", "replace")
    head = raw[:2048].decode("ascii", "ignore")
    cands = []
    m = (re.search(r"encoding\s*=\s*[\"']([A-Za-z0-9_\-]+)", head)
         or re.search(r"charset\s*=\s*[\"']?([A-Za-z0-9_\-]+)", head, re.I))
    if m:
        cands.append(m.group(1))
    cands += ["utf-8", "gb18030", "big5", "cp1252"]
    fallback = None
    for enc in cands:
        try:
            text = raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
        if enc.lower() in ("gb18030", "gbk", "gb2312") and text:
            pua = sum(1 for ch in text if "\ue000" <= ch <= "\uf8ff")
            if pua / len(text) > 0.002:      # looks like Big5 read as GB: try the next one
                if fallback is None:
                    fallback = text
                continue
        return text
    return fallback if fallback is not None else raw.decode("utf-8", "replace")


def fix_charset(text):
    text = re.sub(r"(<\?xml[^>]*?encoding\s*=\s*[\"'])[^\"']+([\"'])", r"\1utf-8\2",
                  text, count=1, flags=re.I)
    text = re.sub(r"(<meta[^>]*?charset\s*=\s*[\"']?)[A-Za-z0-9_\-]+", r"\1utf-8",
                  text, flags=re.I)
    return text


def fmt_t(sec):
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02}:{s:02}" if h else f"{m:02}:{s:02}"


def fmt_n(n):
    return f"{int(n):,}"


def progress_bar(p, n=20):
    p = min(1.0, max(0.0, p))
    filled = int(p * n + 1e-9)
    return "█" * filled + "░" * (n - filled)


# --------------------------------------------------------------------------
# Segment table and fragments
# --------------------------------------------------------------------------
class SegTable:
    def __init__(self):
        self.index = {}
        self.texts = []

    def add(self, text):
        i = self.index.get(text)
        if i is None:
            i = len(self.texts)
            self.index[text] = i
            self.texts.append(text)
        return i


class Frag:
    __slots__ = ("lead", "trail", "idxs", "esc")

    def __init__(self, lead, trail, idxs, esc):
        self.lead = lead
        self.trail = trail
        self.idxs = idxs
        self.esc = esc

    def render(self, results):
        s = self.lead + " ".join(results[i] for i in self.idxs) + self.trail
        return html.escape(s, quote=False) if self.esc else s


def render_parts(parts, results):
    return "".join(p if isinstance(p, str) else p.render(results) for p in parts)


def make_translate_handler(table, esc=True):
    def handler(raw):
        plain = html.unescape(raw) if (esc and "&" in raw) else raw
        lead, core, trail = split_core(plain)
        if not core or not needs_translation(core):
            return raw
        core = WS_RE.sub(" ", core)
        idxs = [table.add(p) for p in split_pieces(core)]
        return Frag(lead, trail, idxs, esc)
    return handler


def make_check_handler(acc, esc=True):
    def handler(raw):
        plain = html.unescape(raw) if (esc and "&" in raw) else raw
        core = plain.strip()
        if core:
            acc.append(core)
        return raw
    return handler


# --------------------------------------------------------------------------
# HTML / XHTML tokenizer that keeps every byte of markup untouched
# --------------------------------------------------------------------------
TAG_RE = re.compile(
    r"<!--.*?-->|<!\[CDATA\[.*?\]\]>|<\?.*?\?>|<![^>]*>"
    r"|</?[A-Za-z][^\s/>]*(?:[^>\"']|\"[^\"]*\"|'[^']*')*>", re.S)
TAG_NAME_RE = re.compile(r"<(/?)([A-Za-z][^\s/>]*)")
LANG_ATTR_RE = re.compile(r"(\blang\s*=\s*)([\"'])[^\"']*\2", re.I)


def parse_markup(s, handler):
    parts = []
    pos = 0
    pre_depth = 0

    def text_part(text):
        if not text:
            return
        if pre_depth:
            lines = text.split("\n")
            for i, line in enumerate(lines):
                parts.append(handler(line) if line else "")
                if i < len(lines) - 1:
                    parts.append("\n")
        else:
            parts.append(handler(text))

    while True:
        m = TAG_RE.search(s, pos)
        if m is None:
            break
        text_part(s[pos:m.start()])
        tag = m.group()
        pos = m.end()
        nm = TAG_NAME_RE.match(tag)
        closing = False
        local = ""
        if nm:
            closing = nm.group(1) == "/"
            local = nm.group(2).lower().split(":")[-1]
        if local == "html" and not closing:
            tag = LANG_ATTR_RE.sub(r'\1"hi"', tag)
        parts.append(tag)
        if not nm:
            continue
        selfclose = tag.endswith("/>")
        if local in ("script", "style") and not closing and not selfclose:
            em = re.compile(r"</\s*" + local + r"\s*>", re.I).search(s, pos)
            end = em.start() if em else len(s)
            if end > pos:
                parts.append(s[pos:end])
            pos = end
        elif local == "pre" and not selfclose:
            pre_depth = max(0, pre_depth + (-1 if closing else 1))
    text_part(s[pos:])
    return parts


NCX_RE = re.compile(r"(<(?:\w+:)?text\b[^>]*>)([^<]*)(</(?:\w+:)?text>)", re.I)
OPF_RE = re.compile(
    r"(<(?:\w+:)?(?:title|description)\b[^>]*>)([^<]*)(</(?:\w+:)?(?:title|description)>)", re.I)


def parse_regex_doc(text, pattern, handler):
    parts = []
    pos = 0
    for m in pattern.finditer(text):
        parts.append(text[pos:m.start(2)])
        parts.append(handler(m.group(2)))
        pos = m.end(2)
    parts.append(text[pos:])
    return parts


# --------------------------------------------------------------------------
# EPUB and TXT processors
# --------------------------------------------------------------------------
ITEM_RE = re.compile(r"<(?:\w+:)?item\b([^>]*)>", re.I)
ITEMREF_RE = re.compile(r"<(?:\w+:)?itemref\b([^>]*)>", re.I)
ATTR_RE = re.compile(r"([\w:.-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)')")
HTML_MEDIA = ("application/xhtml+xml", "text/html")
NCX_MEDIA = ("application/x-dtbncx+xml",)


def _attrs(blob):
    return {k.lower(): (a if a is not None and a != "" else b or "")
            for k, a, b in ATTR_RE.findall(blob)}


class Doc:
    __slots__ = ("name", "kind", "parts")

    def __init__(self, name, kind, parts):
        self.name = name
        self.kind = kind
        self.parts = parts


class EpubProcessor:
    def __init__(self, path):
        self.path = path
        self.table = SegTable()
        self.docs = []

    def load(self):
        try:
            zf = zipfile.ZipFile(self.path)
        except (zipfile.BadZipFile, OSError):
            raise UserError("Ye valid EPUB nahi hai (andar ka ZIP kharab hai).")
        with zf:
            infos = zf.infolist()
            if len(infos) > ZIP_MAX_ENTRIES:
                raise UserError("Is EPUB mein bahut zyada files hain.")
            if sum(i.file_size for i in infos) > ZIP_MAX_UNPACKED:
                raise UserError("Ye EPUB unpack hone par bahut bada hai.")
            names = [i.filename for i in infos]
            self._check_drm(zf, names)
            html_names, ncx_names, opf_name = self._content_files(zf, names)
            handler = make_translate_handler(self.table, True)
            for name in html_names:
                text = fix_charset(decode_text(self._read(zf, name)))
                self.docs.append(Doc(name, "html", parse_markup(text, handler)))
            for name in ncx_names:
                text = fix_charset(decode_text(self._read(zf, name)))
                self.docs.append(Doc(name, "ncx", parse_regex_doc(text, NCX_RE, handler)))
            if opf_name:
                text = fix_charset(decode_text(self._read(zf, opf_name)))
                self.docs.append(Doc(opf_name, "opf", parse_regex_doc(text, OPF_RE, handler)))
        if not self.table.texts:
            raise UserError("Is EPUB mein translate karne layak text nahi mila "
                            "(shayad pehle se Hindi hai ya sirf images hain).")

    @staticmethod
    def _read(zf, name):
        try:
            return zf.read(name)
        except (zipfile.BadZipFile, KeyError, RuntimeError, OSError, zlib.error, EOFError):
            raise UserError(f"Ye EPUB kharab hai ({name} padh nahi paya).")

    @staticmethod
    def _check_drm(zf, names):
        if "META-INF/encryption.xml" not in names:
            return
        try:
            txt = decode_text(zf.read("META-INF/encryption.xml"))
        except Exception:
            return
        ok = ("http://www.idpf.org/2008/embedding", "http://ns.adobe.com/pdf/enc#RC")
        algos = re.findall(r"<(?:\w+:)?EncryptionMethod\b[^>]*Algorithm\s*=\s*[\"']([^\"']+)", txt)
        if any(a not in ok for a in algos):
            raise UserError("Ye EPUB DRM-protected hai, translate nahi ho sakta.")

    def _content_files(self, zf, names):
        lookup = {n: n for n in names}
        folded = {n.casefold(): n for n in names}

        def resolve(base_dir, href):
            href = urllib.parse.unquote(html.unescape(href.split("#")[0]))
            if not href:
                return None
            full = posixpath.normpath(posixpath.join(base_dir, href))
            return lookup.get(full) or folded.get(full.casefold())

        opf_name = None
        if "META-INF/container.xml" in lookup:
            try:
                cont = decode_text(zf.read("META-INF/container.xml"))
                m = re.search(r"full-path\s*=\s*[\"']([^\"']+)", cont)
                if m:
                    opf_name = resolve("", m.group(1))
            except Exception:
                opf_name = None
        if not opf_name:
            opf_name = next((n for n in names if n.lower().endswith(".opf")), None)

        html_names, ncx_names = [], []
        if opf_name:
            try:
                opf = decode_text(zf.read(opf_name))
            except Exception:
                opf = ""
            base = posixpath.dirname(opf_name)
            manifest = {}
            order = []
            for blob in ITEM_RE.findall(opf):
                a = _attrs(blob)
                if a.get("id") and a.get("href"):
                    manifest[a["id"]] = (a["href"], a.get("media-type", "").lower())
                    order.append(a["id"])
            spine = [_attrs(b).get("idref") for b in ITEMREF_RE.findall(opf)]
            seen = set()
            for iid in [i for i in spine if i in manifest] + order:
                if iid in seen:
                    continue
                seen.add(iid)
                href, media = manifest[iid]
                target = resolve(base, href)
                if not target:
                    continue
                if media in HTML_MEDIA and target not in html_names:
                    html_names.append(target)
                elif media in NCX_MEDIA and target not in ncx_names:
                    ncx_names.append(target)
        if not html_names:
            html_names = sorted(n for n in names
                                if n.lower().endswith((".xhtml", ".html", ".htm"))
                                and not n.startswith("META-INF/"))
        if not ncx_names:
            ncx_names = [n for n in names if n.lower().endswith(".ncx")]
        return html_names, ncx_names, opf_name

    def build(self, results, out_path):
        replaced = {}
        for doc in self.docs:
            text = render_parts(doc.parts, results)
            if doc.kind == "opf":
                text = re.sub(r"(<(?:\w+:)?language\b[^>]*>)[^<]*(</(?:\w+:)?language>)",
                              r"\1hi\2", text, flags=re.I)
            replaced[doc.name] = text.encode("utf-8")
        try:
            with zipfile.ZipFile(self.path) as zin, \
                    zipfile.ZipFile(out_path, "w", allowZip64=True) as zout:
                infos = sorted(zin.infolist(), key=lambda i: 0 if i.filename == "mimetype" else 1)
                for info in infos:
                    data = replaced[info.filename] if info.filename in replaced else zin.read(info)
                    zi = zipfile.ZipInfo(info.filename, info.date_time)
                    zi.external_attr = info.external_attr
                    zi.compress_type = (zipfile.ZIP_STORED if info.filename == "mimetype"
                                        else zipfile.ZIP_DEFLATED)
                    zout.writestr(zi, data)
        except (zipfile.BadZipFile, OSError, zlib.error) as exc:
            raise UserError(f"Hindi EPUB bana nahi paya ({type(exc).__name__}).")

    def scan_output(self, out_path):
        acc = []
        try:
            with zipfile.ZipFile(out_path) as zf:
                if zf.testzip() is not None:
                    raise UserError("Bani hui EPUB apni integrity jaanch mein fail hui.")
                handler = make_check_handler(acc, True)
                for doc in self.docs:
                    if doc.kind == "opf":
                        continue
                    text = zf.read(doc.name).decode("utf-8")
                    if doc.kind == "html":
                        parse_markup(text, handler)
                    else:
                        parse_regex_doc(text, NCX_RE, handler)
        except UserError:
            raise
        except (zipfile.BadZipFile, KeyError, UnicodeDecodeError, OSError, zlib.error) as exc:
            raise UserError(f"Bani hui EPUB final check mein fail hui ({type(exc).__name__}).")
        return acc


class TxtProcessor:
    def __init__(self, path):
        self.path = path
        self.table = SegTable()
        self.lines = []
        self.eol = "\n"

    def load(self):
        try:
            with open(self.path, "rb") as fh:
                raw = fh.read()
        except OSError:
            raise UserError("Text file padh nahi paya.")
        text = decode_text(raw).lstrip("\ufeff")
        self.eol = "\r\n" if "\r\n" in text else "\n"
        handler = make_translate_handler(self.table, False)
        for line in NL_RE.split(text):
            self.lines.append(handler(line) if line.strip() else line)
        if not self.table.texts:
            raise UserError("Is file mein translate karne layak text nahi mila "
                            "(shayad pehle se Hindi hai).")

    def build(self, results, out_path):
        try:
            with open(out_path, "w", encoding="utf-8", newline="") as fh:
                fh.write(self.eol.join(render_parts([ln], results) for ln in self.lines))
        except OSError:
            raise UserError("Hindi text file likh nahi paya.")

    def scan_output(self, out_path):
        acc = []
        try:
            with open(out_path, "r", encoding="utf-8", newline="") as fh:
                for line in NL_RE.split(fh.read()):
                    core = line.strip()
                    if core:
                        acc.append(core)
        except (OSError, UnicodeDecodeError):
            raise UserError("Bani hui text file final check mein fail hui.")
        return acc


# --------------------------------------------------------------------------
# Offline translation engine (Argos Translate, runs on this machine)
# --------------------------------------------------------------------------
ARGOS_PAIRS = (("en", "hi"), ("zh", "en"))      # Chinese goes zh -> en -> hi
TRANSLATE_LOCK = threading.RLock()              # one translation at a time (CPU bound)
_TR_CACHE = {}
LANG_NAME = {"zh": "Chinese", "en": "English"}


def _argos(module):
    try:
        if module == "package":
            import argostranslate.package as mod
        else:
            import argostranslate.translate as mod
        return mod
    except ImportError:
        raise UserError("The offline translation engine (argostranslate) is not installed "
                        "on this server.")


def ensure_models():
    """Download the language models once if they are missing.
    Only model files are downloaded; no user text is involved."""
    ap = _argos("package")
    installed = {(p.from_code, p.to_code) for p in ap.get_installed_packages()}
    missing = [p for p in ARGOS_PAIRS if p not in installed]
    if not missing:
        log.info("offline models already installed")
        return
    log.info("downloading offline models: %s", ", ".join(f"{a}->{b}" for a, b in missing))
    ap.update_package_index()
    avail = ap.get_available_packages()
    for pair in missing:
        pkg = next((p for p in avail if (p.from_code, p.to_code) == pair), None)
        if pkg is None:
            log.error("model %s->%s is not in the Argos package list", pair[0], pair[1])
            continue
        ap.install_from_path(pkg.download())
        log.info("installed model %s->%s", pair[0], pair[1])


def get_translate_fn(sl):
    """Returns a thread-safe translate(text) function for source language `sl`."""
    src = sl if sl in ("zh", "en") else "en"
    with TRANSLATE_LOCK:
        fn = _TR_CACHE.get(src)
        if fn is not None:
            return fn
        at = _argos("translate")
        probe_text = "你好。你今天好吗？" if src == "zh" else "Hello. How are you today?"
        try:
            tr = at.get_translation_from_codes(src, TARGET)
            probe = tr.translate(probe_text)
        except Exception:
            log.debug("model probe failed:\n%s", traceback.format_exc())
            tr, probe = None, None
        if tr is None or not isinstance(probe, str):
            raise UserError(f"The offline {LANG_NAME.get(src, src)} → Hindi model is not "
                            f"installed on this server.")
        raw = tr.translate

        def locked(text):
            with TRANSLATE_LOCK:
                return raw(text)
        _TR_CACHE[src] = locked
        return locked


def lock_network(allowed_hosts):
    """After this call the process can only look up the listed host names
    (plus localhost). Anything else (for example an online translate service)
    fails immediately."""
    real = socket.getaddrinfo
    ok = {h.lower() for h in allowed_hosts if h} | {"localhost", "127.0.0.1", "::1"}

    def guarded(host, *args, **kwargs):
        name = host.decode("ascii", "ignore") if isinstance(host, bytes) else host
        if name is not None and str(name).lower() not in ok:
            raise OSError(f"network blocked (offline mode): {name}")
        return real(host, *args, **kwargs)

    socket.getaddrinfo = guarded


class LocalRun:
    """Translates the segment table one line at a time on this machine."""

    def __init__(self, texts, translate, cancel_event):
        self.texts = texts
        self.translate = translate
        self.cancel = cancel_event
        self.total_chars = sum(len(t) for t in texts) or 1
        self.done_chars = 0
        self.retried = 0
        self.out = []

    def segment(self, text):
        out = clean_result(self.translate(text) or "")
        best, best_score = out, leftover_score(text, out)
        if best_score == 0.0:
            return best
        self.retried += 1
        pieces = split_pieces(text, 120)             # retry in small pieces
        if len(pieces) > 1:
            alt = clean_result(" ".join(self.translate(p) or "" for p in pieces))
            s = leftover_score(text, alt)
            if alt and s < best_score:
                best = alt
        return best if best else text

    def execute(self):
        streak = 0
        for t in self.texts:
            if self.cancel.is_set() or STOP.is_set():
                raise Cancelled()
            try:
                r = self.segment(t)
                streak = 0
            except Exception as exc:
                streak += 1
                log.warning("segment failed: %s: %s", type(exc).__name__, exc)
                r = t
                if streak >= 10:
                    raise UserError("The offline translator keeps failing on this file. "
                                    "Please try again later.")
            self.out.append(r)
            self.done_chars += len(t)

    def results(self):
        return list(self.out)


def make_outputs(ext, out_path, base_name):
    size = os.path.getsize(out_path)
    if ext == ".epub":
        if size > TG_UPLOAD_LIMIT:
            raise UserError("The translated EPUB is bigger than Telegram's 50 MB bot upload limit.")
        return [(out_path, f"{base_name}_hindi.epub")]
    if size <= TG_UPLOAD_LIMIT:
        return [(out_path, f"{base_name}_hindi.txt")]
    res = []
    part, written = 1, 0
    limit = 45 * 1000 * 1000
    cur = None
    folder = os.path.dirname(out_path)
    try:
        with open(out_path, "r", encoding="utf-8", newline="") as src:
            for line in src:
                b = len(line.encode("utf-8"))
                if cur is None or written + b > limit:
                    if cur:
                        cur.close()
                    p = os.path.join(folder, f"part{part}.txt")
                    cur = open(p, "w", encoding="utf-8", newline="")
                    res.append((p, f"{base_name}_hindi_part{part}.txt"))
                    part += 1
                    written = 0
                cur.write(line)
                written += b
    finally:
        if cur:
            cur.close()
    return res


# --------------------------------------------------------------------------
# Telegram client (Bot API over plain HTTPS)
# --------------------------------------------------------------------------
class TgClient:
    def __init__(self, token, base="https://api.telegram.org"):
        self.token = token
        self.base = base.rstrip("/")

    def url(self, method):
        return f"{self.base}/bot{self.token}/{method}"

    def call(self, method, params=None, timeout=30.0, retries=4):
        body = json.dumps(params or {}).encode("utf-8")
        delay = 1.0
        for attempt in range(retries + 1):
            try:
                status, _, data = http_request("POST", self.url(method),
                                               {"Content-Type": "application/json"}, body, timeout)
            except HttpError as exc:
                if attempt >= retries:
                    raise TelegramError(0, f"network problem: {exc}")
                time.sleep(delay)
                delay = min(delay * 2, 15.0)
                continue
            try:
                j = json.loads(data.decode("utf-8", "replace"))
            except ValueError:
                j = {}
            if j.get("ok"):
                return j.get("result")
            code = j.get("error_code") or status
            desc = j.get("description") or f"HTTP {status}"
            ra = (j.get("parameters") or {}).get("retry_after")
            if code == 429 and attempt < retries:
                time.sleep(min((ra or 1) + 1, 60))
                continue
            if code >= 500 and attempt < retries:
                time.sleep(delay)
                delay = min(delay * 2, 15.0)
                continue
            raise TelegramError(code, desc, ra)
        raise TelegramError(0, "unreachable")

    def download_file(self, file_id, dest, max_bytes):
        info = self.call("getFile", {"file_id": file_id})
        fp = (info or {}).get("file_path")
        if not fp:
            raise TelegramError(400, "no file path")
        url = f"{self.base}/file/bot{self.token}/{fp.lstrip('/')}"
        last = None
        for attempt in range(3):
            try:
                return http_download(url, dest, timeout=120.0, max_bytes=max_bytes)
            except HttpError as exc:
                last = exc
                if "too big" in str(exc):
                    raise
                time.sleep(1 + attempt * 2)
        raise TelegramError(0, f"download failed: {last}")

    def send_document(self, chat_id, path, filename, caption="", retries=3):
        last = None
        for attempt in range(retries + 1):
            try:
                return self._send_document_once(chat_id, path, filename, caption)
            except TelegramError as exc:
                last = exc
                if exc.code == 429:
                    time.sleep(min((exc.retry_after or 2) + 1, 60))
                elif exc.code in (400, 401, 403, 404):
                    raise
                else:
                    time.sleep(2 + attempt * 3)
        raise last

    def _send_document_once(self, chat_id, path, filename, caption):
        boundary = "----hb" + uuid.uuid4().hex
        pre = b""
        for k, v in (("chat_id", str(chat_id)), ("caption", caption[:1000])):
            pre += (f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'
                    ).encode("utf-8")
        safe = filename.replace('"', "_").replace("\r", " ").replace("\n", " ")
        pre += (f'--{boundary}\r\nContent-Disposition: form-data; name="document"; '
                f'filename="{safe}"\r\nContent-Type: application/octet-stream\r\n\r\n'
                ).encode("utf-8")
        post = f"\r\n--{boundary}--\r\n".encode()
        size = os.path.getsize(path)
        total = len(pre) + size + len(post)

        def gen():
            yield pre
            with open(path, "rb") as fh:
                while True:
                    chunk = fh.read(262144)
                    if not chunk:
                        break
                    yield chunk
            yield post

        sp = urllib.parse.urlsplit(self.url("sendDocument"))
        port = sp.port or (443 if sp.scheme == "https" else 80)
        if sp.scheme == "https":
            conn = http.client.HTTPSConnection(sp.hostname, port, timeout=600, context=_SSL_CTX)
        else:
            conn = http.client.HTTPConnection(sp.hostname, port, timeout=600)
        try:
            conn.request("POST", sp.path, body=gen(), headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(total), "User-Agent": UA})
            resp = conn.getresponse()
            data = resp.read()
        except (socket.timeout, TimeoutError):
            raise TelegramError(0, "upload timed out")
        except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
            raise TelegramError(0, f"upload failed: {type(exc).__name__}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
        try:
            j = json.loads(data.decode("utf-8", "replace"))
        except ValueError:
            j = {}
        if j.get("ok"):
            return j.get("result")
        raise TelegramError(j.get("error_code") or resp.status, j.get("description") or "upload failed",
                            (j.get("parameters") or {}).get("retry_after"))


# --------------------------------------------------------------------------
# Jobs
# --------------------------------------------------------------------------
HELP_TEXT = (
    "📚 Hindi Translate Bot\n\n"
    "Send me a .epub or .txt file (up to 15 MB).\n"
    "I translate Chinese or English into Hindi and send the file back.\n\n"
    "• Translation runs on an offline model on my own server\n"
    "  (no Google, Bing or other online translate service)\n"
    "• Live progress bar, time used and time left\n"
    "• Every line is checked, leftovers are retried\n"
    "• Send several files: they wait in a queue (see /queue)\n\n"
    "Just send the file. No settings needed."
)


class Job:
    def __init__(self, bot, chat_id, file_id, file_name, ext):
        self.bot = bot
        self.tg = bot.tg
        self.chat_id = chat_id
        self.file_id = file_id
        self.file_name = file_name
        self.ext = ext
        self.id = uuid.uuid4().hex[:10]
        self.cancel_ev = threading.Event()
        self.msg_id = None
        self.t0 = time.time()
        self.phase = "queued"
        self.run = None
        self.sl_label = ""
        self.samples = collections.deque()
        self.tr_start = None
        self.last_text = None
        self.next_edit = 0.0
        self.workdir = None
        self.done_flag = threading.Event()
        self.started = False
        self.queue_pos = 0
        self.queue_total = 0

    # ----- progress text -----
    def eta_rate(self):
        run = self.run
        if run is None:
            return None, 0.0
        now = time.time()
        done, total = run.done_chars, run.total_chars
        self.samples.append((now, done))
        while len(self.samples) > 2 and now - self.samples[0][0] > 60:
            self.samples.popleft()
        t0, d0 = self.samples[0]
        rate = None
        if now - t0 >= 4 and done > d0:
            rate = (done - d0) / (now - t0)
        if rate is None and done > 0 and self.tr_start:
            rate = done / max(0.001, now - self.tr_start)
        if not rate:
            return None, 0.0
        return (total - done) / rate, rate

    def render(self):
        el = time.time() - self.t0
        name = self.file_name if len(self.file_name) <= 40 else self.file_name[:37] + "..."
        lines = [f"📄 {name} → Hindi"]
        if self.sl_label:
            lines.append(self.sl_label)
        run = self.run
        if self.phase == "queued":
            lines += ["", f"⏳ Queue: {self.queue_pos or 1} of {self.queue_total or 1}",
                      f"⚙️ {MAX_JOBS} file(s) run at a time. Starts automatically."]
        elif self.phase == "translate" and run is not None:
            p = run.done_chars / run.total_chars
            eta, rate = self.eta_rate()
            left = f"~{fmt_t(eta)}" if eta is not None else "calculating..."
            lines += ["", "🔒 Translating offline",
                      f"{progress_bar(p)} {p * 100:.1f}%",
                      f"⏱ Used {fmt_t(el)}  •  Left {left}"]
            if rate:
                lines.append(f"⚡ {fmt_n(rate)} chars/s")
            if run.retried:
                lines.append(f"🔁 Re-checked lines: {fmt_n(run.retried)}")
        else:
            labels = {"download": "📥 Downloading...",
                      "read": "🔍 Reading the file...",
                      "build": "📦 Building the Hindi file...", "check": "🧪 Final check...",
                      "send": "📤 Sending the file..."}
            lines += ["", labels.get(self.phase, "Working..."), f"⏱ Used {fmt_t(el)}"]
        return "\n".join(lines)

    def push(self, force=False, final=False):
        if self.msg_id is None:
            return
        now = time.time()
        if not force and now < self.next_edit:
            return
        text = self.render() if not final else self.last_text
        if text == self.last_text and not final:
            return
        params = {"chat_id": self.chat_id, "message_id": self.msg_id, "text": text}
        if not final:
            params["reply_markup"] = {"inline_keyboard": [[
                {"text": "❌ Cancel", "callback_data": f"c:{self.id}"}]]}
        try:
            self.tg.call("editMessageText", params, timeout=20, retries=0)
            self.last_text = text
            self.next_edit = now + 3.0
        except TelegramError as exc:
            if "not modified" in exc.description.lower():
                self.last_text = text
            elif exc.code == 429:
                self.next_edit = now + (exc.retry_after or 5) + 1
            else:
                self.next_edit = now + 5.0

    def finish_text(self, text):
        self.last_text = text
        params = {"chat_id": self.chat_id, "message_id": self.msg_id, "text": text}
        try:
            self.tg.call("editMessageText", params, timeout=20, retries=2)
        except TelegramError:
            try:
                self.tg.call("sendMessage", {"chat_id": self.chat_id, "text": text}, timeout=20, retries=2)
            except TelegramError:
                pass

    def ticker(self):
        try:
            while not self.done_flag.wait(1.0):
                try:
                    self.push()
                except Exception:
                    log.debug("ticker: %s", traceback.format_exc())
        finally:
            close_thread_connections()

    def check_cancel(self):
        if self.cancel_ev.is_set() or STOP.is_set():
            raise Cancelled()

    # ----- the whole job -----
    def start(self):
        try:
            m = self.tg.call("sendMessage", {
                "chat_id": self.chat_id, "text": f"⏳ Got it: {self.file_name}\nWaiting to start...",
                "reply_markup": {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": f"c:{self.id}"}]]}},
                timeout=20)
            self.msg_id = m["message_id"]
        except (TelegramError, KeyError, TypeError):
            self.bot.finish_job(self)
            return False
        return True

    def execute(self):
        threading.Thread(target=self.ticker, daemon=True).start()
        try:
            self.t0 = time.time()
            self._pipeline()
        except Cancelled:
            self.done_flag.set()
            self.finish_text("❌ Cancelled." if not STOP.is_set()
                             else "⚠️ The bot is restarting. Please send the file again in a minute.")
        except UserError as exc:
            self.done_flag.set()
            self.finish_text(f"⚠️ {exc}")
        except TelegramError as exc:
            log.warning("telegram problem in job: %s", exc)
            self.done_flag.set()
            self.finish_text("⚠️ Telegram had a problem while I worked on your file. Please send it again.")
        except MemoryError:
            self.done_flag.set()
            self.finish_text("⚠️ This file needs more memory than this server has.")
        except Exception:
            log.error("job crashed:\n%s", traceback.format_exc())
            self.done_flag.set()
            self.finish_text("⚠️ Something went wrong while translating this file. "
                             "Please send it again.")
        finally:
            self.done_flag.set()
            if self.workdir:
                shutil.rmtree(self.workdir, ignore_errors=True)
            self.bot.finish_job(self)

    def _pipeline(self):
        self.check_cancel()
        self.workdir = tempfile.mkdtemp(prefix="hindibot_")
        src = os.path.join(self.workdir, "input" + self.ext)
        self.phase = "download"
        self.push(force=True)
        try:
            self.tg.download_file(self.file_id, src, MAX_FILE_BYTES + 1)
        except HttpError:
            raise UserError("That file is bigger than 15 MB.")
        except TelegramError as exc:
            if "too big" in exc.description.lower():
                raise UserError("That file is bigger than 15 MB.")
            raise UserError("I could not download that file from Telegram. Please send it again.")
        if os.path.getsize(src) > MAX_FILE_BYTES:
            raise UserError("That file is bigger than 15 MB.")
        self.check_cancel()

        self.phase = "read"
        self.push(force=True)
        proc = EpubProcessor(src) if self.ext == ".epub" else TxtProcessor(src)
        proc.load()
        texts = proc.table.texts
        sl = detect_source(texts)
        total_chars = sum(len(t) for t in texts)
        self.sl_label = (f"🈶 {LANG_NAME.get(sl, 'English')}"
                         f"  •  {fmt_n(total_chars)} chars  •  {fmt_n(len(texts))} lines")
        translate = get_translate_fn(sl)
        self.check_cancel()

        run = LocalRun(texts, translate, self.cancel_ev)
        self.run = run
        self.tr_start = time.time()
        self.phase = "translate"
        self.push(force=True)
        run.execute()
        self.check_cancel()
        results = run.results()

        self.phase = "build"
        self.push(force=True)
        out_path = os.path.join(self.workdir, "output" + self.ext)
        proc.build(results, out_path)

        self.phase = "check"
        self.push(force=True)
        units = proc.scan_output(out_path)
        problems, kept = [], []
        for u in units:
            flag = final_flag(u)
            if flag == "problem":
                problems.append(u)
            elif flag == "kept":
                kept.append(u)
        total_units = sum(1 for u in units if any(ch.isalpha() for ch in u))
        self.check_cancel()

        base_name = re.sub(r"[^\w\-. ()\[\]]+", "_", os.path.splitext(self.file_name)[0]).strip(" ._") or "book"
        base_name = base_name[:80]
        files = make_outputs(self.ext, out_path, base_name)
        if problems:
            lp = os.path.join(self.workdir, "leftover_lines.txt")
            with open(lp, "w", encoding="utf-8") as fh:
                fh.write("Lines that still look untranslated (first 300):\n\n")
                fh.write("\n".join(problems[:300]))
            extra = (lp, f"{base_name}_leftover_lines.txt")
        else:
            extra = None

        elapsed = time.time() - self.t0
        many = total_units > 0 and len(problems) > 0.2 * total_units
        head = (f"⚠️ Finished in {fmt_t(elapsed)}, but many lines are still untranslated. "
                f"The offline model could not handle this text well."
                if many else f"✅ Done in {fmt_t(elapsed)}")
        caption = [head, f"🈶 {LANG_NAME.get(sl, 'English')} → Hindi",
                   f"🧪 Check: {fmt_n(total_units - len(problems) - len(kept))} of {fmt_n(total_units)} lines fully translated"]
        if kept:
            caption.append(f"ℹ️ {fmt_n(len(kept))} short names/words kept as they are")
        if problems:
            caption.append(f"⚠️ {fmt_n(len(problems))} lines may still have untranslated words (list attached)")
        else:
            caption.append("✔️ No untranslated lines found")
        caption.append("🔒 Translated offline, no online translate service was used")
        cap = "\n".join(caption)

        self.phase = "send"
        self.push(force=True)
        for i, (path, fname) in enumerate(files):
            self.check_cancel()
            self.tg.send_document(self.chat_id, path, fname, cap if i == len(files) - 1 else "")
        if extra:
            self.tg.send_document(self.chat_id, extra[0], extra[1], "Lines to check by hand.")
        self.done_flag.set()
        self.finish_text(cap)


# --------------------------------------------------------------------------
# Bot (update handling + queue)
# --------------------------------------------------------------------------
class Bot:
    def __init__(self, tg):
        self.tg = tg
        self.jobs = {}                                   # all jobs (waiting + running)
        self.queue = collections.deque()                 # waiting jobs
        self.running = 0
        self.chat_jobs = collections.defaultdict(set)    # chat_id -> job ids
        self.lock = threading.Lock()
        self.wake = threading.Event()
        threading.Thread(target=self._dispatcher, daemon=True, name="dispatcher").start()
        threading.Thread(target=self._queue_ticker, daemon=True, name="queue-ticker").start()

    def say(self, chat_id, text):
        try:
            self.tg.call("sendMessage", {"chat_id": chat_id, "text": text}, timeout=20, retries=2)
        except TelegramError as exc:
            log.debug("say failed: %s", exc)

    # ---- queue ----
    def _dispatcher(self):
        while not STOP.is_set():
            self.wake.wait(1.0)
            self.wake.clear()
            while not STOP.is_set():
                with self.lock:
                    if self.running >= MAX_JOBS or not self.queue:
                        break
                    job = self.queue.popleft()
                    self.running += 1
                    job.started = True
                threading.Thread(target=job.execute, daemon=True, name=f"job-{job.id}").start()
                self.refresh_queue()

    def _queue_ticker(self):
        while not STOP.wait(4.0):
            try:
                self.refresh_queue()
            except Exception:
                log.debug("queue ticker: %s", traceback.format_exc())

    def refresh_queue(self, force=False):
        with self.lock:
            waiting = list(self.queue)
        n = len(waiting)
        for i, j in enumerate(waiting, 1):
            j.queue_pos, j.queue_total = i, n
            j.push(force=force)

    def finish_job(self, job):
        with self.lock:
            self.jobs.pop(job.id, None)
            self.chat_jobs[job.chat_id].discard(job.id)
            if not self.chat_jobs[job.chat_id]:
                self.chat_jobs.pop(job.chat_id, None)
            if job.started:
                job.started = False
                self.running = max(0, self.running - 1)
        self.wake.set()

    def drop_queue(self):
        with self.lock:
            waiting = list(self.queue)
            self.queue.clear()
        for j in waiting:
            j.finish_text("⚠️ The bot is restarting. Please send the file again in a minute.")
            self.finish_job(j)

    def status_text(self, chat_id):
        with self.lock:
            mine = [j for j in self.jobs.values() if j.chat_id == chat_id]
            q = list(self.queue)
            running = self.running
        lines = [f"⚙️ Running: {running}/{MAX_JOBS}   ⏳ Waiting: {len(q)}/{MAX_QUEUE}"]
        for j in mine:
            if j in q:
                lines.append(f"• {j.file_name} → queue #{q.index(j) + 1}")
            else:
                lines.append(f"• {j.file_name} → running ({j.phase})")
        if not mine:
            lines.append("You have no files in the queue.")
        return "\n".join(lines)

    # ---- updates ----
    def handle_update(self, upd):
        if "callback_query" in upd:
            self.on_callback(upd["callback_query"])
        elif "message" in upd:
            self.on_message(upd["message"])

    def on_callback(self, cq):
        data = cq.get("data") or ""
        text = ""
        if data.startswith("c:"):
            with self.lock:
                job = self.jobs.get(data[2:])
            chat = ((cq.get("message") or {}).get("chat") or {}).get("id")
            if job and chat == job.chat_id:
                with self.lock:
                    waiting = job in self.queue
                    if waiting:
                        self.queue.remove(job)
                job.cancel_ev.set()
                text = "Cancelling..."
                if waiting:
                    job.finish_text("❌ Cancelled (removed from queue).")
                    self.finish_job(job)
                    self.refresh_queue()
            else:
                text = "This job is already finished."
        try:
            self.tg.call("answerCallbackQuery", {"callback_query_id": cq.get("id"), "text": text},
                         timeout=15, retries=1)
        except TelegramError:
            pass

    def on_message(self, msg):
        chat_id = (msg.get("chat") or {}).get("id")
        if chat_id is None:
            return
        doc = msg.get("document")
        if not doc:
            txt = (msg.get("text") or "").strip()
            if txt.startswith("/queue"):
                self.say(chat_id, self.status_text(chat_id))
            elif txt.startswith("/start") or txt.startswith("/help") or not txt.startswith("/"):
                self.say(chat_id, HELP_TEXT if txt.startswith("/") else
                         "Please send a .epub or .txt file (up to 15 MB). I will translate it to Hindi.")
            return
        name = doc.get("file_name") or ""
        mime = (doc.get("mime_type") or "").lower()
        ext = os.path.splitext(name)[1].lower()
        if ext not in (".epub", ".txt"):
            if mime == "application/epub+zip":
                ext = ".epub"
            elif mime == "text/plain":
                ext = ".txt"
            else:
                self.say(chat_id, "I can only translate .epub and .txt files.")
                return
        if not name:
            name = "file" + ext
        size = doc.get("file_size")
        if isinstance(size, int) and size > MAX_FILE_BYTES:
            self.say(chat_id, f"That file is {size / 1048576:.1f} MB. The limit is 15 MB.")
            return
        job = None
        reason = None
        with self.lock:
            if len(self.chat_jobs.get(chat_id, ())) >= MAX_PER_CHAT:
                reason = (f"⏳ You already have {MAX_PER_CHAT} files in progress. "
                          f"Wait for one to finish, or cancel one.")
            elif len(self.queue) >= MAX_QUEUE:
                reason = "⏳ The queue is full right now. Please try again in a few minutes."
            else:
                job = Job(self, chat_id, doc.get("file_id"), name, ext)
                self.jobs[job.id] = job
                self.chat_jobs[chat_id].add(job.id)
        if reason:
            self.say(chat_id, reason)
            return
        if not job.start():
            return
        with self.lock:
            self.queue.append(job)
        self.wake.set()
        self.refresh_queue(force=True)

    def wait_idle(self, seconds):
        """Wait until every RUNNING job is finished (or the time is up)."""
        end = time.time() + seconds
        while time.time() < end and not STOP.is_set():
            with self.lock:
                if self.running == 0:
                    return True
            time.sleep(1.0)
        with self.lock:
            return self.running == 0

    def shutdown(self):
        self.drop_queue()
        with self.lock:
            jobs = list(self.jobs.values())
        for j in jobs:
            j.cancel_ev.set()


# --------------------------------------------------------------------------
# Optional tiny web server (some free hosts want an open port)
# --------------------------------------------------------------------------
class _Health(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"Hindi translate bot is running"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_HEAD = do_GET

    def log_message(self, *args):
        pass


def start_health_server():
    port = os.environ.get("PORT", "").strip()
    if not port.isdigit():
        return None
    srv = http.server.ThreadingHTTPServer(("0.0.0.0", int(port)), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("health server on port %s", port)
    return srv


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------
class _Redact(logging.Filter):
    def __init__(self, token):
        super().__init__()
        self.token = token

    def filter(self, record):
        if self.token:
            msg = record.getMessage()
            if self.token in msg:
                record.msg = msg.replace(self.token, "***")
                record.args = ()
        return True


def poll_loop(bot, tg, deadline=None):
    """Returns (next_offset, timed_out)."""
    offset = None
    last_conflict = 0.0
    while not STOP.is_set():
        if deadline is not None and time.time() >= deadline:
            return offset, True
        params = {"timeout": 25, "allowed_updates": ["message", "callback_query"]}
        if offset is not None:
            params["offset"] = offset
        try:
            updates = tg.call("getUpdates", params, timeout=45, retries=0)
        except TelegramError as exc:
            if exc.code == 401:
                log.error("Telegram says the BOT_TOKEN is not valid. Stopping.")
                FATAL.set()
                STOP.set()
                return offset, False
            if exc.code == 409:
                if time.time() - last_conflict > 60:
                    log.warning("Another copy of this bot is polling with the same token. Waiting...")
                    last_conflict = time.time()
                try:
                    tg.call("deleteWebhook", {"drop_pending_updates": False}, retries=0)
                except TelegramError:
                    pass
            else:
                log.warning("getUpdates problem: %s", exc)
            STOP.wait(3.0)
            continue
        except Exception:
            log.error("poll crashed:\n%s", traceback.format_exc())
            STOP.wait(3.0)
            continue
        for upd in updates or []:
            offset = upd.get("update_id", 0) + 1
            try:
                bot.handle_update(upd)
            except Exception:
                log.error("update crashed:\n%s", traceback.format_exc())
    return offset, False


def prepare_offline_engine():
    """Install models if needed and warm them up, BEFORE the network lock goes on.
    Returns the list of source languages that work."""
    ensure_models()
    working = []
    for sl in ("en", "zh"):
        try:
            get_translate_fn(sl)
            working.append(sl)
        except UserError as exc:
            log.warning("%s", exc)
    return working


def main():
    token = os.environ.get("BOT_TOKEN", "").strip()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for handler in logging.getLogger().handlers:
        handler.addFilter(_Redact(token))
    if not token:
        print("BOT_TOKEN is missing.\n"
              "Get one from @BotFather in Telegram, then run:\n"
              "  export BOT_TOKEN='123456:ABC...'\n  python bot.py")
        return 1
    if not re.match(r"^\d+:[\w-]{20,}$", token):
        print("BOT_TOKEN does not look right. It should look like 123456789:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        return 1
    api_base = os.environ.get("TELEGRAM_API_BASE", "https://api.telegram.org")
    tg = TgClient(token, api_base)
    try:
        me = tg.call("getMe", retries=3)
    except TelegramError as exc:
        print(f"Could not log in to Telegram: {exc.description}")
        return 1
    try:
        tg.call("deleteWebhook", {"drop_pending_updates": False}, retries=2)
    except TelegramError:
        pass

    try:
        working = prepare_offline_engine()
    except UserError as exc:
        print(f"{exc}\nInstall it with:  pip install argostranslate")
        return 1
    except Exception as exc:
        print(f"Could not prepare the offline translation models: {type(exc).__name__}: {exc}")
        return 1
    if not working:
        print("No offline translation model could be loaded. Stopping.")
        return 1
    if os.environ.get("OFFLINE_LOCK", "1") != "0":
        lock_network({urllib.parse.urlsplit(api_base).hostname})
        log.info("network lock on: only %s is reachable", urllib.parse.urlsplit(api_base).hostname)

    bot = Bot(tg)
    start_health_server()
    log.info("Hindi translate bot v%s running as @%s, offline models: %s "
             "(jobs=%d, queue=%d, per chat=%d)",
             VERSION, (me or {}).get("username", "?"), ",".join(working),
             MAX_JOBS, MAX_QUEUE, MAX_PER_CHAT)

    def _stop(*_):
        STOP.set()

    signal.signal(signal.SIGINT, _stop)
    try:
        signal.signal(signal.SIGTERM, _stop)
    except (ValueError, AttributeError, OSError):
        pass
    deadline = time.time() + MAX_RUNTIME_MIN * 60 if MAX_RUNTIME_MIN > 0 else None
    offset, timed_out = poll_loop(bot, tg, deadline)
    if timed_out:
        log.info("time limit reached: no new files, finishing running jobs (up to %d min)", DRAIN_MIN)
        try:        # confirm what we already handled so the next copy does not repeat it
            if offset is not None:
                for upd in tg.call("getUpdates", {"offset": offset, "limit": 1, "timeout": 0},
                                   timeout=20, retries=1) or []:
                    bot.handle_update(upd)
        except TelegramError as exc:
            log.warning("final confirm failed: %s", exc)
        bot.drop_queue()
        bot.wait_idle(DRAIN_MIN * 60)
    log.info("stopping...")
    bot.shutdown()
    time.sleep(1.0)
    return 1 if FATAL.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())

"""
Extension probe diagnostic for GulfLINK An Nasiriyah III interview files.

Hypothesis: some interviews exist as OCR text files (.htm or .txt) rather
than scanned GIF images, and the wrapper pages pointing to .gif extensions
return placeholders because the GIF format does not exist for those files.

This script tests multiple file extensions for a single filename base in
the refimages directory. Once the correct extension is identified for each
document type, the multi-document fetcher can be updated accordingly.
"""
import time
import random
import hashlib
import logging
from pathlib import Path
import requests

OUT_DIR = Path(r'C:\Users\geoff\Downloads\New folder (4)\Transcript run')
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(OUT_DIR / 'extension_probe.log'), encoding='utf-8')
    ]
)
log = logging.getLogger()

BASE = 'https://www.gulflink.osd.mil'
UA = 'Mozilla/5.0 (compatible; AcademicResearchBot/1.0; Gulf War illness dissertation research)'
HEADERS = {'User-Agent': UA}

PLACEHOLDER_MD5 = '1c12ed1397c20160e751e4e7af33bcbb'

# Test targets: filename bases known to exist in the endnote references.
TEST_BASES = [
    ('7259_079_0000001', 'LS 895 page 1 (failed as .gif)'),
    ('aug62_001_0000001', 'Eglin AFB transcript page 1 (failed as .gif)'),
    ('7162_968_0000001', 'LS 12002 page 1 (known good as .gif)'),
]

# File extensions and subdirectories to probe
CANDIDATES = [
    # (subdirectory, extension)
    ('/an_iii/refimages', '.gif'),
    ('/an_iii/refimages', '.htm'),
    ('/an_iii/refimages', '.html'),
    ('/an_iii/refimages', '.txt'),
    ('/an_iii/refimages', '.pdf'),
    ('/an_iii/reftext', '.htm'),
    ('/an_iii/reftext', '.txt'),
    ('/an_iii/reftext', '.html'),
    ('/an_iii/ocr', '.htm'),
    ('/an_iii/ocr', '.txt'),
]


def polite_get(url, stream=False):
    time.sleep(random.uniform(3, 7))
    return requests.get(url, headers=HEADERS, timeout=30, stream=stream)


def classify(data, content_type=''):
    """Return a short description of what the bytes are."""
    if len(data) < 10:
        return f'too short ({len(data)} bytes)'
    magic = data[:6]
    md5 = hashlib.md5(data).hexdigest()
    if md5 == PLACEHOLDER_MD5:
        return f'PLACEHOLDER (876-byte HTML error)'
    if magic in (b'GIF87a', b'GIF89a'):
        return f'VALID GIF ({len(data):,} bytes)'
    if magic.startswith(b'%PDF'):
        return f'PDF ({len(data):,} bytes)'
    # Try to decode as text
    try:
        text = data[:200].decode('utf-8', errors='replace')
        # Check if it looks like substantive content vs an error page
        if len(data) > 2000:
            return f'TEXT/HTML ({len(data):,} bytes) starts: {text[:100]!r}'
        else:
            return f'short text ({len(data):,} bytes) starts: {text[:100]!r}'
    except Exception:
        return f'binary unknown magic={magic}, size={len(data):,}'


for base, description in TEST_BASES:
    log.info('')
    log.info(f'=== {description}: {base} ===')

    for subdir, ext in CANDIDATES:
        url = f'{BASE}{subdir}/{base}{ext}'
        log.info(f'  Testing: {url}')

        try:
            r = polite_get(url, stream=True)
            if r.status_code == 404:
                log.info(f'    HTTP 404')
                continue
            if r.status_code != 200:
                log.info(f'    HTTP {r.status_code}')
                continue

            data = b''
            for chunk in r.iter_content(8192):
                data += chunk

            content_type = r.headers.get('Content-Type', '')
            result = classify(data, content_type)
            log.info(f'    HTTP 200, CT={content_type} -> {result}')

            # Save any successful response that is not a placeholder
            if 'PLACEHOLDER' not in result and len(data) > 1000:
                safe_base = base.replace('/', '_')
                safe_subdir = subdir.replace('/', '_').strip('_')
                out_path = OUT_DIR / f'probe_{safe_subdir}_{safe_base}{ext}'
                out_path.write_bytes(data)
                log.info(f'    SAVED: {out_path}')

        except Exception as e:
            log.warning(f'    Error: {e}')

log.info('')
log.info('Extension probe complete.')
log.info('Review the saved probe_*.* files to identify the correct format')
log.info('for each document in the An Nasiriyah III corpus.')

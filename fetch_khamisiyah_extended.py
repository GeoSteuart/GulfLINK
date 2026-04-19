"""
Extended Khamisiyah Lead Report extraction including documents from the
1999 Wayback capture of the original 1997 Khamisiyah I narrative.

Pattern found:
  Original 1997 narrative: /khamisiyah/kham_ref/n01enNNN/NNNldN.htm
  Final 2002 narrative:    /khamisiyah_iii/khamisiyah_iii_refs/n70enNNN/NNNldN.htm
                       OR: /khamisiyah_iii/khamisiyah_iii_refs/n70enNNN/NNNN_NNN_NNNNNNN.htm

Some Lead Reports cited in the 1997 narrative are not cited in the 2002
narrative with either filename convention. Those documents may still exist
on the server at either path.

New targets from the 1999 Wayback capture:
  Lead Sheet 806 - EOD NCOIC (June 1996) - filename 806ld
  Lead Sheet 819 - 37th EN BN CSM and Commander (June 1996) - filename 819ld
  Lead Sheet 910 - EOD NCO (September 1996) - filename 910ld

The 1999 capture also references an "Interview Notes" file named 37e696a.htm
(endnote 34 in 1999 narrative) which does not follow either standard
filename convention.

This script tests both path structures for every target, prioritizing the
current Khamisiyah III path and falling back to the original Khamisiyah I
path if the primary is not served.
"""
import re
import json
import time
import random
import hashlib
import logging
from pathlib import Path
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser

OUT_ROOT = Path(r'C:\Users\geoff\Downloads\New folder (4)\Transcript run')
OUT_ROOT.mkdir(parents=True, exist_ok=True)

OUT_DIR = OUT_ROOT / 'khamisiyah_leavenworth_extended'
OUT_DIR.mkdir(exist_ok=True)

LOG_FILE = OUT_ROOT / 'fetch_khamisiyah_extended.log'
MANIFEST_FILE = OUT_ROOT / 'khamisiyah_extended_manifest.json'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE), encoding='utf-8')
    ]
)
log = logging.getLogger()

BASE = 'https://gulflink.health.mil'
UA = 'Mozilla/5.0 (compatible; AcademicResearchBot/1.0; Gulf War illness dissertation research)'
HEADERS = {'User-Agent': UA}

OSD_PLACEHOLDER_MD5 = '1c12ed1397c20160e751e4e7af33bcbb'

CONSECUTIVE_MISS_LIMIT = 3
MAX_PAGE_PER_DOC = 50

log.info(f'Output directory: {OUT_DIR}')

rp = RobotFileParser()
rp.set_url(BASE + '/robots.txt')
try:
    rp.read()
    log.info('robots.txt loaded')
except Exception as e:
    log.warning(f'robots.txt: {e}')


# -----------------------------------------------------------------------------
# Path configuration: try multiple case narrative paths for each target
# -----------------------------------------------------------------------------
# The 1999 capture shows the original path structure used /khamisiyah/kham_ref/
# with n01enNNN subdirectories. The 2002 Kham III narrative uses
# /khamisiyah_iii/khamisiyah_iii_refs/ with n70enNNN subdirectories.
#
# Files cited in only the 1997 narrative may still exist at either path, or
# may exist only at the original kham_ref path, or may have been withdrawn.
# -----------------------------------------------------------------------------

# The candidate paths are (case_narrative_slug, refs_slug, endnote_prefix)
CANDIDATE_PATH_BASES = [
    # Khamisiyah III (2002 final narrative) paths
    ('khamisiyah_iii', 'khamisiyah_iii_refs', 'n70en'),
    # Original Khamisiyah I (1997 narrative) paths
    ('khamisiyah', 'kham_ref', 'n01en'),
    # Khamisiyah II (2000 revised narrative) paths
    ('khamisiyah_ii', 'khamisiyah_ii_refs', 'n41en'),
]


TARGETS = [
    # Lead Reports from Khamisiyah III endnotes (2002 narrative) with known
    # endnote_dir mappings
    {'filename_base': '1094ld', 'page_format': 'ld',
     'lead_report': 1094, 'witness': '37th EN BN chemical NCO',
     'interview_date': '1996-06-03',
     'kham_iii_endnote_dirs': ['n70en061', 'n70en075'],
     'kham_i_endnote_dirs': ['n01en028', 'n01en037'],
     'kham_iii_endnotes': [61, 75]},
    {'filename_base': '843ld', 'page_format': 'ld',
     'lead_report': 843, 'witness': '37th EN BN first sergeant',
     'interview_date': '1996-07-12',
     'kham_iii_endnote_dirs': ['n70en074'],
     'kham_i_endnote_dirs': ['n01en023', 'n01en026'],
     'kham_iii_endnotes': [74]},
    {'filename_base': '1223ld', 'page_format': 'ld',
     'lead_report': 1223, 'witness': '37th EN BN NCO',
     'interview_date': '1996-08-08',
     'kham_iii_endnote_dirs': ['n70en090'],
     'kham_i_endnote_dirs': ['n01en044'],
     'kham_iii_endnotes': [90]},
    {'filename_base': '1077ld', 'page_format': 'ld',
     'lead_report': 1077, 'witness': '60th EOD Detachment NCO',
     'interview_date': '1996-10-23',
     'kham_iii_endnote_dirs': ['n70en093', 'n70en095', 'n70en112', 'n70en122'],
     'kham_i_endnote_dirs': ['n01en038', 'n01en039', 'n01en054'],
     'kham_iii_endnotes': [93, 95, 112, 122]},
    {'filename_base': '825ld', 'page_format': 'ld',
     'lead_report': 825, 'witness': '37th EN BN Co B chemical NCO',
     'interview_date': '1996-06-30',
     'kham_iii_endnote_dirs': ['n70en096'],
     'kham_i_endnote_dirs': ['n01en041'],
     'kham_iii_endnotes': [96]},
    {'filename_base': '832ld', 'page_format': 'ld',
     'lead_report': 832, 'witness': '37th EN BN Co B commander',
     'interview_date': '1996-06-28',
     'kham_iii_endnote_dirs': ['n70en097'],
     'kham_i_endnote_dirs': ['n01en041'],
     'kham_iii_endnotes': [97]},
    {'filename_base': '857ld', 'page_format': 'ld',
     'lead_report': 857, 'witness': '37th EN BN intelligence staff NCO',
     'interview_date': '1996-07-02',
     'kham_iii_endnote_dirs': ['n70en099', 'n70en114', 'n70en123'],
     'kham_i_endnote_dirs': ['n01en052', 'n01en054'],
     'kham_iii_endnotes': [99, 114, 123]},
    {'filename_base': '1266ld', 'page_format': 'ld',
     'lead_report': 1266, 'witness': '307th EN BN A Company commander',
     'interview_date': '1997-01-27',
     'kham_iii_endnote_dirs': ['n70en106'],
     'kham_i_endnote_dirs': ['n01en046'],
     'kham_iii_endnotes': [106]},
    {'filename_base': '1053ld', 'page_format': 'ld',
     'lead_report': 1053, 'witness': '37th EN BN operations officer',
     'interview_date': '1996-08-20',
     'kham_iii_endnote_dirs': ['n70en110'],
     'kham_i_endnote_dirs': ['n01en051'],
     'kham_iii_endnotes': [110]},
    {'filename_base': '1221ld', 'page_format': 'ld',
     'lead_report': 1221, 'witness': '307th EN BN liaison officer',
     'interview_date': '1996-09-17',
     'kham_iii_endnote_dirs': ['n70en111'],
     'kham_i_endnote_dirs': ['n01en051'],
     'kham_iii_endnotes': [111]},

    # New targets from the 1999 Wayback capture NOT in Khamisiyah III
    {'filename_base': '806ld', 'page_format': 'ld',
     'lead_report': 806, 'witness': 'EOD NCOIC',
     'interview_date': '1996-06',
     'kham_iii_endnote_dirs': [],
     'kham_i_endnote_dirs': ['n01en029'],
     'kham_iii_endnotes': [],
     'note': 'Cited in 1997 narrative endnote 29 only, not in Kham III'},
    {'filename_base': '819ld', 'page_format': 'ld',
     'lead_report': 819,
     'witness': '37th EN BN CSM and Commander',
     'interview_date': '1996-06',
     'kham_iii_endnote_dirs': [],
     'kham_i_endnote_dirs': ['n01en034'],
     'kham_iii_endnotes': [],
     'note': 'Cited in 1997 narrative endnote 34 only, not in Kham III'},
    {'filename_base': '910ld', 'page_format': 'ld',
     'lead_report': 910, 'witness': 'EOD NCO',
     'interview_date': '1996-09',
     'kham_iii_endnote_dirs': [],
     'kham_i_endnote_dirs': ['n01en054', 'n01en056'],
     'kham_iii_endnotes': [],
     'note': 'Cited in 1997 narrative endnotes 54 and 56 only, not in Kham III'},

    # CMAT-style Lead Reports
    {'filename_base': '7350_048', 'page_format': 'seven',
     'lead_report': 6664, 'witness': 'XVIII Airborne Corps plans officer',
     'interview_date': '1997-10-30',
     'kham_iii_endnote_dirs': ['n70en031'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [31]},
    {'filename_base': '8229_016', 'page_format': 'seven',
     'lead_report': 18591, 'witness': 'CENTCOM plans staff officer',
     'interview_date': '1998-08-17',
     'kham_iii_endnote_dirs': ['n70en040'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [40]},
    {'filename_base': '7350_052', 'page_format': 'seven',
     'lead_report': 6662, 'witness': 'XVIII Airborne Corps operations officer',
     'interview_date': '1997-10-30',
     'kham_iii_endnote_dirs': ['n70en044'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [44]},
    {'filename_base': '7350_053', 'page_format': 'seven',
     'lead_report': 6670, 'witness': '4-64 Armor commander',
     'interview_date': '1997-10-31',
     'kham_iii_endnote_dirs': ['n70en047'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [47]},
    {'filename_base': '9343_027', 'page_format': 'seven',
     'lead_report': 25537, 'witness': 'CENTCOM order of battle analyst',
     'interview_date': '1999-12-09',
     'kham_iii_endnote_dirs': ['n70en056'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [56, 57]},
    {'filename_base': '8006_036', 'page_format': 'seven',
     'lead_report': 1098, 'witness': '37th EN BN Co B commander (first CMAT)',
     'interview_date': '1996-07-02',
     'kham_iii_endnote_dirs': ['n70en062'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [62]},
    {'filename_base': '7311_018', 'page_format': 'seven',
     'lead_report': 6931, 'witness': '37th EN BN Co B commander (second CMAT)',
     'interview_date': '1997-11-07',
     'kham_iii_endnote_dirs': ['n70en062'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [62]},
    {'filename_base': '7162_187', 'page_format': 'seven',
     'lead_report': 11260, 'witness': '60th EOD executive officer',
     'interview_date': '1997-12-20',
     'kham_iii_endnote_dirs': ['n70en082'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [82]},
    {'filename_base': '7162_189', 'page_format': 'seven',
     'lead_report': 11262, 'witness': '37th EN BN Co A platoon sergeant',
     'interview_date': '1997-12-19',
     'kham_iii_endnote_dirs': ['n70en085'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [85]},
    {'filename_base': '0327_023', 'page_format': 'seven',
     'lead_report': 822, 'witness': '37th EN BN executive officer',
     'interview_date': '1996-07-05',
     'kham_iii_endnote_dirs': ['n70en087'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [87]},
    {'filename_base': '0327_022', 'page_format': 'seven',
     'lead_report': 909, 'witness': '37th EN BN Co A chemical NCO',
     'interview_date': '1996-09-12',
     'kham_iii_endnote_dirs': ['n70en098', 'n70en101'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [98, 101]},
    {'filename_base': '7311_009', 'page_format': 'seven',
     'lead_report': 6930, 'witness': '37th EN BN intelligence staff specialist',
     'interview_date': '1997-11-07',
     'kham_iii_endnote_dirs': ['n70en100', 'n70en121', 'n70en200'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [100, 121, 200]},
    {'filename_base': '7350_058', 'page_format': 'seven',
     'lead_report': 6652, 'witness': '60th EOD executive officer (second interview)',
     'interview_date': '1997-10-24',
     'kham_iii_endnote_dirs': ['n70en119', 'n70en120', 'n70en201'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [119, 120, 201]},
    {'filename_base': '7303_041', 'page_format': 'seven',
     'lead_report': 6665, 'witness': '2nd ACR operations action officer',
     'interview_date': '1997-10-30',
     'kham_iii_endnote_dirs': ['n70en131', 'n70en133', 'n70en137', 'n70en138'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [131, 133, 137, 138]},
    {'filename_base': '7350_060', 'page_format': 'seven',
     'lead_report': 6663, 'witness': '84th Engineer Company commander',
     'interview_date': '1997-10-30',
     'kham_iii_endnote_dirs': ['n70en145'],
     'kham_i_endnote_dirs': [],
     'kham_iii_endnotes': [145]},
]


def polite_get(url, stream=False):
    time.sleep(random.uniform(3, 7))
    return requests.get(url, headers=HEADERS, timeout=30, stream=stream)


def classify_response(data):
    if len(data) < 10:
        return False, 'too_short'
    magic = data[:6]
    md5 = hashlib.md5(data).hexdigest()
    if md5 == OSD_PLACEHOLDER_MD5:
        return False, 'placeholder'
    if magic in (b'GIF87a', b'GIF89a'):
        return True, 'valid_gif'
    if magic.startswith(b'<!') or magic.startswith(b'<h') or magic.startswith(b'<H'):
        try:
            text = data.decode('utf-8', errors='replace')
            if ('<img' in text.lower() or 'refimages' in text or
                'Lead Report' in text or 'LEAD REPORT' in text or
                'CMAT' in text or 'INVESTIGATOR' in text):
                return True, 'valid_html_wrapper'
            if ('Not Found' in text or 'File or directory' in text or
                'Deprecated' in text):
                return False, 'dismantled_error'
            return False, 'html_unclassified'
        except Exception:
            return False, 'html_decode_error'
    return False, f'unknown_magic_{magic}'


def build_page_filename(filename_base, page_num, page_format):
    if page_format == 'ld':
        return f'{filename_base}{page_num}'
    elif page_format == 'seven':
        return f'{filename_base}_{page_num:07d}'
    else:
        raise ValueError(f'Unknown page_format: {page_format}')


def build_candidate_urls(target, page_id):
    """Build list of URLs to try for a given page_id across all known paths."""
    urls = []

    # Khamisiyah III paths (2002 narrative)
    for en_dir in target.get('kham_iii_endnote_dirs', []):
        urls.append({
            'url': f'{BASE}/khamisiyah_iii/khamisiyah_iii_refs/{en_dir}/{page_id}.htm',
            'source': 'khamisiyah_iii',
            'en_dir': en_dir,
        })

    # Khamisiyah I paths (1997 original narrative)
    for en_dir in target.get('kham_i_endnote_dirs', []):
        urls.append({
            'url': f'{BASE}/khamisiyah/kham_ref/{en_dir}/{page_id}.htm',
            'source': 'khamisiyah_i',
            'en_dir': en_dir,
        })

    return urls


def try_fetch_page(target, page_num, doc_dir):
    page_id = build_page_filename(target['filename_base'], page_num,
                                   target['page_format'])

    candidates = build_candidate_urls(target, page_id)
    attempts = []

    for candidate in candidates:
        url = candidate['url']
        wrapper_dest = doc_dir / f'{page_id}_{candidate["source"]}.htm'
        gif_dest = doc_dir / f'{page_id}_{candidate["source"]}.gif'

        # Check existing GIF
        if gif_dest.exists():
            existing = gif_dest.read_bytes()
            is_valid, _ = classify_response(existing)
            if is_valid:
                attempts.append({'url': url, 'status': 'skip_existing_valid'})
                return True, {'url': url, 'size': len(existing),
                              'source': candidate['source'], 'path': 'existing'}, attempts

        if not rp.can_fetch(UA, url):
            attempts.append({'url': url, 'status': 'robots_blocked'})
            continue

        try:
            r = polite_get(url)
            if r.status_code == 404:
                attempts.append({'url': url, 'status': 'http_404'})
                continue
            if r.status_code != 200:
                attempts.append({'url': url, 'status': f'http_{r.status_code}'})
                continue

            is_valid, label = classify_response(r.content)
            attempts.append({
                'url': url, 'status': f'http_200_{label}',
                'size': len(r.content),
            })

            if not is_valid:
                continue

            # Save wrapper
            wrapper_dest.write_text(r.text, encoding='utf-8', errors='replace')

            # Find embedded GIF
            soup = BeautifulSoup(r.text, 'html.parser')
            img = soup.find('img')
            if not img or not img.get('src'):
                return True, {'url': url, 'size': len(r.content),
                              'source': candidate['source'], 'path': 'wrapper_only'}, attempts

            img_src = img['src']
            if img_src.startswith('/'):
                img_url = BASE + img_src
            elif img_src.startswith('http'):
                img_url = img_src
            elif img_src.startswith('../'):
                # Resolve relative to endnote dir's parent (the refs dir)
                refs_path = url.rsplit('/', 2)[0]  # Strip endnote dir and filename
                img_url = f'{refs_path}/{img_src[3:]}'
            else:
                img_url = f'{url.rsplit("/", 1)[0]}/{img_src}'

            r2 = polite_get(img_url, stream=True)
            if r2.status_code != 200:
                attempts.append({'url': img_url, 'status': f'http_{r2.status_code}'})
                return True, {'url': url, 'size': len(r.content),
                              'source': candidate['source'],
                              'path': 'wrapper_gif_404'}, attempts

            data = b''
            for chunk in r2.iter_content(8192):
                data += chunk

            gif_valid, gif_label = classify_response(data)
            attempts.append({'url': img_url, 'status': f'http_200_{gif_label}',
                             'size': len(data)})

            if gif_valid:
                gif_dest.write_bytes(data)
                return True, {'url': img_url, 'size': len(data),
                              'source': candidate['source'], 'path': 'gif'}, attempts

            # Wrapper was valid but GIF was placeholder
            return True, {'url': url, 'size': len(r.content),
                          'source': candidate['source'],
                          'path': 'wrapper_only_no_valid_gif'}, attempts

        except Exception as e:
            attempts.append({'url': url, 'status': f'error_{type(e).__name__}'})

    return False, None, attempts


def safe_dir_name(target):
    label = f"LR{target['lead_report']}_{target['witness']}"
    cleaned = re.sub(r'[^A-Za-z0-9_\-]+', '_', label)
    return cleaned.strip('_')[:120]


def fetch_target(target):
    log.info('')
    log.info(f"=== LR {target['lead_report']}: {target['witness']} ===")
    log.info(f"    Date: {target['interview_date']}")
    if target.get('note'):
        log.info(f"    Note: {target['note']}")
    doc_dir = OUT_DIR / safe_dir_name(target)
    doc_dir.mkdir(parents=True, exist_ok=True)

    page_results = []
    consecutive_misses = 0
    for page in range(1, MAX_PAGE_PER_DOC + 1):
        success, saved_info, attempts = try_fetch_page(target, page, doc_dir)

        page_results.append({
            'page': page, 'success': success,
            'saved': saved_info, 'attempts': attempts,
        })

        if success:
            src = saved_info.get('source', 'unknown') if saved_info else 'unknown'
            path_type = saved_info.get('path', 'unknown') if saved_info else 'unknown'
            size = saved_info.get('size', 0) if saved_info else 0
            log.info(f"    Page {page}: {src} {path_type} ({size:,} bytes)")
            consecutive_misses = 0
        else:
            log.info(f"    Page {page}: missing")
            consecutive_misses += 1
            if consecutive_misses >= CONSECUTIVE_MISS_LIMIT:
                log.info(f"  Stopping: {CONSECUTIVE_MISS_LIMIT} consecutive misses")
                break

    captured = sum(1 for r in page_results if r['success'])
    sources_used = set()
    for r in page_results:
        if r['success'] and r.get('saved'):
            sources_used.add(r['saved'].get('source', 'unknown'))
    log.info(f"  Pages captured: {captured}")
    log.info(f"  Sources: {sorted(sources_used)}")

    return {
        'target': target,
        'pages_captured': captured,
        'page_results': page_results,
        'fully_missing': captured == 0,
        'sources_used': sorted(sources_used),
    }


log.info('')
log.info(f'Targets: {len(TARGETS)}')

results = []
for target in TARGETS:
    result = fetch_target(target)
    results.append(result)

# Manifest
with open(MANIFEST_FILE, 'w', encoding='utf-8') as f:
    json.dump({
        'run_timestamp': datetime.utcnow().isoformat() + 'Z',
        'base_url': BASE,
        'targets_total': len(TARGETS),
        'results': [
            {
                'lead_report': r['target']['lead_report'],
                'witness': r['target']['witness'],
                'interview_date': r['target']['interview_date'],
                'filename_base': r['target']['filename_base'],
                'kham_iii_endnotes': r['target'].get('kham_iii_endnotes', []),
                'pages_captured': r['pages_captured'],
                'fully_missing': r['fully_missing'],
                'sources_used': r['sources_used'],
                'note': r['target'].get('note', ''),
            }
            for r in results
        ],
    }, f, indent=2)

log.info('')
log.info('=== Summary ===')
restored = sum(1 for r in results if not r['fully_missing'])
missing = [r for r in results if r['fully_missing']]
log.info(f'Total targets: {len(TARGETS)}')
log.info(f'At least partially restored: {restored}')
log.info(f'Fully missing: {len(missing)}')

# Source-based pattern analysis
log.info('')
log.info('=== Source path analysis ===')
source_counts = {}
for r in results:
    if not r['fully_missing']:
        for src in r['sources_used']:
            source_counts[src] = source_counts.get(src, 0) + 1

for src, count in sorted(source_counts.items()):
    log.info(f'  Content found via {src}: {count} Lead Reports')

if missing:
    log.info('')
    log.info('Fully missing Lead Reports:')
    for r in missing:
        t = r['target']
        note = f" [{t['note']}]" if t.get('note') else ''
        log.info(f"  LR {t['lead_report']}: {t['witness']} ({t['interview_date']}){note}")

log.info('')
log.info(f'Manifest: {MANIFEST_FILE}')
log.info(f'Files: {OUT_DIR}')

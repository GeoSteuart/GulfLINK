"""
An Nasiriyah III multi-document fetcher with missing-document accounting.

Four-output architecture:
  1. RESTORED: Documents successfully fetched, saved in per-document subdirs.
  2. MISSING: Every filename that returned placeholder or 404 is logged with
     metadata (document type, date, unit, endnote citation, attempted URL).
  3. MANIFEST: JSON record of every attempt, for audit and re-run tracking.
  4. AUDIT REPORT: Markdown summary categorizing missing documents by type,
     date range, unit, and citation source to surface restoration patterns.

The missing-document audit is the evidentiary output. It documents which
references from the An Nasiriyah III case narrative were not included in
the March 2026 partial restoration.

Run this after health_mil_an_iii_probe.py identifies the correct path(s).
Update the PATH_CONFIG below based on probe findings before running.
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
from urllib.robotparser import RobotFileParser

OUT_ROOT = Path(r'C:\Users\geoff\Downloads\New folder (4)\Transcript run')
OUT_ROOT.mkdir(parents=True, exist_ok=True)

RESTORED_DIR = OUT_ROOT / 'restored'
RESTORED_DIR.mkdir(exist_ok=True)

MANIFEST_FILE = OUT_ROOT / 'extraction_manifest.json'
MISSING_LOG_FILE = OUT_ROOT / 'missing_documents.json'
AUDIT_REPORT_FILE = OUT_ROOT / 'restoration_audit_report.md'
RUN_LOG_FILE = OUT_ROOT / 'fetcher_run.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(RUN_LOG_FILE), encoding='utf-8')
    ]
)
log = logging.getLogger()

# -----------------------------------------------------------------------------
# Path configuration — update based on probe findings
# -----------------------------------------------------------------------------
BASE = 'https://gulflink.health.mil'
# Primary path for An Nasiriyah III restored content.
# If probe shows content under /an_iii/an_iii_refs/refimages/, use that.
# If probe shows /an_nas_iii/an_nas_iii_refs/refimages/, update accordingly.
PRIMARY_PATH = '/an_iii/an_iii_refs/refimages'
# Fallback paths to try if primary returns placeholder
FALLBACK_PATHS = [
    '/an_nas_iii/an_nas_iii_refs/refimages',
    '/an_iii/refimages',
    '/an_nas_iii/refimages',
]
EXTENSIONS_TO_TRY = ['.gif', '.htm', '.html']

# -----------------------------------------------------------------------------

UA = 'Mozilla/5.0 (compatible; AcademicResearchBot/1.0; Gulf War illness dissertation research)'
HEADERS = {'User-Agent': UA}

OSD_PLACEHOLDER_MD5 = '1c12ed1397c20160e751e4e7af33bcbb'

CONSECUTIVE_MISS_LIMIT = 3
MAX_PAGE_PER_DOC = 300

log.info(f'Output root: {OUT_ROOT}')
log.info(f'Primary path: {BASE}{PRIMARY_PATH}')
log.info(f'Fallback paths: {len(FALLBACK_PATHS)}')

rp = RobotFileParser()
rp.set_url(BASE + '/robots.txt')
try:
    rp.read()
    log.info('robots.txt loaded')
except Exception as e:
    log.warning(f'robots.txt: {e}')


# -----------------------------------------------------------------------------
# Document registry with rich metadata for audit purposes
# -----------------------------------------------------------------------------
# Each entry: dict with keys
#   label: human-readable identifier
#   filename_base: base for page iteration
#   page_format: 'seven' or 'three'
#   doc_type: 'sworn_proceeding', 'lead_sheet_interview', 'early_interview',
#             'eod_evaluation_2000'
#   date: interview date (ISO format)
#   unit: military unit designation
#   role: witness role
#   citing_endnotes: list of endnote numbers in the case narrative that cite
#                    this document
# -----------------------------------------------------------------------------

DOCUMENTS = [
    {
        'label': 'April 10 1997 Eglin AFB transcript',
        'filename_base': 'aug62_001',
        'page_format': 'seven',
        'doc_type': 'sworn_proceeding',
        'date': '1997-04-10',
        'unit': '60th EOD Detachment',
        'role': 'technician',
        'citing_endnotes': [37, 87, 89, 93],
    },
    {
        'label': 'LS 895 146th EOD cdr',
        'filename_base': '7259_079', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1996-08-13', 'unit': '146th EOD Detachment',
        'role': 'commander', 'citing_endnotes': [101],
    },
    {
        'label': 'LS 1079 307 EN C Co plt ldr',
        'filename_base': '8043_002', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1996-10-23', 'unit': '307th EN BN C Company',
        'role': 'platoon leader', 'citing_endnotes': [87, 88, 90],
    },
    {
        'label': 'LS 1080 307 EN XO',
        'filename_base': '8141_017', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1996-12-18', 'unit': '307th EN BN',
        'role': 'executive officer', 'citing_endnotes': [88, 89, 90],
    },
    {
        'label': 'LS 6498 307 EN cdr',
        'filename_base': '7294_049', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-07', 'unit': '307th EN BN',
        'role': 'commander', 'citing_endnotes': [67, 88, 90, 91, 94],
    },
    {
        'label': 'LS 7834 513 MI chem off',
        'filename_base': '7334_024', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-12-03', 'unit': '513th MI Brigade',
        'role': 'chemical officer', 'citing_endnotes': [62, 65],
    },
    {
        'label': 'LS 7938 BH pilot',
        'filename_base': '7105_017', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-04-15', 'unit': 'Black Hawk BW sampling mission',
        'role': 'pilot', 'citing_endnotes': [52, 53, 57],
    },
    {
        'label': 'LS 7945 BH co-pilot',
        'filename_base': '7107_001', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-04-17', 'unit': 'Black Hawk BW sampling mission',
        'role': 'co-pilot', 'citing_endnotes': [52, 55, 56, 58, 77],
    },
    {
        'label': 'LS 7947 82 ABN chem off',
        'filename_base': '7109_024', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-04-21', 'unit': '82nd Airborne Division',
        'role': 'chemical officer', 'citing_endnotes': [22, 24, 75],
    },
    {
        'label': 'LS 7948 307 EN C Co 3rd plt ldr',
        'filename_base': '7109_30', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-04-17', 'unit': '307th EN BN C Company',
        'role': '3rd platoon leader', 'citing_endnotes': [88],
    },
    {
        'label': 'LS 7949 HHC 307 EN S-3',
        'filename_base': '7109_034', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-04-17', 'unit': 'HHC 307th EN BN',
        'role': 'S-3 operations officer', 'citing_endnotes': [66, 68],
    },
    {
        'label': 'LS 9507 BH crew chief',
        'filename_base': '5361_001', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-02-14', 'unit': 'Black Hawk BW sampling mission',
        'role': 'crew chief', 'citing_endnotes': [52, 59, 77],
    },
    {
        'label': 'LS 9522 BH door gunner',
        'filename_base': '6176_016', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-13', 'unit': 'Black Hawk BW sampling mission',
        'role': 'door gunner', 'citing_endnotes': [52, 57, 60, 65, 77],
    },
    {
        'label': 'LS 9918 307 EN C Co 2nd plt ldr',
        'filename_base': '7113_145', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-04-29', 'unit': '307th EN BN C Company',
        'role': '2nd platoon leader', 'citing_endnotes': [88, 90],
    },
    {
        'label': 'LS 10168 Fox vehicle operator',
        'filename_base': '7013_053', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-15', 'unit': 'Fox vehicle team',
        'role': 'operator', 'citing_endnotes': [26, 30, 92],
    },
    {
        'label': 'LS 10358 1703 EOD',
        'filename_base': '7063_004', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-03-03', 'unit': '1703rd EOD Detachment',
        'role': 'member', 'citing_endnotes': [97],
    },
    {
        'label': 'LS 10523 146 EOD cdr',
        'filename_base': '7112_040', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-06-03', 'unit': '146th EOD Detachment',
        'role': 'commander', 'citing_endnotes': [101],
    },
    {
        'label': 'LS 10775 1703 EOD',
        'filename_base': '7118_043', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-06', 'unit': '1703rd EOD Detachment',
        'role': 'member', 'citing_endnotes': [97],
    },
    {
        'label': 'LS 10787 1703 EOD',
        'filename_base': '7121_012', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-27', 'unit': '1703rd EOD Detachment',
        'role': 'member', 'citing_endnotes': [33],
    },
    {
        'label': 'LS 10789 1703 EOD',
        'filename_base': '7121_014', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-14', 'unit': '1703rd EOD Detachment',
        'role': 'member', 'citing_endnotes': [97],
    },
    {
        'label': 'LS 11036 60 EOD sr tech',
        'filename_base': '7140_115', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-23', 'unit': '60th EOD Detachment',
        'role': 'senior technician', 'citing_endnotes': [34, 38, 93, 115],
    },
    {
        'label': 'LS 11043 1703 EOD tm ldr',
        'filename_base': '7141_081', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-05-29', 'unit': '1703rd EOD Detachment',
        'role': 'team leader', 'citing_endnotes': [97],
    },
    {
        'label': 'LS 11249 307 EN C Co plt ldr',
        'filename_base': '7162_175', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-12-18', 'unit': '307th EN BN C Company',
        'role': 'platoon leader', 'citing_endnotes': [90],
    },
    {
        'label': 'LS 11325 307 EN C Co eng',
        'filename_base': '7162_255', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-06-11', 'unit': '307th EN BN C Company',
        'role': 'engineer', 'citing_endnotes': [36],
    },
    {
        'label': 'LS 11833 307 EN eng',
        'filename_base': '7162_792', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-11-04', 'unit': '307th EN BN',
        'role': 'engineer', 'citing_endnotes': [79],
    },
    {
        'label': 'LS 11875 307 EN B Co 1st plt eng',
        'filename_base': '7162_837', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-06-13', 'unit': '307th EN BN B Company',
        'role': '1st platoon engineer', 'citing_endnotes': [35, 89],
    },
    {
        'label': 'LS 12002 307 EN S-2',
        'filename_base': '7162_968', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-09-15', 'unit': '307th EN BN',
        'role': 'S-2 intelligence officer', 'citing_endnotes': [28, 67, 88, 90],
    },
    {
        'label': 'LS 12100 505 PIR',
        'filename_base': '7175_203', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-07-14', 'unit': '505th Parachute Infantry Regiment',
        'role': 'member', 'citing_endnotes': [42, 46, 67],
    },
    {
        'label': 'LS 13189 Fox vehicle cdr',
        'filename_base': '7290_041', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-10-21', 'unit': 'Fox vehicle team',
        'role': 'commander', 'citing_endnotes': [26, 92],
    },
    {
        'label': 'LS 13573 9 Chem Co sr enl',
        'filename_base': '7329_018', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-12-01', 'unit': '9th Chemical Company',
        'role': 'senior enlisted BW sample team',
        'citing_endnotes': [64],
    },
    {
        'label': 'LS 13654 JCMEC cdr',
        'filename_base': '7339_003', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1997-12-16', 'unit': 'JCMEC',
        'role': 'commander', 'citing_endnotes': [70],
    },
    {
        'label': 'LS 14102 JCMEC ops off',
        'filename_base': '7344_033', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1998-01-09', 'unit': 'JCMEC',
        'role': 'operations officer', 'citing_endnotes': [71],
    },
    {
        'label': 'LS 14159 USAMRIID SPB chief',
        'filename_base': '8015_006', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1998-04-07', 'unit': 'USAMRIID',
        'role': 'Special Pathogens Branch chief',
        'citing_endnotes': [73],
    },
    {
        'label': 'LS 15388 AFMIC med intel off',
        'filename_base': '8069_011', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1998-03-09', 'unit': 'AFMIC',
        'role': 'medical intelligence officer',
        'citing_endnotes': [63, 65],
    },
    {
        'label': 'LS 17555 Fox vehicle crew chief',
        'filename_base': '8180_045', 'page_format': 'seven',
        'doc_type': 'lead_sheet_interview',
        'date': '1999-10-04', 'unit': 'Fox vehicle team',
        'role': 'crew chief', 'citing_endnotes': [26, 92],
    },
    {
        'label': 'LS 26772 767 Ord Co EOD',
        'filename_base': '26772', 'page_format': 'three',
        'doc_type': 'eod_evaluation_2000',
        'date': '2000-05-23', 'unit': '767th Ordnance Company (EOD)',
        'role': 'company commanders and sergeants',
        'citing_endnotes': [40],
    },
    {
        'label': 'LS 26854 52 Ord Gp EOD',
        'filename_base': '26854', 'page_format': 'three',
        'doc_type': 'eod_evaluation_2000',
        'date': '2000-05-18', 'unit': '52nd Ordnance Group (EOD)',
        'role': 'operations officer', 'citing_endnotes': [40],
    },
    {
        'label': 'LS 26933 NGIC chem wpns spec',
        'filename_base': '26933', 'page_format': 'three',
        'doc_type': 'eod_evaluation_2000',
        'date': '2000-05-25', 'unit': 'NGIC',
        'role': 'chemical weapons specialist',
        'citing_endnotes': [40],
    },
    {
        'label': 'LS 26934 NAVEODTC munitions',
        'filename_base': '26934', 'page_format': 'three',
        'doc_type': 'eod_evaluation_2000',
        'date': '2000-06-05', 'unit': 'NAVEODTC',
        'role': 'ground munitions technologist',
        'citing_endnotes': [40],
    },
    {
        'label': 'LS 27064 741 Ord Co EOD',
        'filename_base': '27064', 'page_format': 'three',
        'doc_type': 'eod_evaluation_2000',
        'date': '2000-06-12', 'unit': '741st Ordnance Company (EOD)',
        'role': 'first sergeant', 'citing_endnotes': [40],
    },
    {
        'label': 'LS 27636 Battelle sr scientist',
        'filename_base': '27636', 'page_format': 'three',
        'doc_type': 'eod_evaluation_2000',
        'date': '2000-08-31', 'unit': 'Battelle',
        'role': 'senior scientist', 'citing_endnotes': [39],
    },
    {
        'label': '82 ABN chem off early interview',
        'filename_base': '1998043', 'page_format': 'seven',
        'doc_type': 'early_interview',
        'date': '1996-06-17', 'unit': '82nd Airborne Division',
        'role': 'chemical officer',
        'citing_endnotes': [14, 18, 22, 24, 75],
    },
]

log.info(f'Documents in registry: {len(DOCUMENTS)}')


def polite_get(url, stream=False):
    time.sleep(random.uniform(3, 7))
    return requests.get(url, headers=HEADERS, timeout=30, stream=stream)


def build_page_filename(filename_base, page_num, page_format):
    if page_format == 'seven':
        return f'{filename_base}_{page_num:07d}'
    elif page_format == 'three':
        return f'{filename_base}{page_num:03d}'
    else:
        raise ValueError(f'Unknown page_format: {page_format}')


def classify_response(data):
    """Return tuple (is_valid, result_label)."""
    if len(data) < 10:
        return False, 'too_short'
    magic = data[:6]
    md5 = hashlib.md5(data).hexdigest()
    if md5 == OSD_PLACEHOLDER_MD5:
        return False, 'osd_placeholder'
    if magic in (b'GIF87a', b'GIF89a'):
        return True, 'valid_gif'
    if magic.startswith(b'<!') or magic.startswith(b'<h') or magic.startswith(b'<H'):
        try:
            text = data.decode('utf-8', errors='replace')
            if ('Lead Report' in text or 'LEAD REPORT' in text or
                'CMAT' in text or 'INVESTIGATOR' in text):
                return True, 'valid_ocr_html'
            if ('Not Found' in text or 'File or directory' in text or
                'Deprecated' in text or 'not been activated' in text):
                return False, 'dismantled_error'
            return False, 'html_unclassified'
        except Exception:
            return False, 'html_decode_error'
    return False, f'unknown_magic_{magic}'


def try_fetch_page(doc_dir, filename_base, page_num, page_format, attempt_log):
    """Try primary and fallback paths, return (success, saved_path_info, attempts)."""
    page_id = build_page_filename(filename_base, page_num, page_format)
    attempts = []

    all_paths = [PRIMARY_PATH] + FALLBACK_PATHS
    for path in all_paths:
        for ext in EXTENSIONS_TO_TRY:
            url = f'{BASE}{path}/{page_id}{ext}'
            dest = doc_dir / f'{page_id}{ext}'

            if dest.exists():
                existing = dest.read_bytes()
                is_valid, _ = classify_response(existing)
                if is_valid:
                    attempts.append({'url': url, 'status': 'skip_existing_valid'})
                    return True, {'url': url, 'size': len(existing), 'ext': ext}, attempts

            if not rp.can_fetch(UA, url):
                attempts.append({'url': url, 'status': 'robots_blocked'})
                continue

            try:
                r = polite_get(url, stream=True)
                if r.status_code == 404:
                    attempts.append({'url': url, 'status': 'http_404'})
                    continue
                if r.status_code != 200:
                    attempts.append({'url': url, 'status': f'http_{r.status_code}'})
                    continue

                data = b''
                for chunk in r.iter_content(8192):
                    data += chunk

                is_valid, label = classify_response(data)
                attempts.append({
                    'url': url, 'status': f'http_200_{label}',
                    'size': len(data),
                })

                if is_valid:
                    dest.write_bytes(data)
                    return True, {'url': url, 'size': len(data), 'ext': ext}, attempts

            except Exception as e:
                attempts.append({'url': url, 'status': f'error_{type(e).__name__}'})

    return False, None, attempts


def safe_dir_name(label):
    cleaned = re.sub(r'[^A-Za-z0-9_\-]+', '_', label)
    return cleaned.strip('_')


def fetch_document(doc):
    log.info('')
    log.info(f'=== {doc["label"]} ===')
    doc_dir = RESTORED_DIR / safe_dir_name(doc['label'])
    doc_dir.mkdir(parents=True, exist_ok=True)

    page_results = []
    consecutive_misses = 0
    for page in range(1, MAX_PAGE_PER_DOC + 1):
        success, saved_info, attempts = try_fetch_page(
            doc_dir, doc['filename_base'], page, doc['page_format'], None
        )

        page_results.append({
            'page': page,
            'success': success,
            'saved': saved_info,
            'attempts': attempts,
        })

        if success:
            log.info(f'    Page {page}: saved via {saved_info["ext"]} ({saved_info["size"]:,} bytes)')
            consecutive_misses = 0
        else:
            log.info(f'    Page {page}: missing (tried {len(attempts)} URLs)')
            consecutive_misses += 1
            if consecutive_misses >= CONSECUTIVE_MISS_LIMIT:
                log.info(f'  Stopping: {CONSECUTIVE_MISS_LIMIT} consecutive misses')
                break

    captured = sum(1 for r in page_results if r['success'])
    log.info(f'  Pages captured: {captured} / {len(page_results)}')

    return {
        'document': doc,
        'pages_captured': captured,
        'pages_attempted': len(page_results),
        'page_results': page_results,
        'fully_missing': captured == 0,
    }


# Execute fetch for all documents
log.info('')
log.info('Starting extraction sweep...')

extraction_results = []
for doc in DOCUMENTS:
    result = fetch_document(doc)
    extraction_results.append(result)

# Write manifest
with open(MANIFEST_FILE, 'w', encoding='utf-8') as f:
    json.dump({
        'run_timestamp': datetime.utcnow().isoformat() + 'Z',
        'base_url': BASE,
        'primary_path': PRIMARY_PATH,
        'fallback_paths': FALLBACK_PATHS,
        'documents_total': len(DOCUMENTS),
        'results': [
            {
                'label': r['document']['label'],
                'filename_base': r['document']['filename_base'],
                'doc_type': r['document']['doc_type'],
                'date': r['document']['date'],
                'unit': r['document']['unit'],
                'citing_endnotes': r['document']['citing_endnotes'],
                'pages_captured': r['pages_captured'],
                'pages_attempted': r['pages_attempted'],
                'fully_missing': r['fully_missing'],
            }
            for r in extraction_results
        ],
    }, f, indent=2)
log.info(f'Manifest written: {MANIFEST_FILE}')

# Write missing documents log
fully_missing = [r for r in extraction_results if r['fully_missing']]
with open(MISSING_LOG_FILE, 'w', encoding='utf-8') as f:
    json.dump({
        'run_timestamp': datetime.utcnow().isoformat() + 'Z',
        'total_documents_attempted': len(DOCUMENTS),
        'fully_missing_count': len(fully_missing),
        'missing_documents': [
            {
                'label': r['document']['label'],
                'filename_base': r['document']['filename_base'],
                'doc_type': r['document']['doc_type'],
                'date': r['document']['date'],
                'unit': r['document']['unit'],
                'role': r['document']['role'],
                'citing_endnotes': r['document']['citing_endnotes'],
                'attempted_urls': r['page_results'][0]['attempts'] if r['page_results'] else [],
            }
            for r in fully_missing
        ],
    }, f, indent=2)
log.info(f'Missing documents log: {MISSING_LOG_FILE}')

# Write audit report
def generate_audit_report(results):
    lines = []
    lines.append('# An Nasiriyah III Extraction Audit Report')
    lines.append('')
    lines.append(f'Run timestamp: {datetime.utcnow().isoformat()}Z')
    lines.append(f'Base URL: {BASE}')
    lines.append(f'Primary path: {PRIMARY_PATH}')
    lines.append('')

    total = len(results)
    fully_restored = sum(1 for r in results if not r['fully_missing'])
    fully_missing_list = [r for r in results if r['fully_missing']]

    lines.append('## Summary')
    lines.append('')
    lines.append(f'Total documents attempted: {total}')
    lines.append(f'At least partially restored: {fully_restored}')
    lines.append(f'Fully missing: {len(fully_missing_list)}')
    lines.append('')

    if not fully_missing_list:
        lines.append('All documents returned at least one page of valid content.')
        lines.append('No restoration gaps identified in this run.')
        return '\n'.join(lines)

    lines.append('## Missing Document Analysis')
    lines.append('')
    lines.append('The following references from the An Nasiriyah III case narrative')
    lines.append('returned no valid content at any tested path. These documents were')
    lines.append('cited in the official case narrative endnotes but were not included')
    lines.append('in the March 2026 partial restoration of GulfLINK.')
    lines.append('')

    # Group by document type
    by_type = {}
    for r in fully_missing_list:
        dt = r['document']['doc_type']
        by_type.setdefault(dt, []).append(r)

    lines.append('### Missing by document type')
    lines.append('')
    for dt, items in sorted(by_type.items()):
        lines.append(f'**{dt}**: {len(items)} missing')
        for r in items:
            d = r['document']
            endnotes = ', '.join(str(n) for n in d['citing_endnotes'])
            lines.append(f'  - {d["label"]} ({d["date"]}, {d["unit"]}, endnotes {endnotes})')
        lines.append('')

    # Group by unit
    by_unit = {}
    for r in fully_missing_list:
        u = r['document']['unit']
        by_unit.setdefault(u, []).append(r)

    lines.append('### Missing by unit')
    lines.append('')
    for u, items in sorted(by_unit.items()):
        lines.append(f'**{u}**: {len(items)} missing')
    lines.append('')

    # Group by year
    by_year = {}
    for r in fully_missing_list:
        y = r['document']['date'][:4]
        by_year.setdefault(y, []).append(r)

    lines.append('### Missing by year')
    lines.append('')
    for y, items in sorted(by_year.items()):
        lines.append(f'**{y}**: {len(items)} missing')
    lines.append('')

    # Endnote coverage
    lines.append('### Endnote citation coverage')
    lines.append('')
    missing_endnotes = set()
    for r in fully_missing_list:
        missing_endnotes.update(r['document']['citing_endnotes'])
    lines.append(f'Endnotes in the case narrative citing missing documents: {len(missing_endnotes)}')
    lines.append(f'Endnote numbers affected: {sorted(missing_endnotes)}')
    lines.append('')

    lines.append('### Pattern observations')
    lines.append('')
    lines.append('Review the groupings above for patterns that may indicate whether')
    lines.append('the restoration excluded documents systematically (e.g., by unit,')
    lines.append('interview date, investigative thread, or endnote cluster) versus')
    lines.append('random attrition.')

    return '\n'.join(lines)

audit_report = generate_audit_report(extraction_results)
AUDIT_REPORT_FILE.write_text(audit_report, encoding='utf-8')
log.info(f'Audit report: {AUDIT_REPORT_FILE}')

# Final summary to console
log.info('')
log.info('=== Extraction Complete ===')
log.info(f'Total documents attempted: {len(DOCUMENTS)}')
restored = sum(1 for r in extraction_results if not r['fully_missing'])
missing = len(extraction_results) - restored
log.info(f'At least partially restored: {restored}')
log.info(f'Fully missing (evidentiary finding): {missing}')
log.info('')
log.info(f'Review {AUDIT_REPORT_FILE} for the restoration pattern analysis.')

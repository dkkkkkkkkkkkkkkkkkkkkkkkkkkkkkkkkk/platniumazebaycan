# PLATNIUM AZƏRBAYCAN — single-file Flask 24/7 HLS channel + admin
# Everything except Flask itself lives in this file: HTML, CSS, JS, SQLite, HLS proxy and queue engine.
# Start: python app.py
# Render: set Start Command to: python app.py

import os, sys, json, re, time, base64, hmac, hashlib, sqlite3, threading, socket, ipaddress
from collections import OrderedDict
from urllib.parse import urljoin, urlparse, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

try:
    from flask import Flask, Response, jsonify, request, make_response
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'Flask==3.1.2'])
    from flask import Flask, Response, jsonify, request, make_response

# ------------------------------
# Configuration / admin details
# ------------------------------
CHANNEL_NAME = os.getenv('CHANNEL_NAME', 'PLATNIUM AZƏRBAYCAN')
ADMIN_USER = os.getenv('ADMIN_USER', 'admin')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'ekber20132014')
SESSION_SECRET = os.getenv('SESSION_SECRET', 'platinum-azerbaijan-change-this-secret-2026')
PORT = int(os.getenv('PORT', '3000'))
HOST = os.getenv('HOST', '0.0.0.0')
DB_PATH = os.getenv('DB_PATH', 'platinum.sqlite3')
PUBLIC_BASE_URL = os.getenv('PUBLIC_BASE_URL', '').rstrip('/')
POLL_SECONDS = max(1.0, float(os.getenv('PLAYLIST_REFRESH_SECONDS', '3')))
UPSTREAM_TIMEOUT = max(5, int(os.getenv('UPSTREAM_TIMEOUT_SECONDS', '20')))
LIVE_WINDOW = max(3, int(os.getenv('LIVE_WINDOW_SEGMENTS', '7')))
LIVE_PREFETCH = max(1, int(os.getenv('LIVE_PREFETCH_SEGMENTS', '2')))
CACHE_TTL = max(5, int(os.getenv('SEGMENT_CACHE_TTL_SECONDS', '120')))
CACHE_MAX_BYTES = max(10 * 1024 * 1024, int(os.getenv('SEGMENT_CACHE_MAX_BYTES', str(256 * 1024 * 1024))))
ALLOW_PRIVATE_UPSTREAM = os.getenv('ALLOW_PRIVATE_UPSTREAM', '0').lower() in ('1', 'true', 'yes')

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024

# ------------------------------
# DB helpers
# ------------------------------
DB_LOCK = threading.RLock()

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn

with DB_LOCK:
    conn = db()
    conn.executescript('''
      CREATE TABLE IF NOT EXISTS playlist_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT NOT NULL UNIQUE,
        position INTEGER NOT NULL,
        created_at REAL NOT NULL
      );
      CREATE INDEX IF NOT EXISTS playlist_queue_position_idx ON playlist_queue(position, id);
    ''')
    conn.commit(); conn.close()

# ------------------------------
# In-memory runtime state/cache
# ------------------------------
STATE_LOCK = threading.RLock()
STATE = {
    'current': None,
    'last_error': '',
    'started_at': time.time(),
    'next_virtual_sequence': 0,
    'revision': 0,
}
CACHE_LOCK = threading.RLock()
SEGMENT_CACHE = OrderedDict()  # url -> (expires, bytes, content_type)
CACHE_BYTES = 0
ENGINE_STARTED = False

# ------------------------------
# Security / utility
# ------------------------------
def now():
    return time.time()

def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip('=')

def b64udata(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + '=' * (-len(value) % 4))

def sign(value: str) -> str:
    return b64u(hmac.new(SESSION_SECRET.encode(), value.encode(), hashlib.sha256).digest())

def make_session(username: str) -> str:
    payload = b64u(json.dumps({'u': username, 't': int(now())}, separators=(',', ':')).encode())
    return payload + '.' + sign(payload)

def valid_session():
    raw = request.cookies.get('platinum_session', '')
    try:
        payload, signature = raw.split('.', 1)
        expected = sign(payload)
        if not hmac.compare_digest(signature, expected): return False
        data = json.loads(b64udata(payload).decode())
        return data.get('u') == ADMIN_USER and now() - int(data.get('t', 0)) < 30 * 86400
    except Exception:
        return False

def auth_required():
    if not valid_session():
        return jsonify(ok=False, error='AUTH_REQUIRED'), 401
    return None

def safe_url(raw):
    try:
        u = urlparse(str(raw).strip())
        if u.scheme not in ('http', 'https') or not u.hostname:
            return None
        if not ALLOW_PRIVATE_UPSTREAM:
            for info in socket.getaddrinfo(u.hostname, None):
                ip = ipaddress.ip_address(info[4][0])
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                    return None
        return u.geturl()
    except Exception:
        return None

def title_from_url(url):
    try:
        path = urlparse(url).path
        name = path.rstrip('/').split('/')[-1] or 'M3U8 video'
        name = re.sub(r'\.(m3u8|m3u|mp4|ts|m4s)$', '', name, flags=re.I)
        name = re.sub(r'[._-]+', ' ', name)
        return re.sub(r'\s+', ' ', name).strip() or 'M3U8 video'
    except Exception:
        return 'M3U8 video'

def parse_input(text):
    items, extinf = [], ''
    for raw in str(text or '').splitlines():
        line = raw.strip()
        if not line: continue
        if line.upper().startswith('#EXTINF:'):
            extinf = line.split(',', 1)[1].strip() if ',' in line else ''
            continue
        if line.startswith('#'): continue
        title, url = extinf, line
        if '|' in line:
            left, right = line.split('|', 1)
            if right.strip():
                title, url = left.strip() or extinf, right.strip()
        u = safe_url(url)
        if u: items.append({'title': title or title_from_url(u), 'url': u})
        extinf = ''
    return items

# ------------------------------
# HLS resolver/parser
# ------------------------------
def upstream_get(url, accept='*/*'):
    u = safe_url(url)
    if not u: raise ValueError('Source URL is blocked or invalid')
    req = Request(u, headers={
        'User-Agent': 'PLATNIUM-AZERBAYCAN-HLS/2.0',
        'Accept': accept,
        'Accept-Encoding': 'identity',
        'Connection': 'close',
    })
    return urlopen(req, timeout=UPSTREAM_TIMEOUT)

def parse_attr_list(raw):
    out = {}
    # Keeps quoted commas safe enough for standard HLS attribute values.
    for m in re.finditer(r'([A-Z0-9-]+)=((?:"(?:[^"]*)")|(?:[^,]*))(?:,|$)', raw):
        key, value = m.group(1), m.group(2).strip()
        out[key] = value[1:-1] if value.startswith('"') and value.endswith('"') else value
    return out

def fetch_text_playlist(url, depth=0):
    if depth > 4: raise ValueError('Master playlist nesting is too deep')
    try:
        with upstream_get(url, 'application/vnd.apple.mpegurl, application/x-mpegURL, */*') as r:
            text = r.read().decode('utf-8', errors='replace')
            final_url = r.geturl() or url
    except HTTPError as e:
        raise ValueError(f'Upstream HTTP {e.code}')
    except URLError as e:
        raise ValueError(f'Upstream connection failed: {e.reason}')
    if '#EXTM3U' not in text:
        raise ValueError('Source is not an HLS/M3U8 playlist')

    # Master playlist -> highest bandwidth variant.
    if re.search(r'#EXT-X-STREAM-INF:', text, re.I):
        lines = [x.strip() for x in text.splitlines()]
        variants = []
        for i, line in enumerate(lines):
            if not line.upper().startswith('#EXT-X-STREAM-INF:'): continue
            attrs = parse_attr_list(line.split(':', 1)[1])
            try: bw = int(attrs.get('AVERAGE-BANDWIDTH') or attrs.get('BANDWIDTH') or 0)
            except: bw = 0
            uri = ''
            for nxt in lines[i+1:]:
                if nxt and not nxt.startswith('#'):
                    uri = urljoin(final_url, nxt); break
            if uri and not attrs.get('VIDEO-RANGE') == 'I': variants.append((bw, uri))
        if not variants: raise ValueError('Master playlist contains no playable variant')
        variants.sort(reverse=True)
        return fetch_text_playlist(variants[0][1], depth + 1)

    lines = [x.strip() for x in text.splitlines()]
    target = 6.0
    media_seq = 0
    has_end = bool(re.search(r'#EXT-X-ENDLIST', text, re.I))
    segments = []
    duration = None
    title = ''
    map_url = None
    key_url = None
    key_method = 'NONE'
    for line in lines:
        up = line.upper()
        if up.startswith('#EXT-X-TARGETDURATION:'):
            try: target = float(line.split(':', 1)[1])
            except: pass
        elif up.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            try: media_seq = int(line.split(':', 1)[1])
            except: media_seq = 0
        elif up.startswith('#EXTINF:'):
            value = line.split(':', 1)[1]
            d, _, t = value.partition(',')
            try: duration = float(d)
            except: duration = target
            title = t.strip()
        elif up.startswith('#EXT-X-MAP:'):
            a = parse_attr_list(line.split(':', 1)[1])
            if a.get('URI'): map_url = urljoin(final_url, a['URI'])
        elif up.startswith('#EXT-X-KEY:'):
            a = parse_attr_list(line.split(':', 1)[1])
            key_method = a.get('METHOD', 'NONE')
            key_url = urljoin(final_url, a['URI']) if a.get('URI') else None
        elif line and not line.startswith('#'):
            segments.append({
                'url': urljoin(final_url, line),
                'duration': float(duration or target),
                'title': title,
                'map_url': map_url,
                'key_url': key_url if key_method == 'AES-128' else None,
            })
            duration, title = None, ''
    if not segments: raise ValueError('M3U8 has no media segments')
    total = sum(x['duration'] for x in segments)
    return {
        'segments': segments,
        'total_duration': total,
        'target_duration': target,
        'end_list': has_end,
        'media_sequence': media_seq,
        'source_url': final_url,
    }

# ------------------------------
# Queue engine
# ------------------------------
def queue_rows():
    with DB_LOCK:
        c = db(); rows = c.execute('SELECT id,title,url,position,created_at FROM playlist_queue ORDER BY position,id').fetchall(); c.close()
    return [dict(r) for r in rows]

def normalize_positions():
    with DB_LOCK:
        c = db(); rows = c.execute('SELECT id FROM playlist_queue ORDER BY position,id').fetchall()
        for i, r in enumerate(rows): c.execute('UPDATE playlist_queue SET position=? WHERE id=?', (i, r['id']))
        c.commit(); c.close()

def first_queue_item():
    with DB_LOCK:
        c = db(); row = c.execute('SELECT id,title,url,position FROM playlist_queue ORDER BY position,id LIMIT 1').fetchone(); c.close()
    return dict(row) if row else None

def delete_queue_id(item_id):
    with DB_LOCK:
        c = db(); c.execute('DELETE FROM playlist_queue WHERE id=?', (str(item_id),)); c.commit(); c.close()
    normalize_positions()

def start_item(item):
    with STATE_LOCK:
        try:
            media = fetch_text_playlist(item['url'])
            STATE['current'] = {
                'id': str(item['id']),
                'title': item['title'],
                'source_url': item['url'],
                'segments': media['segments'],
                'total_duration': media['total_duration'],
                'target_duration': media['target_duration'],
                'end_list': media['end_list'],
                'started_at': now(),
                'virtual_start_sequence': STATE['next_virtual_sequence'],
                'last_refresh': now(),
            }
            STATE['next_virtual_sequence'] += len(media['segments'])
            STATE['last_error'] = ''
            if not media['end_list']:
                STATE['last_error'] = 'Mənbədə #EXT-X-ENDLIST yoxdur; canlı/sonsuz playlist kimi saxlanılır.'
            return True
        except Exception as e:
            STATE['last_error'] = f"{item['title']}: {e}"
            try: delete_queue_id(item['id'])
            except: pass
            STATE['current'] = None
            return False

def skip_current():
    with STATE_LOCK:
        current = STATE.get('current')
        if current: delete_queue_id(current['id'])
        STATE['current'] = None
        nxt = first_queue_item()
        if nxt: start_item(nxt)

def engine_tick():
    with STATE_LOCK:
        current = STATE.get('current')
        if not current:
            item = first_queue_item()
            if item: start_item(item)
            return
        if current['end_list']:
            elapsed = now() - current['started_at']
            if elapsed >= current['total_duration'] + max(1, current['target_duration'] * 0.25):
                delete_queue_id(current['id'])
                STATE['current'] = None
                nxt = first_queue_item()
                if nxt: start_item(nxt)
                return
        if now() - current.get('last_refresh', 0) > 15:
            try:
                media = fetch_text_playlist(current['source_url'])
                current['segments'] = media['segments']
                current['total_duration'] = media['total_duration']
                current['target_duration'] = media['target_duration']
                current['end_list'] = media['end_list']
                current['last_refresh'] = now()
                if media['end_list']: STATE['last_error'] = ''
            except Exception:
                current['last_refresh'] = now()

def engine_loop():
    global ENGINE_STARTED
    ENGINE_STARTED = True
    while True:
        try: engine_tick()
        except Exception as e: STATE['last_error'] = str(e)
        time.sleep(POLL_SECONDS)

def start_engine():
    global ENGINE_STARTED
    if ENGINE_STARTED: return
    t = threading.Thread(target=engine_loop, daemon=True, name='platinum-hls-engine')
    t.start()

# ------------------------------
# Segment cache/proxy
# ------------------------------
def cache_get(url):
    with CACHE_LOCK:
        item = SEGMENT_CACHE.get(url)
        if not item: return None
        expires, body, ctype = item
        if expires <= now():
            SEGMENT_CACHE.pop(url, None)
            return None
        SEGMENT_CACHE.move_to_end(url)
        return body, ctype

def cache_put(url, body, ctype):
    global CACHE_BYTES
    if len(body) > 32 * 1024 * 1024: return
    with CACHE_LOCK:
        old = SEGMENT_CACHE.pop(url, None)
        if old: CACHE_BYTES -= len(old[1])
        SEGMENT_CACHE[url] = (now() + CACHE_TTL, body, ctype)
        CACHE_BYTES += len(body)
        while CACHE_BYTES > CACHE_MAX_BYTES and SEGMENT_CACHE:
            _, v = SEGMENT_CACHE.popitem(last=False)
            CACHE_BYTES -= len(v[1])

def fetch_binary(url):
    cached = cache_get(url)
    if cached: return cached
    try:
        with upstream_get(url) as r:
            body = r.read()
            ctype = r.headers.get('Content-Type', 'video/mp2t')
    except HTTPError as e:
        raise ValueError(f'Upstream HTTP {e.code}')
    except URLError as e:
        raise ValueError(f'Upstream connection failed: {e.reason}')
    cache_put(url, body, ctype)
    return body, ctype

# ------------------------------
# HLS playlist generator
# ------------------------------
def current_snapshot():
    with STATE_LOCK:
        c = STATE.get('current')
        if not c: return None
        return dict(c)

def elapsed_index(segments, elapsed):
    acc = 0
    for i, s in enumerate(segments):
        acc += float(s.get('duration', 6))
        if elapsed < acc: return i
    return max(0, len(segments) - 1)

def base_url():
    return PUBLIC_BASE_URL or request.host_url.rstrip('/')

def build_live_playlist():
    c = current_snapshot()
    lines = ['#EXTM3U', '#EXT-X-VERSION:6', '#EXT-X-TARGETDURATION:6', '#EXT-X-MEDIA-SEQUENCE:0', '#EXT-X-INDEPENDENT-SEGMENTS', '#EXT-X-PLAYLIST-TYPE:EVENT']
    if not c: return '\n'.join(lines) + '\n'
    elapsed = max(0, now() - c['started_at'])
    idx = elapsed_index(c['segments'], elapsed)
    start = max(0, idx - LIVE_WINDOW + 1)
    end = min(len(c['segments']) - 1, idx + LIVE_PREFETCH)
    lines[2] = f"#EXT-X-TARGETDURATION:{max(1, int(round(c['target_duration'])))}"
    lines[3] = f"#EXT-X-MEDIA-SEQUENCE:{max(0, c['virtual_start_sequence'] + start)}"
    previous_map = previous_key = None
    if start > 0: lines.append('#EXT-X-DISCONTINUITY')
    for i in range(start, end + 1):
        s = c['segments'][i]
        if s.get('map_url') and s['map_url'] != previous_map:
            lines.append(f"#EXT-X-MAP:URI=\"{base_url()}/hls/resource/{quote(c['id'], safe='')}/map/{i}\"")
            previous_map = s['map_url']
        if s.get('key_url') and s['key_url'] != previous_key:
            lines.append(f"#EXT-X-KEY:METHOD=AES-128,URI=\"{base_url()}/hls/resource/{quote(c['id'], safe='')}/key/{i}\"")
            previous_key = s['key_url']
        lines.append(f"#EXTINF:{float(s.get('duration', c['target_duration'])):.3f},{s.get('title','')}")
        lines.append(f"{base_url()}/hls/segment/{quote(c['id'], safe='')}/{i}")
    return '\n'.join(lines) + '\n'

# ------------------------------
# Embedded front-end
# ------------------------------
CSS = r'''
:root{--bg:#07090d;--card:#0e1219;--line:#1a2230;--text:#f4f6fb;--muted:#8791a5;--accent:#e7b85a;--danger:#d95c5c;--ok:#45d483}
*{box-sizing:border-box}html,body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif}body{min-height:100vh}button,input,textarea{font:inherit}button{cursor:pointer}.hidden{display:none!important}.muted,.hint,.tiny{color:var(--muted)}
.watch{min-height:100vh;background:radial-gradient(circle at 50% -20%,#18202e,transparent 50%),#050608}.watch-shell{max-width:1500px;margin:auto;padding:22px}.player-card{position:relative;aspect-ratio:16/9;background:#000;border-radius:22px;overflow:hidden;box-shadow:0 20px 80px #0008}.player-card video{width:100%;height:100%;object-fit:contain;background:#000}.shade{position:absolute;inset:0;background:linear-gradient(180deg,#0009,transparent 20%,transparent 70%,#000c);pointer-events:none}.brand{position:absolute;left:28px;top:24px;font-weight:900;letter-spacing:.08em;font-size:18px}.live{position:absolute;right:26px;top:24px;border:1px solid #ffffff22;background:#0007;padding:9px 12px;border-radius:999px;font-size:12px;font-weight:800}.live span,.status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#ff5151;box-shadow:0 0 16px #ff5151;margin-right:7px}.now{position:absolute;right:26px;bottom:26px;text-align:right}.now small{display:block;font-size:10px;letter-spacing:.18em;color:#d7dbea99;margin-bottom:5px}.now strong{display:block;font-size:24px;max-width:min(60vw,700px)}.now time{display:block;color:#d7dbea99;margin-top:5px;font-size:12px}.big-play{position:absolute;inset:50% auto auto 50%;transform:translate(-50%,-50%);width:76px;height:76px;border-radius:50%;border:1px solid #ffffff26;background:#0009;color:#fff;font-size:28px;display:grid;place-items:center;backdrop-filter:blur(8px)}.player-error{position:absolute;left:26px;bottom:24px;color:#ffd4d4;font-size:12px;max-width:60%}.below{display:flex;justify-content:space-between;gap:16px;padding:14px 4px;color:var(--muted);font-size:13px}.below b{color:var(--accent);margin-right:8px}.below a{color:#fff;text-decoration:none}
.admin-page{background:radial-gradient(circle at 20% 0%,#18202e,transparent 35%),#07090d}.login-shell{min-height:100vh;display:grid;place-items:center;padding:20px}.panel{background:linear-gradient(180deg,#101620,#0b0f16);border:1px solid var(--line);border-radius:18px;box-shadow:0 20px 70px #0007}.login-card{width:min(460px,100%);padding:34px}.eyebrow{color:var(--accent);font-size:11px;font-weight:900;letter-spacing:.18em}.login-card h1,.topbar h1{margin:8px 0 5px}.login-card form{display:grid;gap:10px;margin-top:24px}.login-card input,.add-panel textarea,.copyrow input{width:100%;background:#090c12;color:#fff;border:1px solid var(--line);border-radius:12px;padding:13px 14px;outline:none}.login-card input:focus,.add-panel textarea:focus{border-color:#30445f}.login-card button,.add-panel button{border:0;border-radius:11px;padding:12px 15px;background:var(--accent);color:#17120a;font-weight:900}.form-error{min-height:20px;margin-top:10px;color:#ff9d9d;font-size:13px}.topbar{max-width:1400px;margin:auto;padding:28px 22px 10px;display:flex;justify-content:space-between;gap:15px;align-items:end}.actions{display:flex;gap:8px}.ghost{background:#111722;color:#d8deea;border:1px solid var(--line);border-radius:10px;padding:10px 13px;text-decoration:none}.grid{max-width:1400px;margin:auto;padding:12px 22px 40px;display:grid;grid-template-columns:1.2fr .8fr;gap:16px}.now-panel,.add-panel,.queue-panel,.links-panel{padding:20px}.panel-head{display:flex;justify-content:space-between;gap:10px;align-items:center;color:#d9deea}.panel-head span:last-child{color:var(--muted);font-size:12px}.now-panel h2{font-size:30px;margin:22px 0 20px}.progress{height:7px;border-radius:99px;background:#161c26;overflow:hidden}.progress i{display:block;height:100%;width:0;background:linear-gradient(90deg,var(--accent),#fff);box-shadow:0 0 18px #e7b85a55}.add-panel textarea{min-height:180px;resize:vertical;margin-top:16px}.row{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}.row .danger{background:#3a1518;color:#ffb4b4;border:1px solid #592126}.hint{font-size:12px;line-height:1.5;margin-top:12px}.queue-panel{grid-column:1/-1}.queue{display:grid;gap:8px;margin-top:14px}.queue-item{display:grid;grid-template-columns:34px 1fr auto;gap:10px;align-items:center;padding:12px;border:1px solid var(--line);border-radius:12px;background:#0a0e14}.num{font-weight:900;color:var(--accent);text-align:center}.qtext b{display:block}.qtext small{display:block;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:3px}.qactions{display:flex;gap:5px}.qactions button,.copyrow button{border:1px solid var(--line);background:#111722;color:#dce3ef;border-radius:9px;padding:7px 10px}.qactions .del{color:#ffaaaa}.links-panel{grid-column:1/-1}.links-panel label{display:block;color:var(--muted);font-size:12px;margin:14px 0 6px}.copyrow{display:grid;grid-template-columns:1fr auto;gap:8px}.copyrow input{font-size:12px}.toast{position:fixed;right:18px;bottom:18px;background:#111722;border:1px solid var(--line);padding:12px 14px;border-radius:12px;display:none}
@media(max-width:850px){.grid{grid-template-columns:1fr}.topbar{align-items:start;flex-direction:column}.now-panel h2{font-size:22px}.now strong{font-size:18px}.brand{left:18px;top:18px;font-size:14px}.live{right:18px;top:18px}.watch-shell{padding:10px}.player-card{border-radius:14px}}
'''

HLS_PLAYER_JS = r'''
let playerHls=null;
async function loadHlsLibrary(){
  if(window.Hls) return true;
  return new Promise(resolve=>{
    const s=document.createElement('script');s.src='https://cdn.jsdelivr.net/npm/hls.js@1.6.13/dist/hls.min.js';
    s.onload=()=>resolve(!!window.Hls);s.onerror=()=>resolve(false);document.head.appendChild(s);
  });
}
async function startPlayer(){
  const v=document.getElementById('video'), err=document.getElementById('error');
  const src='/hls/channel.m3u8?ts='+Date.now();
  const hasHls=await loadHlsLibrary();
  if(hasHls && Hls.isSupported()){
    if(playerHls) playerHls.destroy();
    playerHls=new Hls({enableWorker:true,liveSyncDurationCount:3,liveMaxLatencyDurationCount:8,backBufferLength:30});
    playerHls.loadSource(src);playerHls.attachMedia(v);
    playerHls.on(Hls.Events.MANIFEST_PARSED,()=>v.play().catch(()=>{}));
    playerHls.on(Hls.Events.ERROR,(e,d)=>{if(d.fatal){err.textContent='Yayım yenidən qoşulur…';setTimeout(startPlayer,2500)}});
  }else if(v.canPlayType('application/vnd.apple.mpegurl')){v.src=src;v.play().catch(()=>{})}
  else{err.textContent='Bu brauzer HLS oynatmanı dəstəkləmir.'}
}
'''

INDEX_HTML = '''<!doctype html><html lang="az"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{channel}</title><style>{css}</style></head><body class="watch"><main class="watch-shell"><section class="player-card"><video id="video" playsinline controls autoplay></video><div class="shade"></div><div class="brand">{channel}</div><div class="live"><span></span> CANLI</div><div class="now"><small>İNDİ GEDİR</small><strong id="title">Növbə hazırlanır…</strong><time id="clock"></time></div><button id="playBtn" class="big-play">▶</button><div id="error" class="player-error"></div></section><section class="below"><div><b>HLS</b><span>/hls/channel.m3u8</span></div><a href="/admin">Admin panel</a></section></main><script>{hls_js}</script><script>
const title=document.getElementById('title'),clock=document.getElementById('clock'),errorBox=document.getElementById('error'),playBtn=document.getElementById('playBtn'),video=document.getElementById('video');
function tick(){clock.textContent=new Intl.DateTimeFormat('az-AZ',{timeZone:'Asia/Baku',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}).format(new Date())}tick();setInterval(tick,1000);
async function state(){try{const r=await fetch('/api/state?ts='+Date.now(),{cache:'no-store'}),d=await r.json();title.textContent=d.current?.title||'Növbədə video yoxdur';errorBox.textContent=d.lastError||''}catch(e){}}
playBtn.onclick=()=>video.play().catch(()=>{});video.onplay=()=>playBtn.style.display='none';video.onpause=()=>playBtn.style.display='grid';video.onended=()=>setTimeout(startPlayer,400);state();setInterval(state,3000);startPlayer();
</script></body></html>'''

ADMIN_HTML = '''<!doctype html><html lang="az"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Admin — {channel}</title><style>{css}</style></head><body class="admin-page">
<div id="login" class="login-shell"><div class="panel login-card"><div class="eyebrow">24/7 HLS CONTROL</div><h1>{channel}</h1><p class="muted">M3U8 növbəsinə giriş</p><form id="loginForm"><input id="user" placeholder="İstifadəçi adı" autocomplete="username" value="admin"><input id="pass" placeholder="Şifrə" type="password" autocomplete="current-password"><button>Giriş</button></form><div id="loginError" class="form-error"></div></div></div>
<div id="app" class="hidden"><header class="topbar"><div><div class="eyebrow">24/7 CHANNEL CONTROL</div><h1>{channel}</h1></div><div class="actions"><a class="ghost" href="/">Canlı kanal</a><button id="logout" class="ghost">Çıxış</button></div></header><main class="grid">
<section class="panel now-panel"><div class="panel-head"><div><span class="status-dot"></span><b>İndi yayımda</b></div><span id="liveState">Yoxlanılır…</span></div><h2 id="currentTitle">—</h2><div class="progress"><i id="progress"></i></div><div class="tiny" id="currentTime">00:00 / 00:00</div><div class="row"><button id="skip">Növbəti videoya keç</button></div></section>
<section class="panel add-panel"><div class="panel-head"><b>M3U8 əlavə et</b><span>Link və playlist</span></div><textarea id="input" placeholder="Film adı | https://example.com/movie.m3u8\n\nvə ya sadəcə M3U8 linklərini sətir-sətir yapışdır…"></textarea><div class="row"><button id="add">Növbəyə əlavə et</button><button id="clear" class="danger">Bütün növbəni sil</button></div><div class="hint">M3U8/VOD mənbələri. #EXT-X-ENDLIST olan mənbələr avtomatik bitmiş sayılır və növbəti videoya keçir. Təkrar linklər əlavə edilmir.</div><div id="msg" class="form-error"></div></section>
<section class="panel queue-panel"><div class="panel-head"><b>Növbə</b><span id="count">0 video</span></div><div id="queue" class="queue"></div></section>
<section class="panel links-panel"><div class="panel-head"><b>Kanal linkləri</b><span>Hazır</span></div><label>HLS</label><div class="copyrow"><input id="hls" readonly><button data-copy="hls">Kopyala</button></div><label>M3U</label><div class="copyrow"><input id="m3u" readonly><button data-copy="m3u">Kopyala</button></div></section>
</main></div><div id="toast" class="toast"></div>
<script>
const $=id=>document.getElementById(id);let data={};
function fmt(s){s=Math.max(0,Math.floor(s||0));return String(Math.floor(s/60)).padStart(2,'0')+':'+String(s%60).padStart(2,'0')}
function toast(t){$('toast').textContent=t;$('toast').style.display='block';setTimeout(()=>$('toast').style.display='none',1600)}
async function api(path,opt={}){const r=await fetch(path,{...opt,headers:{'Content-Type':'application/json',...(opt.headers||{})}});let d={};try{d=await r.json()}catch{};if(r.status===401)throw new Error('AUTH');if(!r.ok)throw new Error(d.error||'Xəta');return d}
async function refresh(){try{data=await api('/api/state?ts='+Date.now(),{cache:'no-store'});$('login').classList.add('hidden');$('app').classList.remove('hidden');const c=data.current;$('currentTitle').textContent=c?.title||'Növbədə heç nə yoxdur';$('liveState').textContent=c?(c.live?'VOD / CANLI':'Yoxlanılır'):'Gözləyir';const e=c?.elapsed||0,d=c?.duration||0;$('progress').style.width=d?Math.min(100,e/d*100)+'%':'0%';$('currentTime').textContent=fmt(e)+' / '+fmt(d);$('count').textContent=`${data.queue.length} video`;renderQueue();$('hls').value=location.origin+'/hls/channel.m3u8';$('m3u').value=location.origin+'/playlist.m3u'}catch(e){if(e.message==='AUTH'){$('login').classList.remove('hidden');$('app').classList.add('hidden')}}}
function renderQueue(){const q=$('queue');q.innerHTML='';data.queue.forEach((x,i)=>{const el=document.createElement('div');el.className='queue-item';el.draggable=true;el.innerHTML=`<span class="num">${i+1}</span><div class="qtext"><b></b><small></small></div><div class="qactions"><button data-a="up">↑</button><button data-a="down">↓</button><button data-a="del" class="del">×</button></div>`;el.querySelector('b').textContent=x.title;el.querySelector('small').textContent=x.url;q.appendChild(el);el.querySelector('[data-a=del]').onclick=async()=>{await api('/api/queue/'+x.id,{method:'DELETE'});refresh()};el.querySelector('[data-a=up]').onclick=()=>move(i,-1);el.querySelector('[data-a=down]').onclick=()=>move(i,1)})}
async function move(i,d){const j=i+d;if(j<0||j>=data.queue.length)return;const ids=data.queue.map(x=>x.id);[ids[i],ids[j]]=[ids[j],ids[i]];await api('/api/queue/reorder',{method:'POST',body:JSON.stringify({ids})});refresh()}
$('loginForm').onsubmit=async e=>{e.preventDefault();$('loginError').textContent='';try{await api('/api/login',{method:'POST',body:JSON.stringify({username:$('user').value,password:$('pass').value})});refresh()}catch(e){$('loginError').textContent=e.message}}
$('logout').onclick=async()=>{await api('/api/logout',{method:'POST'});location.reload()};$('skip').onclick=async()=>{await api('/api/queue/skip',{method:'POST'});toast('Növbəti video başladı');refresh()};$('add').onclick=async()=>{try{const d=await api('/api/queue',{method:'POST',body:JSON.stringify({text:$('input').value})});$('input').value='';toast(`${d.added} video əlavə edildi`);refresh()}catch(e){$('msg').textContent=e.message}};$('clear').onclick=async()=>{if(confirm('Bütün növbə silinsin?')){await api('/api/queue',{method:'DELETE'});refresh()}};document.querySelectorAll('[data-copy]').forEach(b=>b.onclick=async()=>{try{await navigator.clipboard.writeText($(b.dataset.copy).value);b.textContent='Kopyalandı';setTimeout(()=>b.textContent='Kopyala',1200)}catch{}});refresh();setInterval(refresh,3000);
</script></body></html>'''

# ------------------------------
# Routes
# ------------------------------
@app.after_request
def headers(resp):
    resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
    resp.headers.setdefault('Referrer-Policy', 'same-origin')
    resp.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
    return resp

@app.route('/')
def home():
    return INDEX_HTML.format(channel=CHANNEL_NAME, css=CSS, hls_js=HLS_PLAYER_JS)

@app.route('/admin')
def admin():
    return ADMIN_HTML.format(channel=CHANNEL_NAME, css=CSS)

@app.route('/health')
def health():
    c = current_snapshot()
    return jsonify(ok=True, channel=CHANNEL_NAME, db=True, current=c['title'] if c else None, error=STATE['last_error'] or None)

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json(silent=True) or {}
    if str(data.get('username','')) != ADMIN_USER or str(data.get('password','')) != ADMIN_PASSWORD:
        return jsonify(ok=False, error='İstifadəçi adı və ya şifrə yanlışdır'), 401
    resp = make_response(jsonify(ok=True))
    secure = request.is_secure or request.headers.get('X-Forwarded-Proto','').lower() == 'https'
    resp.set_cookie('platinum_session', make_session(ADMIN_USER), max_age=30*86400, httponly=True, samesite='Lax', secure=secure, path='/')
    return resp

@app.route('/api/logout', methods=['POST'])
def logout():
    a = auth_required()
    if a: return a
    resp = make_response(jsonify(ok=True))
    resp.set_cookie('platinum_session','',max_age=0,httponly=True,samesite='Lax',path='/')
    return resp

@app.route('/api/state')
def api_state():
    c = current_snapshot()
    current = None
    if c:
        current = {'id': c['id'], 'title': c['title'], 'elapsed': max(0, now()-c['started_at']), 'duration': c['total_duration'], 'live': c['end_list']}
    return jsonify(ok=True, channel=CHANNEL_NAME, current=current, queue=queue_rows(), lastError=STATE['last_error'] or None, hlsPath='/hls/channel.m3u8', m3uPath='/playlist.m3u')

@app.route('/api/queue', methods=['POST'])
def add_queue():
    a=auth_required()
    if a:return a
    data=request.get_json(silent=True) or {}
    items=parse_input(data.get('text',''))
    if not items:return jsonify(ok=False,error='Etibarlı HTTP/HTTPS M3U8 linki tapılmadı.'),400
    added=0
    with DB_LOCK:
        c=db(); existing={r['url'] for r in c.execute('SELECT url FROM playlist_queue').fetchall()}; mx=c.execute('SELECT COALESCE(MAX(position),-1) p FROM playlist_queue').fetchone()['p']
        for item in items:
            if item['url'] in existing: continue
            mx+=1
            c.execute('INSERT INTO playlist_queue(title,url,position,created_at) VALUES(?,?,?,?)',(item['title'],item['url'],mx,now())); existing.add(item['url']);added+=1
        c.commit();c.close()
    start_engine()
    return jsonify(ok=True,added=added)

@app.route('/api/queue/<int:item_id>', methods=['DELETE'])
def delete_queue(item_id):
    a=auth_required()
    if a:return a
    delete_queue_id(item_id)
    with STATE_LOCK:
        if STATE.get('current') and STATE['current']['id']==str(item_id): STATE['current']=None
    return jsonify(ok=True)

@app.route('/api/queue/reorder', methods=['POST'])
def reorder():
    a=auth_required()
    if a:return a
    ids=[str(x) for x in (request.get_json(silent=True) or {}).get('ids',[]) if str(x).isdigit()]
    with DB_LOCK:
        c=db();
        for i,item_id in enumerate(ids): c.execute('UPDATE playlist_queue SET position=? WHERE id=?',(i,item_id))
        c.commit();c.close()
    return jsonify(ok=True)

@app.route('/api/queue/skip', methods=['POST'])
def skip():
    a=auth_required()
    if a:return a
    skip_current(); return jsonify(ok=True)

@app.route('/api/queue', methods=['DELETE'])
def clear_queue():
    a=auth_required()
    if a:return a
    with DB_LOCK:
        c=db();c.execute('DELETE FROM playlist_queue');c.commit();c.close()
    with STATE_LOCK: STATE['current']=None
    return jsonify(ok=True)

@app.route('/hls/channel.m3u8')
def hls_playlist():
    body=build_live_playlist()
    return Response(body,mimetype='application/vnd.apple.mpegurl',headers={'Cache-Control':'no-store, no-cache, must-revalidate','Access-Control-Allow-Origin':'*'})

@app.route('/hls/segment/<item_id>/<int:index>')
def hls_segment(item_id,index):
    c=current_snapshot()
    if not c or c['id']!=str(item_id): return Response(status=404)
    if index<0 or index>=len(c['segments']): return Response(status=404)
    try:
        body,ctype=fetch_binary(c['segments'][index]['url'])
        return Response(body,mimetype=ctype.split(';')[0],headers={'Cache-Control':'public, max-age=30','Access-Control-Allow-Origin':'*'})
    except Exception as e: return Response(str(e),status=502)

@app.route('/hls/resource/<item_id>/<kind>/<int:index>')
def hls_resource(item_id,kind,index):
    c=current_snapshot()
    if not c or c['id']!=str(item_id) or index<0 or index>=len(c['segments']): return Response(status=404)
    s=c['segments'][index]; url=s.get('map_url') if kind=='map' else s.get('key_url') if kind=='key' else None
    if not url:return Response(status=404)
    try:
        body,ctype=fetch_binary(url)
        return Response(body,mimetype='application/octet-stream' if kind=='key' else ctype.split(';')[0],headers={'Cache-Control':'public, max-age=60','Access-Control-Allow-Origin':'*'})
    except Exception as e:return Response(str(e),status=502)

@app.route('/playlist.m3u')
def playlist_m3u():
    base=base_url(); text=f'#EXTM3U\n#EXTINF:-1 tvg-name="{CHANNEL_NAME}",{CHANNEL_NAME}\n{base}/hls/channel.m3u8\n'
    return Response(text,mimetype='audio/x-mpegurl',headers={'Content-Disposition':'inline; filename="playlist.m3u"','Cache-Control':'no-store'})

@app.route('/favicon.ico')
def favicon():
    return Response(status=204)

# Start engine on first import/run.
start_engine()

if __name__ == '__main__':
    print(f'PLATNIUM AZƏRBAYCAN running on http://{HOST}:{PORT}')
    print(f'Admin: /admin | user={ADMIN_USER} | password={ADMIN_PASSWORD}')
    app.run(host=HOST, port=PORT, threaded=True)

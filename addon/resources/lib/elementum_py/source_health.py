"""Local source history; persist hashes and aggregate observations, never URLs."""
import hashlib
import json
import math
import os
import re
import threading
import time
from urllib.parse import parse_qs, urlsplit

def resolution(name):
    match = re.search(r'(?<!\d)(480|576|720|1080|2160|4320)p\b', str(name or ''), re.I)
    return int(match[1]) if match else 0

def identity(uri):
    if uri.startswith('magnet:?'):
        try:
            for value in parse_qs(urlsplit(uri).query, max_num_fields=128).get('xt', []):
                if re.fullmatch(r'urn:btih:[a-fA-F0-9]{40}', value):
                    uri = value.lower()
                    break
        except ValueError:
            pass
    return hashlib.sha256(uri.encode('utf-8')).hexdigest()

class SourceHealth:
    def __init__(self, profile, clock=time.time):
        self.path = os.path.join(profile, 'python-source-health.json')
        self.clock, self.lock, self.entries = clock, threading.Lock(), {}
        try:
            with open(self.path, encoding='utf-8') as handle:
                raw = json.loads(handle.read(131073))
            now = clock()
            for key, row in list(raw.items())[:256]:
                if not re.fullmatch('[a-f0-9]{64}', key) or not isinstance(row, dict):
                    continue
                values = {field:float(row.get(field, 0)) for field in ('when','success','failure','startup','rate')}
                if all(math.isfinite(v) and v >= 0 for v in values.values()) and 0 <= now-values['when'] < 7*86400:
                    self.entries[key] = values
        except (OSError, ValueError, TypeError, AttributeError):
            pass

    def record(self, uri, success=None, elapsed=0, rate=0):
        if not uri:
            return
        key, now = identity(uri), self.clock()
        with self.lock:
            row = self.entries.setdefault(key, {'success':0,'failure':0,'startup':0,'rate':0})
            row['when'] = now
            if success is not None:
                field = 'success' if success else 'failure'
                row[field] = min(100, row[field]+1)
                if success:
                    row['failure'] *= .5
            if elapsed > 0 and success:
                row['startup'] = elapsed if not row['startup'] else row['startup']*.7+elapsed*.3
            if rate > 0 and math.isfinite(rate):
                row['rate'] = rate if not row['rate'] else row['rate']*.7+rate*.3
            self.entries = dict(sorted(((k,v) for k,v in self.entries.items() if now-v['when'] < 7*86400),
                                       key=lambda pair:pair[1]['when'], reverse=True)[:256])
            temporary = self.path+'.tmp'
            try:
                with open(temporary, 'w', encoding='utf-8') as handle:
                    json.dump(self.entries, handle, separators=(',',':'))
                os.replace(temporary, self.path)
            except OSError:
                pass  # History must never prevent playback.

    def score(self, uri):
        with self.lock:
            row = dict(self.entries.get(identity(uri), {}))
        if not row or self.clock()-row['when'] >= 7*86400:
            return 0
        return min(3,row['success'])*4-min(4,row['failure'])*12 + min(4,row['rate']/1048576) - min(4,row['startup']/30)

    def rank(self, sources):
        # Reorder within the existing quality slots; retain Burst's quality
        # order instead of silently promoting 4K over a selected 1080p source.
        groups = {}
        for source in sources:
            groups.setdefault(resolution(source.get('name')), []).append(source)
        for group in groups.values():
            group.sort(key=lambda item:self.score(item.get('uri','')), reverse=True)
        return [groups[resolution(item.get('name'))].pop(0) for item in sources]

"""Recognize native subtitle requests without changing movie search routes."""
from urllib.parse import parse_qs, urlsplit

def is_subtitle_request(argv):
    if len(argv) < 3 or urlsplit(argv[0]).path.rstrip('/') not in ('', '/subtitles'):
        return False
    try:
        action = parse_qs(argv[2].lstrip('?'), max_num_fields=20).get('action', [''])[0]
    except ValueError:
        return False
    return action in ('search', 'manualsearch', 'download')

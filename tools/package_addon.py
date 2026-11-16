"""Build an installable add-on ZIP from this checkout; standard library only.

    python tools/package_addon.py --repo . --version 0.3.4~dev1 --output dist

The archive has the single root folder Kodi requires (plugin.video.elementum),
keeps every license file, skips bytecode, and carries the requested version in
both addon.xml and elementum_py/version.py. The checkout is never modified and
nothing is published. The same input always produces the same bytes.

The checksum sidecar is written as exactly 64 ASCII hex bytes without a line
ending: a CRLF sidecar made Kodi report "Installation failed" in the past.
"""

import argparse
import ast
import hashlib
import io
import json
import os
import re
import sys
import warnings
import zipfile

ADDON_ID = 'plugin.video.elementum'
TEXT = ('.py', '.xml', '.txt', '.po', '.json', '.md', '.html', '.css', '.js', '.svg')
SKIPPED_FOLDERS = ('__pycache__', '.git', '.idea', '.vscode')
SKIPPED_SUFFIXES = ('.pyc', '.pyo', '.orig', '.rej', '.tmp')
SKIPPED_NAMES = ('.DS_Store', 'Thumbs.db')
VERSION_PATTERN = re.compile(r'\d+\.\d+\.\d+(?:[~+.-][A-Za-z0-9]+(?:[.~+-][A-Za-z0-9]+)*)?')
STAMP = (2026, 1, 1, 0, 0, 0)   # Fixed, so a rebuild of the same source is byte-identical.


class PackagingError(Exception):
    pass


def addon_files(folder):
    for root, folders, names in os.walk(folder):
        folders[:] = sorted(name for name in folders if name not in SKIPPED_FOLDERS)
        for name in sorted(names):
            if name in SKIPPED_NAMES or name.endswith(SKIPPED_SUFFIXES):
                continue
            path = os.path.join(root, name)
            if os.path.islink(path):
                raise PackagingError('Refusing to package a symbolic link: ' + path)
            yield path, os.path.relpath(path, folder).replace(os.sep, '/')


def with_version(relative, data, version):
    """The two places that state the add-on version, rewritten inside the archive only."""
    if relative == 'addon.xml':
        text = data.decode('utf-8')
        pattern = re.compile(r'(<addon\b[^>]*?\bversion=")[^"]*(")')
        if len(pattern.findall(text[:text.index('>') + 1 if '<addon' in text else 0])) != 1 and not pattern.search(text):
            raise PackagingError('addon.xml has no version attribute on its <addon> element')
        return pattern.sub(lambda match: match.group(1) + version + match.group(2), text, count=1).encode('utf-8')
    if relative == 'resources/lib/elementum_py/version.py':
        text = data.decode('utf-8')
        pattern = re.compile(r"^VERSION = '[^']*'", re.M)
        if len(pattern.findall(text)) != 1:
            raise PackagingError('version.py must contain exactly one VERSION line')
        return pattern.sub("VERSION = '" + version + "'", text).encode('utf-8')
    return data


def check_python(relative, data, engine_only):
    try:
        source = data.decode('utf-8')
        with warnings.catch_warnings():
            # Old escape sequences in the upstream frontend are warnings, not errors.
            warnings.simplefilter('ignore', SyntaxWarning)
            warnings.simplefilter('ignore', DeprecationWarning)
            compile(source, relative, 'exec')
            if engine_only:
                # The engine is promised to stay loadable by Kodi builds with Python 3.8.
                ast.parse(source, relative, feature_version=(3, 8))
    except (SyntaxError, UnicodeDecodeError, ValueError) as error:
        raise PackagingError('Python syntax check failed for {}: {}'.format(relative, error))


def build(repo, version, output, force=False):
    if not VERSION_PATTERN.fullmatch(version):
        raise PackagingError('Use a version like 0.3.4 or 0.3.4~dev1')
    folder = os.path.join(repo, 'addon')
    if not os.path.isfile(os.path.join(folder, 'addon.xml')):
        raise PackagingError('No addon/addon.xml under ' + os.path.abspath(repo))
    os.makedirs(output, exist_ok=True)
    name = '{}-{}.zip'.format(ADDON_ID, version)
    target = os.path.join(output, name)
    if os.path.exists(target) and not force:
        # A version that may already be installed somewhere must never change.
        raise PackagingError(name + ' already exists; choose a new version or pass --force for a local rebuild')
    seen = {'addon.xml': False, 'resources/lib/elementum_py/version.py': False, 'LICENSE': False}
    buffer = io.BytesIO()
    count = 0
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path, relative in addon_files(folder):
            with open(path, 'rb') as handle:
                data = handle.read()
            if relative.endswith(TEXT):
                data = data.replace(b'\r\n', b'\n')   # Same bytes from Windows and from Unix checkouts.
            data = with_version(relative, data, version)
            if relative.endswith('.py'):
                check_python(relative, data, relative.startswith('resources/lib/elementum_py/'))
            if relative in seen:
                seen[relative] = True
            entry = zipfile.ZipInfo(ADDON_ID + '/' + relative, STAMP)
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o644 << 16
            archive.writestr(entry, data)
            count += 1
    missing = [relative for relative, found in seen.items() if not found]
    if missing:
        raise PackagingError('Required files are missing from the add-on: ' + ', '.join(missing))
    payload = buffer.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    with open(target, 'wb') as handle:
        handle.write(payload)
    with open(target + '.sha256', 'wb') as handle:
        handle.write(digest.encode('ascii'))
    return verify(target, version, count)


def verify(target, version, count=None):
    """Re-open what was written, exactly as an installer would see it."""
    with open(target, 'rb') as handle:
        payload = handle.read()
    with open(target + '.sha256', 'rb') as handle:
        sidecar = handle.read()
    if len(sidecar) != 64 or not re.fullmatch(rb'[0-9a-f]{64}', sidecar):
        raise PackagingError('The checksum sidecar must be exactly 64 lowercase hex bytes')
    if sidecar.decode('ascii') != hashlib.sha256(payload).hexdigest():
        raise PackagingError('The checksum sidecar does not match the archive')
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        if archive.testzip() is not None:
            raise PackagingError('The archive failed its CRC check')
        names = archive.namelist()
        if any(not name.startswith(ADDON_ID + '/') for name in names):
            raise PackagingError('Every entry must be under ' + ADDON_ID + '/')
        if any('__pycache__' in name or name.endswith('.pyc') for name in names):
            raise PackagingError('Bytecode must not be packaged')
        manifest = archive.read(ADDON_ID + '/addon.xml').decode('utf-8')
        module = archive.read(ADDON_ID + '/resources/lib/elementum_py/version.py').decode('utf-8')
        if not re.search(r'<addon\b[^>]*\bid="{}"[^>]*\bversion="{}"'.format(re.escape(ADDON_ID), re.escape(version)), manifest):
            raise PackagingError('addon.xml inside the archive does not state version ' + version)
        if "VERSION = '{}'".format(version) not in module:
            raise PackagingError('version.py inside the archive does not state version ' + version)
        licenses = [name for name in names if 'LICENSE' in name.upper() or 'THIRD-PARTY' in name.upper()]
    if count is not None and count != len(names):
        raise PackagingError('The archive does not contain every packaged file')
    return {'file': os.path.abspath(target), 'version': version, 'bytes': len(payload), 'entries': len(names),
            'sha256': sidecar.decode('ascii'), 'license_files': len(licenses)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--repo', default='.', help='checkout that contains the addon/ folder')
    parser.add_argument('--version', required=True, help='for example 0.3.4~dev1')
    parser.add_argument('--output', default='dist')
    parser.add_argument('--force', action='store_true', help='replace a local development build of the same version')
    arguments = parser.parse_args()
    try:
        print(json.dumps(build(arguments.repo, arguments.version, arguments.output, arguments.force), indent=1))
    except PackagingError as error:
        print('Packaging failed: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

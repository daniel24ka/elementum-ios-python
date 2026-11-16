"""Exercise Kodi's real checksum delimiter rules against the release files."""

import hashlib
from pathlib import Path
import re
import unittest


class RepositoryChecksumTests(unittest.TestCase):
    def test_every_checksum_matches_kodi_parser_and_archive(self):
        root = Path(__file__).resolve().parents[1]
        repository = root / 'repo'
        if not repository.is_dir():
            repository = root / 'publish/repo'
        self.assertTrue(repository.is_dir(), 'Prepare the installation repository before this test')
        sidecars = list(repository.rglob('*.zip.sha256'))
        self.assertGreaterEqual(len(sidecars), 9)
        for sidecar in sidecars:
            with self.subTest(package=sidecar.name):
                raw = sidecar.read_bytes()
                # xbmc/addons/Repository.cpp::FetchChecksum uses find_first_of(" \n").
                kodi_digest = re.split(b'[ \n]', raw, maxsplit=1)[0]
                expected = hashlib.sha256(sidecar.with_suffix('').read_bytes()).hexdigest().encode('ascii')
                self.assertEqual(len(kodi_digest), 64)
                self.assertEqual(kodi_digest, expected)
        metadata = (repository / 'addons.xml').read_bytes()
        self.assertEqual((repository / 'addons.xml.md5').read_bytes(),
                         hashlib.md5(metadata).hexdigest().encode('ascii'))


if __name__ == '__main__':
    unittest.main()

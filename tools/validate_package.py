"""Run the test-suite against the code inside a built add-on ZIP.

    python tools/validate_package.py dist/plugin.video.elementum-0.3.4~dev1.zip --out VALIDATION.json

The archive is extracted to a temporary folder next to copies of tests/ and
tools/, so the tests import the packaged engine and never this checkout. The
report lists every test, as VALIDATION.json does for published versions.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

RUNNER = r'''
import json, sys, time, unittest

class Timed(unittest.TextTestResult):
    def startTest(self, test):
        self._started = time.perf_counter()
        super().startTest(test)
    def record(self, test, status):
        self.cases.append({'test': test.id(), 'status': status,
                           'seconds': round(time.perf_counter() - self._started, 4)})
    def addSuccess(self, test):
        super().addSuccess(test); self.record(test, 'pass')
    def addFailure(self, test, err):
        super().addFailure(test, err); self.record(test, 'fail')
    def addError(self, test, err):
        super().addError(test, err); self.record(test, 'error')
    def addSkip(self, test, reason):
        super().addSkip(test, reason); self.record(test, 'skip')

Timed.cases = []
suite = unittest.defaultTestLoader.discover('tests')
started = time.perf_counter()
result = unittest.TextTestRunner(resultclass=Timed, verbosity=1).run(suite)
json.dump({'passed': sum(case['status'] == 'pass' for case in Timed.cases), 'skipped': len(result.skipped),
           'failures': len(result.failures), 'errors': len(result.errors),
           'elapsed_seconds': round(time.perf_counter() - started, 2),
           'cases': sorted(Timed.cases, key=lambda case: case['test'])}, open(sys.argv[1], 'w'), indent=1)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('archive')
    parser.add_argument('--repo', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    parser.add_argument('--out')
    arguments = parser.parse_args()
    with open(arguments.archive, 'rb') as handle:
        digest = hashlib.sha256(handle.read()).hexdigest()
    folder = tempfile.mkdtemp(prefix='elementum-validate-')
    try:
        with zipfile.ZipFile(arguments.archive) as archive:
            for name in archive.namelist():
                relative = name.split('/', 1)[1] if '/' in name else ''
                if not relative or name.endswith('/') or '..' in relative.split('/'):
                    continue
                target = os.path.join(folder, 'addon', *relative.split('/'))
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, 'wb') as handle:
                    handle.write(archive.read(name))
        for name in ('tests', 'tools'):
            shutil.copytree(os.path.join(arguments.repo, name), os.path.join(folder, name),
                            ignore=shutil.ignore_patterns('__pycache__'))
        # The repository-checksum test reads the published index of this checkout.
        if os.path.isdir(os.path.join(arguments.repo, 'repo')):
            shutil.copytree(os.path.join(arguments.repo, 'repo'), os.path.join(folder, 'repo'))
        with open(os.path.join(folder, 'addon', 'resources', 'lib', 'elementum_py', 'version.py'), encoding='utf-8') as handle:
            version = re.search(r"VERSION = '([^']+)'", handle.read()).group(1)
        runner = os.path.join(folder, 'run_packaged_tests.py')
        with open(runner, 'w', encoding='utf-8') as handle:
            handle.write(RUNNER)
        report_path = os.path.join(folder, 'report.json')
        environment = dict(os.environ, PYTHONUTF8='1', PYTHONDONTWRITEBYTECODE='1')
        started = time.time()
        completed = subprocess.run([sys.executable, runner, report_path], cwd=folder, env=environment)
        with open(report_path, encoding='utf-8') as handle:
            report = json.load(handle)
        clean = not report['failures'] and not report['errors'] and completed.returncode == 0
        result = {'version': version, 'package_sha256': digest, 'actual_packaged_backend_tested': True,
                  'python': sys.version.split()[0], 'status': 'pass' if clean else 'fail',
                  'passed': report['passed'], 'skipped': report['skipped'], 'failures': report['failures'],
                  'errors': report['errors'], 'elapsed_seconds': report['elapsed_seconds'],
                  'validated_at': time.strftime('%Y-%m-%d', time.gmtime(started)), 'cases': report['cases']}
        if arguments.out:
            with open(arguments.out, 'w', encoding='utf-8', newline='\n') as handle:
                json.dump(result, handle, indent=2)
                handle.write('\n')
        print(json.dumps({key: value for key, value in result.items() if key != 'cases'}, indent=1))
        return 0 if clean else 1
    finally:
        shutil.rmtree(folder, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())

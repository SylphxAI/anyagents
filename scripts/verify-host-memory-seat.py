#!/usr/bin/env python3
"""Real managed-UID proof using only a newly created synthetic boundary.

Run in an already-authorized privileged executor; this script never escalates,
changes accounts, or opens a real agent tree. No mocked launcher is accepted.
The fixture is deleted after readback; the JSON receipt is created exclusively.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            h.update(block)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--source-sha', required=True)
    p.add_argument('--vendor-test-binary', type=Path, required=True)
    p.add_argument('--launcher', type=Path, default=Path('/usr/bin/setpriv'))
    p.add_argument('--uid', type=int, required=True)
    p.add_argument('--gid', type=int, required=True)
    p.add_argument('--socket-gid', type=int, required=True)
    p.add_argument('--base', type=Path, required=True, help='existing disposable scratch directory, not /tmp')
    p.add_argument('--receipt', type=Path, required=True)
    args = p.parse_args()
    if os.geteuid() != 0:
        p.error('requires an already-authorized privileged executor; no sudo/escalation is attempted')
    if args.uid in (0, os.getuid()) or args.gid in (0, os.getgid()):
        p.error('UID and primary GID must independently differ from server/root identity')
    if min(args.uid, args.gid, args.socket_gid) <= 0:
        p.error('identities must be positive')
    if args.receipt.exists() or not args.base.is_dir() or str(args.base.resolve()).startswith('/tmp'):
        p.error('receipt must not exist; base must be an existing non-/tmp scratch directory')
    binary = args.binary.resolve(strict=True)
    launcher = args.launcher.resolve(strict=True)
    if launcher.read_bytes()[:4] != b'\x7fELF':
        p.error('launcher must be a real native setpriv executable, not a test double')
    receipt = {
        'source_sha': args.source_sha, 'binary_sha256': digest(binary),
        'script_sha256': digest(Path(__file__).resolve()), 'launcher_sha256': digest(launcher),
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'server_uid': os.getuid(), 'server_gid': os.getgid(),
        'expected_uid': args.uid, 'expected_primary_gid': args.gid,
        'entrypoint': 'CLI and actual vendor_launch -> shared harness admission -> native setpriv',
        'cases': [],
    }
    root = Path(tempfile.mkdtemp(prefix='host-memory-seat-', dir=args.base.resolve()))
    # Only objects created below this new directory are ever chmod/chowned.
    def directory(rel, mode=0o711, seat=False):
        path = root / rel
        path.mkdir(parents=True, exist_ok=True)
        if seat:
            os.chown(path, args.uid, args.gid)
        path.chmod(mode)
        return path
    def put(rel, text, mode=0o600, seat=False):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        if seat:
            os.chown(path, args.uid, args.gid)
        path.chmod(mode)
        return path
    try:
        root.chmod(0o711)
        data = directory('data')
        directory('data/agents')
        mine = directory('data/agents/synthetic-own')
        config = directory('data/agents/synthetic-own/config', 0o700, True)
        memory = directory('data/agents/synthetic-own/memory', 0o700, True)
        overlay = directory('data/agents/synthetic-own/skills', 0o700, True)
        put('data/agents/synthetic-own/config/mcp-capability', 'synthetic-capability-only\n', seat=True)
        put('data/agents/synthetic-own/memory/MEMORY.md', 'synthetic-memory-before\n', seat=True)
        directory('data/agents/synthetic-own/skills/local', 0o700, True)
        put('data/agents/synthetic-own/skills/local/SKILL.md', '# synthetic local skill\n', seat=True)
        directory('data/skills', 0o555)
        directory('data/skills/global', 0o555)
        put('data/skills/global/SKILL.md', '# synthetic global skill\n', 0o444)
        for harness in ['.claude', '.agents', '.agy']:
            directory(f'data/agents/synthetic-own/config/{harness}', 0o700, True)
            directory(f'data/agents/synthetic-own/config/{harness}/skills', 0o700, True)
            view = config / harness / 'skills'
            (view / 'local').symlink_to(overlay / 'local')
            (view / 'global').symlink_to(data / 'skills/global')
        for name in ['state.db', 'secrets', 'anyagents.sock', 'operator-token']:
            put(f'data/{name}', 'synthetic-private-only\n')
        directory('data/agents/synthetic-peer')
        for name in ['config', 'memory', 'skills', 'sessions', 'transcripts']:
            directory(f'data/agents/synthetic-peer/{name}', 0o700)
            put(f'data/agents/synthetic-peer/{name}/private', 'synthetic-peer-only\n')
        directory('data/agents/synthetic-own/sessions', 0o2770, True)
        put('data/agents/synthetic-own/sessions/private', 'synthetic-host-only\n', 0o660, True)
        directory('data/agents/synthetic-own/transcripts', 0o700)
        put('data/agents/synthetic-own/transcripts/private', 'synthetic-host-only\n')
        put('data/identity-boundary.json', json.dumps({
            'version': 1, 'data_dir': str(data), 'agent_route': str(data),
            'agents': {'mechanism': 'managed-uid', 'denied': [], 'writable': [], 'readable': []},
            'server': {'user': 'synthetic-executor', 'socket_group': str(args.socket_gid),
                       'socket': str(data / 'anyagents.sock'), 'socket_mode': '0660',
                       'agent_presentation': str(data)},
        }))
        protected = [x for x in root.rglob('*') if x.is_file() and not x.is_symlink()
                     and x != memory / 'MEMORY.md']
        before = {str(x.relative_to(root)): digest(x) for x in protected}
        program = put('probe.sh', '''#!/bin/sh
set -eu
root=$1; expected_uid=$2; expected_gid=$3
[ "$(id -u)" = "$expected_uid" ]
[ "$(id -g)" = "$expected_gid" ]
printf 'ACTUAL_UID=%s PRIMARY_GID=%s\\n' "$(id -u)" "$(id -g)"
[ -x "$root/agents" ]
[ ! -r "$root/agents" ]
echo PEER_PARENT_TRAVERSE_ONLY
printf 'synthetic-memory-after\\n' > "$root/agents/synthetic-own/memory/MEMORY.md"
grep -qx synthetic-memory-after "$root/agents/synthetic-own/memory/MEMORY.md"
printf 'synthetic-created\n' > "$root/agents/synthetic-own/memory/created"
grep -qx synthetic-created "$root/agents/synthetic-own/memory/created"
rm "$root/agents/synthetic-own/memory/created"
echo MEMORY_RW_OK
for h in .claude .agents .agy; do
  grep -qx '# synthetic local skill' "$root/agents/synthetic-own/config/$h/skills/local/SKILL.md"
  grep -qx '# synthetic global skill' "$root/agents/synthetic-own/config/$h/skills/global/SKILL.md"
done
printf '# synthetic authored skill\n' > "$root/agents/synthetic-own/skills/authored.md"
grep -qx '# synthetic authored skill' "$root/agents/synthetic-own/skills/authored.md"
rm "$root/agents/synthetic-own/skills/authored.md"
echo SKILL_VIEWS_READ_AND_OWN_OVERLAY_WRITE_OK
grep -qx synthetic-host-only "$root/agents/synthetic-own/sessions/private"
printf 'synthetic-host-only\n' > "$root/agents/synthetic-own/sessions/private"
printf 'synthetic-created\n' > "$root/agents/synthetic-own/sessions/created"
grep -qx synthetic-created "$root/agents/synthetic-own/sessions/created"
rm "$root/agents/synthetic-own/sessions/created"
echo SESSIONS_RW_OK
for file in "$root/state.db" "$root/secrets" "$root/operator-token" "$root/identity-boundary.json" \
  "$root/agents/synthetic-peer/config/private" "$root/agents/synthetic-peer/memory/private" "$root/agents/synthetic-peer/skills/private" \
  "$root/agents/synthetic-peer/sessions/private" "$root/agents/synthetic-peer/transcripts/private" \
  "$root/agents/synthetic-own/transcripts/private"; do
  if cat "$file" >/dev/null 2>&1; then exit 21; fi
  if (printf forbidden > "$file") 2>/dev/null; then exit 22; fi
done
for file in "$root/skills/global/SKILL.md" \
  "$root/agents/synthetic-own/config/.claude/skills/global/SKILL.md"; do
  if (printf forbidden > "$file") 2>/dev/null; then exit 23; fi
done
echo PRIVATE_AND_GLOBAL_DENIED
''', 0o555)
        env = {k: v for k, v in os.environ.items() if not k.startswith('ANYAGENTS_')}
        env.update(ANYAGENTS_AGENT_UID=str(args.uid),
                   ANYAGENTS_AGENT_GID=str(args.gid), ANYAGENTS_SOCKET_GID=str(args.socket_gid),
                   ANYAGENTS_LAUNCHER=str(launcher))
        command = [str(binary), '--data', str(data), 'agent-run', '--config', str(config), '--',
                   '/bin/sh', str(program), str(data), str(args.uid), str(args.gid)]
        # A private executor ancestor is not repaired by chmod on our own leaf.
        # Diagnose traversal with the same real identity before invoking admission;
        # never alter an existing ancestor or substitute a test launcher.
        preflight = subprocess.run([
            str(launcher), '--reuid', str(args.uid), '--regid', str(args.gid),
            '--groups', f'{args.gid},{args.socket_gid}', '--no-new-privs', '--',
            '/bin/sh', '-c', '''set -eu
expected_uid=$1; expected_gid=$2; shift 2
[ "$(id -u)" = "$expected_uid" ]
[ "$(id -g)" = "$expected_gid" ]
for path do
  if [ ! -x "$path" ]; then
    printf 'fixture ancestor is not traversable: %s\\n' "$path" >&2
    exit 31
  fi
done
echo REAL_UID_FIXTURE_TRAVERSAL_OK
''', 'fixture-traversal', str(args.uid), str(args.gid),
            *map(str, reversed(root.parents)), str(root), str(data), str(mine), str(config),
        ], env=env, text=True, capture_output=True, timeout=30)
        receipt['cases'].append({'name': 'real-uid-fixture-traversal',
                                 'exit': preflight.returncode, 'stdout': preflight.stdout,
                                 'stderr': preflight.stderr})
        if preflight.returncode != 0 or 'REAL_UID_FIXTURE_TRAVERSAL_OK' not in preflight.stdout:
            raise RuntimeError(f'real UID fixture traversal failed: {preflight.stderr}')
        def run_case(name, success, expected_check=None):
            result = subprocess.run(command, env=env, text=True, capture_output=True, timeout=30)
            receipt['cases'].append({'name': name, 'exit': result.returncode,
                                     'stdout': result.stdout, 'stderr': result.stderr})
            if (result.returncode == 0) != success:
                raise RuntimeError(f'{name} unexpected exit {result.returncode}: {result.stderr}')
            if success:
                for marker in [f'ACTUAL_UID={args.uid} PRIMARY_GID={args.gid}', 'MEMORY_RW_OK',
                               'PEER_PARENT_TRAVERSE_ONLY', 'SKILL_VIEWS_READ_AND_OWN_OVERLAY_WRITE_OK',
                               'SESSIONS_RW_OK', 'PRIVATE_AND_GLOBAL_DENIED']:
                    if marker not in result.stdout:
                        raise RuntimeError(f'missing {marker}')
            elif 'MEMORY_RW_OK' in result.stdout or 'agent not started' not in result.stderr:
                raise RuntimeError(f'{name} did not refuse before command')
            elif expected_check and f'boundary check failed: {expected_check}' not in result.stderr:
                raise RuntimeError(f'{name} refused for a different boundary: {result.stderr}')
        # The actual vendor path runs in a required, isolated test subprocess.
        # Remove DAC override/read-search so root cannot fake traversal of the
        # legitimate seat-owned 0700 vendor parent. Retain UID/GID transitions.
        vendor_binary = args.vendor_test_binary.resolve(strict=True)
        public_test = root / 'vendor-test-binary'
        shutil.copyfile(vendor_binary, public_test)
        public_test.chmod(0o555)
        receipt['vendor_test_binary_sha256'] = digest(vendor_binary)
        if digest(public_test) != receipt['vendor_test_binary_sha256']:
            raise RuntimeError('vendor test artifact copy mismatch')
        vendor_env = {k: v for k, v in env.items() if not k.startswith('ANYAGENTS_')}
        vendor_env.update(ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST='1', ANYAGENTS_NATIVE_SEAT_BASE=str(root))
        vendor = subprocess.run([
            str(launcher), '--reuid', '0', '--regid', '0', '--groups', f'{args.gid},{args.socket_gid}',
            '--bounding-set=-dac_override,-dac_read_search', '--no-new-privs', '--',
            str(public_test), '--exact', 'vendor::tests::native_vendor_launch_enforces_shared_boundary_and_real_skill_mounts',
            '--ignored', '--nocapture', '--test-threads=1',
        ], env=vendor_env, text=True, capture_output=True, timeout=90)
        receipt['cases'].append({'name': 'actual-vendor-launch-real-uid-and-mounts',
                                 'exit': vendor.returncode, 'stdout': vendor.stdout, 'stderr': vendor.stderr})
        if vendor.returncode != 0:
            raise RuntimeError(f'actual vendor launch proof failed: {vendor.stdout} {vendor.stderr}')
        for marker in ['REAL_MOUNTS_UMASK077_OWNER_FALLBACK_OK SAME_SEAT_OK',
                       'NATIVE_VENDOR_WRITABLE_PEER_memory_REFUSED_BEFORE_COMMAND',
                       'NATIVE_VENDOR_WRITABLE_PEER_skills_REFUSED_BEFORE_COMMAND',
                       'NATIVE_VENDOR_WRITABLE_PEER_config_REFUSED_BEFORE_COMMAND',
                       'NATIVE_VENDOR_SEARCH_ONLY_PEER_memory_KNOWN_READ_REFUSED_BEFORE_COMMAND',
                       'NATIVE_VENDOR_SEARCH_ONLY_PEER_skills_KNOWN_READ_REFUSED_BEFORE_COMMAND',
                       'NATIVE_VENDOR_SEARCH_ONLY_PEER_config_KNOWN_READ_REFUSED_BEFORE_COMMAND',
                       'NATIVE_MUSE_FRESH_SESSION_PEER_memory_REFUSED_ZERO_PROCESSES',
                       'NATIVE_MUSE_FRESH_SESSION_PEER_skills_REFUSED_ZERO_PROCESSES',
                       'NATIVE_MUSE_FRESH_SESSION_PEER_config_REFUSED_ZERO_PROCESSES',
                       'NATIVE_MUSE_INITIALIZER_ADMITTED_AND_MARKER_REUSED',
                       'NATIVE_CODEX_PRESERVED_FOREIGN_PARENT_LAUNCH_OK',
                       'NATIVE_CODEX_PRESERVED_FOREIGN_MOUNT_LAUNCH_OK',
                       'NATIVE_VENDOR_LEGACY_SQLITE_OWNER_AND_BYTES_RETAINED',
                       'NATIVE_VENDOR_RUNTIME_CACHE_TMP_HANDOFF_AND_CAPABILITY_NORMALIZATION_OK',
                       'NATIVE_VENDOR_SEAT_OWNED_CAPABILITY_NORMALIZED_OK',
                       'NATIVE_AGY_GROUP_DEFAULTS_AND_PRIVATE_REPLACEMENT_REPAIRED',
                       'NATIVE_VENDOR_PROTECTED_BEFORE_EQUALS_AFTER', 'NATIVE_VENDOR_FIXTURE_REMOVED',
                       '1 passed; 0 failed']:
            if marker not in vendor.stdout:
                raise RuntimeError(f'actual vendor launch missing {marker}')
        public_test.unlink()
        run_case('real-uid-positive-and-denial-controls', True)
        capability = config / 'mcp-capability'
        for mode in (0o640, 0o660, 0o600):
            capability.chmod(mode)
            run_case(f'exact-seat-capability-mode-{mode:o}', True)
        for mode in (0o644, 0o664, 0o666):
            capability.chmod(mode)
            run_case(f'outside-readable-capability-mode-{mode:o}', False,
                     'its capability is readable by its own seat only')
        capability.chmod(0o640)
        os.chown(capability, args.uid, args.socket_gid)
        run_case('foreign-group-capability-refuses', False,
                 'its capability is readable by its own seat only')
        os.chown(capability, args.uid, args.gid)
        capability.chmod(0o600)
        if (memory / 'MEMORY.md').read_text() != 'synthetic-memory-after\n':
            raise RuntimeError('host memory readback mismatch')
        memory.rename(mine / 'memory-held')
        run_case('missing-memory-refuses-before-command', False)
        (mine / 'memory-held').rename(memory)
        overlay.rename(mine / 'skills-held')
        run_case('missing-own-overlay-refuses-before-command', False)
        (mine / 'skills-held').rename(overlay)
        for name in ['memory', 'skills', 'config']:
            peer_tree = data / f'agents/synthetic-peer/{name}'
            peer_tree.chmod(0o777)
            run_case(f'writable-peer-{name}-refuses-before-command', False,
                     "peer seats' private trees are denied")
            peer_tree.chmod(0o700)
        (data / 'skills').chmod(0o777)
        run_case('writable-global-refuses-before-command', False)
        (data / 'skills').chmod(0o555)
        after = {str(x.relative_to(root)): digest(x) for x in protected}
        if before != after:
            raise RuntimeError('protected synthetic bytes changed')
        receipt.update(protected_before=before, protected_after=after,
                       memory_after_sha256=digest(memory / 'MEMORY.md'), passed=True)
    except Exception as error:
        receipt.update(passed=False, error=str(error))
        raise
    finally:
        # No recovery data exists: every byte was created by this invocation.
        shutil.rmtree(root)
        receipt['fixture_removed'] = not root.exists()
        receipt['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        with args.receipt.open('x') as output:
            json.dump(receipt, output, indent=2, sort_keys=True)
            output.write('\n')
        print(json.dumps({k: receipt[k] for k in ['source_sha', 'binary_sha256', 'script_sha256', 'fixture_removed', 'passed']}))


if __name__ == '__main__':
    main()

"""Private PTY for a single native login; nothing is read from the operator's home."""
import errno
import http.client
import json
import os
import pty
import re
import select
import shutil
import signal
import socket
import struct
import sys
import termios
import time
import fcntl


# Match the Google OAuth menu on displayed text, not the raw terminal escapes.
ANSI = re.compile(rb'\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))')
AGY_GOOGLE_MENU = re.compile(
    rb'Select\s*login\s*method\s*:\s*>\s*1\s*\.\s*Google\s*OAuth'
)


def artifact_ready(path):
    # AGY stays in its interactive session after it writes the new credentials.
    try:
        with open(path, 'rb') as file:
            doc = json.loads(file.read(65536))
        return bool(doc.get('token', doc).get('access_token'))
    except (OSError, ValueError, AttributeError):
        return False


# The detached supervisor fails closed if the coordinator or transport goes
# away. A late launch cannot start a CLI until the coordinator polls its path.
POLL_GRACE = 12
stopping = False


def abandoned():
    if stopping or os.path.exists('stop'):
        return True
    try:
        return time.time() - os.stat('alive').st_mtime > POLL_GRACE
    except FileNotFoundError:
        return True


def run(harness, artifact, binary, args):
    home = os.path.abspath('home')
    os.makedirs(os.path.join(home, '.codex'), exist_ok=True)
    os.mkdir(os.path.join(home, 'tmp'))
    env = {k: os.environ[k] for k in ('PATH', 'SSL_CERT_FILE', 'SSL_CERT_DIR') if k in os.environ}
    env.update(HOME=home, XDG_CONFIG_HOME=home + '/.config', XDG_DATA_HOME=home + '/.local/share',
               XDG_CACHE_HOME=home + '/.cache', XDG_STATE_HOME=home + '/.local/state',
               TMPDIR=home + '/tmp', CLAUDE_CONFIG_DIR=home + '/.claude',
               AGY_CONFIG_DIR=home + '/.gemini/antigravity-cli', CODEX_HOME=home + '/.codex',
               BROWSER='true', NO_BROWSER='1', DISABLE_AUTOUPDATER='1', NO_COLOR='1',
               TERM='xterm' if harness == 'agy' else 'dumb', LANG='C.UTF-8')
    if not os.path.isabs(binary):
        return 127
    if abandoned():
        return 130
    with socket.socket(socket.AF_UNIX) as listener, open('output', 'wb', buffering=0) as log:
        # Relative socket path avoids sockaddr_un length limits on sandbox mounts.
        listener.bind('relay.sock')
        listener.listen(1)
        pid, master = pty.fork()
        if pid == 0:
            try:
                settings = termios.tcgetattr(0)
                settings[3] &= ~termios.ECHO
                termios.tcsetattr(0, termios.TCSANOW, settings)
                fcntl.ioctl(0, termios.TIOCSWINSZ, struct.pack('HHHH', 30, 160, 0, 0))
                os.chdir(home)
                os.execve(binary, [binary] + args, env)
            finally:
                os._exit(126)
        deadline = time.monotonic() + 240
        last_bytes, total, selected = b'', 0, False
        pty_eof = False
        try:
            while time.monotonic() < deadline:
                if abandoned():
                    return 130
                if pty_eof:
                    # Observe exit without reaping: the child's PID must stay
                    # reserved until we have killed any processes in its group.
                    status = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
                    if status is not None and status.si_pid:
                        return status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
                    time.sleep(0.05)
                    continue
                ready, _, _ = select.select([master, listener], [], [], 0.2)
                if master in ready:
                    try:
                        chunk = os.read(master, 8192)
                    except OSError as err:
                        if err.errno != errno.EIO:
                            raise
                        chunk = b''
                    if not chunk:
                        pty_eof = True
                        continue
                    total += len(chunk)
                    if total > 262144:
                        return 125
                    log.write(chunk)
                    if harness == 'agy' and not selected:
                        last_bytes = (last_bytes + chunk)[-8192:]
                        if AGY_GOOGLE_MENU.search(ANSI.sub(b'', last_bytes)):
                            os.write(master, b'\r')
                            selected = True
                if listener in ready:
                    with listener.accept()[0] as client:
                        client.settimeout(5)
                        try:
                            with client.makefile('rb') as stream:
                                relay(master, json.loads(stream.readline(16384)))
                            client.sendall(b'ok\n')
                        except (OSError, ValueError, KeyError, http.client.HTTPException):
                            try:
                                client.sendall(b'failed\n')
                            except OSError:
                                pass
                if harness == 'agy' and artifact_ready(os.path.join(home, artifact)):
                    return 0
            return 124
        finally:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                # The child may not yet have created its session/process group.
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            os.close(master)
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass


def relay(master, data):
    if data['mode'] == 'terminal':
        settings = termios.tcgetattr(master)
        settings[3] &= ~termios.ECHO
        termios.tcsetattr(master, termios.TCSANOW, settings)
        value = data['value'].encode('ascii')
        if not value or not value.isascii() or any(b < 33 or b > 126 for b in value):
            raise ValueError('invalid authorization code')
        # Character-at-a-time input also works when the CLI uses a raw PTY.
        os.write(master, value + b'\r')
    elif data['mode'] == 'loopback' and data['host'] in ('127.0.0.1', '::1', 'localhost'):
        conn = http.client.HTTPConnection(data['host'], data['port'], timeout=4)
        try:
            conn.request('GET', data['target'])
            if conn.getresponse().status >= 400:
                raise ValueError('callback rejected')
        finally:
            conn.close()
    else:
        raise ValueError('invalid callback')


def stop(_signal, _frame):
    # Use the same finally blocks for a provider-initiated graceful stop.
    global stopping
    stopping = True


def supervise(directory, harness, artifact, binary, args):
    os.umask(0o077)
    # Exclusive atomic creation. Never attach to or clean up an existing path.
    os.mkdir(directory, 0o700)
    try:
        os.chdir(directory)
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        deadline = time.monotonic() + POLL_GRACE
        while not os.path.exists('alive'):
            if stopping or os.path.exists('stop') or time.monotonic() >= deadline:
                return
            time.sleep(0.05)
        try:
            status = run(harness, artifact, binary, args)
        except (OSError, ValueError):
            status = 126
        with open('exit.tmp', 'w') as file:
            print(status, file=file)
        os.replace('exit.tmp', 'exit')
        # Keep the private files only until the coordinator has read them.
        until = time.monotonic() + 20
        while not abandoned() and time.monotonic() < until:
            time.sleep(0.1)
    finally:
        os.chdir('/')
        shutil.rmtree(directory)


if __name__ == '__main__':
    supervise(*sys.argv[1:5], sys.argv[5:])

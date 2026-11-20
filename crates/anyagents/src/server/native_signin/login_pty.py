"""Private PTY for a single native login; nothing is read from the operator's home."""
import errno
import http.client
import json
import os
import pty
import select
import signal
import socket
import struct
import sys
import termios
import time
import fcntl


def artifact_ready(path):
    # AGY stays in its interactive session after it writes the new credentials.
    try:
        with open(path, 'rb') as file:
            doc = json.loads(file.read(65536))
        return bool(doc.get('token', doc).get('access_token'))
    except (OSError, ValueError, AttributeError):
        return False


def run(harness, artifact, binary, args):
    home = os.path.abspath('home')
    os.makedirs(os.path.join(home, '.codex'), exist_ok=True)
    env = {k: os.environ[k] for k in ('PATH', 'SSL_CERT_FILE', 'SSL_CERT_DIR') if k in os.environ}
    env.update(HOME=home, XDG_CONFIG_HOME=home + '/.config', XDG_DATA_HOME=home + '/.local/share',
               XDG_CACHE_HOME=home + '/.cache', XDG_STATE_HOME=home + '/.local/state',
               AGY_CONFIG_DIR=home + '/.gemini/antigravity-cli', CODEX_HOME=home + '/.codex',
               BROWSER='true', NO_BROWSER='1', DISABLE_AUTOUPDATER='1', NO_COLOR='1',
               TERM='xterm' if harness == 'agy' else 'dumb', LANG='C.UTF-8')
    if not os.path.isabs(binary):
        return 127
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
        try:
            while time.monotonic() < deadline:
                if os.path.exists('stop'):
                    return 130
                ready, _, _ = select.select([master, listener], [], [], 0.2)
                if master in ready:
                    try:
                        chunk = os.read(master, 8192)
                    except OSError as err:
                        if err.errno != errno.EIO:
                            raise
                        break
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > 262144:
                        return 125
                    log.write(chunk)
                    if harness == 'agy' and not selected:
                        last_bytes = (last_bytes + chunk)[-8192:]
                        if b'Select login method' in last_bytes and b'Google' in last_bytes:
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
            else:
                return 124
            return os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1])
        finally:
            try:
                os.killpg(pid, signal.SIGKILL)
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


if __name__ == '__main__':
    os.umask(0o077)
    try:
        status = run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:])
    except (OSError, ValueError):
        status = 126
    with open('exit.tmp', 'w') as file:
        print(status, file=file)
    os.replace('exit.tmp', 'exit')

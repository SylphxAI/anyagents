//! Human-only vendor bridge. Child input is piped so it cannot echo keystrokes.
//! Raw/non-TTY callers retain the original runner in the parent module.
use super::input::{self, Action, Editor, Terminal};
use anyagents_installer::{InstallError, LineSink};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::process::Child;

/// Own cleanup as well as terminal state: an error/cancellation must not leave
/// a vendor waiting for input in the background.
pub(super) fn run(
    mut child: Child,
    mut terminal: Terminal,
    on_line: &mut dyn LineSink,
) -> Result<(), InstallError> {
    let result = proxy(&mut child, &mut terminal, on_line);
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait();
    let finish = terminal.finish();
    result.map_err(|err| InstallError::Failed(err.to_string()))?;
    finish.map_err(|err| InstallError::Failed(err.to_string()))?;
    let status = status.map_err(|err| InstallError::Failed(err.to_string()))?;
    if !status.success() {
        return Err(InstallError::Failed(format!("sign-in exited {status}")));
    }
    Ok(())
}

fn proxy(child: &mut Child, terminal: &mut Terminal, on_line: &mut dyn LineSink) -> io::Result<()> {
    let mut stdin = child.stdin.take();
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("missing vendor stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("missing vendor stderr"))?;
    let mut outputs = [Output::default(), Output::default()];
    let mut open = [true, true];
    let mut editor = Editor::default();
    let mut secrets = Vec::<Vec<u8>>::new();
    let mut buf = [0; 4096];
    terminal.draw(&editor)?;
    loop {
        let mut fds = [
            libc::pollfd {
                fd: if stdin.is_some() {
                    libc::STDIN_FILENO
                } else {
                    -1
                },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if open[0] { stdout.as_raw_fd() } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if open[1] { stderr.as_raw_fd() } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // A short wake also notices vendors which close output then exit, and
        // exits with descendants holding their pipes open. No stdin thread.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, 100) };
        if ready < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if fds[0].revents != 0 {
            let n = input::read_stdin(&mut buf)?;
            if n == 0 {
                stdin.take();
            }
            let mut dirty = false;
            for byte in &buf[..n] {
                match editor.feed(*byte)? {
                    Action::Cancel => return Err(input::cancelled()),
                    Action::Eof => {
                        stdin.take();
                    }
                    Action::Submit => {
                        let line = editor.take()?;
                        terminal.finish()?;
                        dirty = false;
                        if !line.is_empty() {
                            secrets.push(line.as_bytes().to_vec());
                        }
                        if let Some(stdin) = stdin.as_mut() {
                            stdin.write_all(line.as_bytes())?;
                            stdin.write_all(b"\n")?;
                            stdin.flush()?;
                        }
                    }
                    Action::Changed => dirty = true,
                    _ => {}
                }
            }
            if dirty {
                terminal.draw(&editor)?;
            }
        }
        for index in 0..2 {
            if fds[index + 1].revents == 0 {
                continue;
            }
            let n = if index == 0 {
                stdout.read(&mut buf)?
            } else {
                stderr.read(&mut buf)?
            };
            terminal.clear()?;
            outputs[index].push(&buf[..n], &secrets, n == 0, on_line)?;
            if n > 0 && buf[n - 1] != b'\n' {
                // Feedback must not erase a vendor's partial-line prompt.
                writeln!(io::stdout())?;
            }
            if n == 0 {
                open[index] = false;
            }
            terminal.draw(&editor)?;
        }
        if child.try_wait()?.is_some() && (ready == 0 || !open.iter().any(|open| *open)) {
            break;
        }
    }
    for output in &mut outputs {
        output.push(&[], &secrets, true, on_line)?;
    }
    Ok(())
}

#[derive(Default)]
struct Output {
    redactor: Redactor,
    protocol: Vec<u8>,
    passthrough: bool,
}

impl Output {
    fn push(
        &mut self,
        bytes: &[u8],
        secrets: &[Vec<u8>],
        end: bool,
        sink: &mut dyn LineSink,
    ) -> io::Result<()> {
        let bytes = self.redactor.push(bytes, secrets, end);
        let mut out = io::BufWriter::new(io::stdout().lock());
        for byte in bytes {
            if self.passthrough {
                out.write_all(&[byte])?;
                if byte == b'\n' {
                    self.passthrough = false;
                }
                continue;
            }
            self.protocol.push(byte);
            let prefixes: [&[u8]; 4] = [b"LINK ", b"DEVICE ", b"PROMPT ", b"SIGNED_IN "];
            let maybe_protocol = prefixes.iter().any(|prefix| {
                prefix.starts_with(&self.protocol) || self.protocol.starts_with(prefix)
            });
            if !maybe_protocol {
                out.write_all(&self.protocol)?;
                self.protocol.clear();
                self.passthrough = byte != b'\n';
            } else if byte == b'\n' {
                // Drop the stdout lock: TerminalSink writes to stdout itself.
                out.flush()?;
                drop(out);
                self.emit(sink);
                out = io::BufWriter::new(io::stdout().lock());
            }
        }
        // PROMPT is specifically a partial-line protocol; show its body now,
        // not after a newline the vendor may wait forever to print.
        if self.protocol.starts_with(b"PROMPT ") && self.protocol.len() > 7 {
            out.flush()?;
            drop(out);
            self.emit(sink);
            self.passthrough = true;
            out = io::BufWriter::new(io::stdout().lock());
        }
        if end && !self.protocol.is_empty() {
            out.flush()?;
            drop(out);
            self.emit(sink);
            out = io::BufWriter::new(io::stdout().lock());
        }
        out.flush()
    }

    fn emit(&mut self, sink: &mut dyn LineSink) {
        let line = String::from_utf8_lossy(&self.protocol);
        sink.line(line.trim_end_matches(['\r', '\n']));
        self.protocol.clear();
    }
}

/// Retain only a possible secret prefix between reads. Exact echoed input is
/// hidden even when a vendor splits it across arbitrary stdout/stderr chunks.
#[derive(Default)]
struct Redactor {
    pending: Vec<u8>,
}
impl Redactor {
    fn push(&mut self, bytes: &[u8], secrets: &[Vec<u8>], end: bool) -> Vec<u8> {
        self.pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        let mut at = 0;
        while at < self.pending.len() {
            let rest = &self.pending[at..];
            // A shorter submitted value may prefix a later one. Retain the
            // ambiguous suffix until the longer match can be decided, and
            // conceal it at EOF even if it is only a truncated longer echo.
            if secrets
                .iter()
                .any(|s| s.len() > rest.len() && s.starts_with(rest))
            {
                if end {
                    out.extend_from_slice(b"[hidden]");
                    at = self.pending.len();
                }
                break;
            } else if let Some(secret) = secrets
                .iter()
                .filter(|s| !s.is_empty() && rest.starts_with(s))
                .max_by_key(|s| s.len())
            {
                out.extend_from_slice(b"[hidden]");
                at += secret.len();
            } else {
                out.push(rest[0]);
                at += 1;
            }
        }
        self.pending.drain(..at);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn echoed_secrets_are_redacted_across_all_chunk_boundaries() {
        let secret = b"synthetic-secret-code".to_vec();
        for split in 0..=secret.len() {
            let mut redactor = Redactor::default();
            let secrets = [secret.clone()];
            let mut out = redactor.push(&secret[..split], &secrets, false);
            out.extend(redactor.push(&secret[split..], &secrets, true));
            assert_eq!(out, b"[hidden]");
        }
    }
    #[test]
    fn overlapping_secrets_choose_the_longest_match_across_chunks() {
        for secrets in [
            [b"abc".to_vec(), b"abcdef".to_vec()],
            [b"abcdef".to_vec(), b"abc".to_vec()],
        ] {
            for echo in [b"abcdef".as_slice(), b"abcd"] {
                for split in 0..=echo.len() {
                    let mut redactor = Redactor::default();
                    let mut out = redactor.push(&echo[..split], &secrets, false);
                    out.extend(redactor.push(&echo[split..], &secrets, true));
                    assert_eq!(out, b"[hidden]");
                }
            }
            let mut redactor = Redactor::default();
            let mut out = Vec::new();
            for byte in b"abcdef / abcX" {
                out.extend(redactor.push(&[*byte], &secrets, false));
            }
            out.extend(redactor.push(b"", &secrets, true));
            assert_eq!(out, b"[hidden] / [hidden]X");
        }
    }

    #[test]
    fn redaction_keeps_unrelated_output_and_hides_truncated_echo() {
        let mut redactor = Redactor::default();
        let secrets = [b"synthetic-secret-code".to_vec()];
        assert_eq!(
            redactor.push(b"URL https://example.test/\nsynthe", &secrets, true),
            b"URL https://example.test/\n[hidden]"
        );
    }
}

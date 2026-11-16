//! Single-line secret entry. The terminal owns presentation, never the secret.

#[cfg(not(unix))]
use std::io::Read;
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

pub(super) fn interactive() -> bool {
    cfg!(unix)
        && std::env::var_os("ANYAGENTS_RAW").is_none()
        && io::stdin().is_terminal()
        && io::stdout().is_terminal()
}

/// Read one secret without its line ending. Interactive terminals show only a
/// mask and character count, including during bracketed paste. Ctrl+U clears,
/// Backspace deletes a Unicode scalar, Ctrl+C cancels, and Ctrl+D/EOF aborts.
/// Pasted line breaks/control characters are ignored: paste never submits.
///
/// Non-TTY/ANYAGENTS_RAW input remains a plain line with no prompt or escapes.
/// A timeout is a single deadline, not restarted by typing. No reader thread is
/// left behind on timeout. The terminal is restored on every return path.
pub fn read_secret_line(prompt: &str, timeout: Option<Duration>) -> io::Result<String> {
    let deadline = timeout
        .map(|duration| {
            Instant::now().checked_add(duration).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "input timeout is too large")
            })
        })
        .transpose()?;
    #[cfg(unix)]
    if interactive() {
        let mut terminal = Terminal::new()?;
        if !prompt.is_empty() {
            writeln!(io::stdout(), "{prompt}")?;
        }
        return terminal_line(&mut terminal, deadline);
    }
    plain_line(deadline)
}

/// The caller can acquire the guard before displaying a vendor's prompt, so
/// input pasted as soon as that prompt appears cannot be echoed by the TTY.
#[cfg(unix)]
pub(super) fn terminal_line(
    terminal: &mut Terminal,
    deadline: Option<Instant>,
) -> io::Result<String> {
    let mut editor = Editor::default();
    let mut dirty = false;
    let mut last_draw = Instant::now();
    terminal.draw(&editor)?;
    let result = (|| {
        loop {
            wait_readable(libc::STDIN_FILENO, deadline)?;
            // One byte avoids consuming a following line belonging to a
            // later prompt. Redraw once per available burst, not per byte.
            let mut byte = [0];
            if read_stdin(&mut byte)? == 0 {
                return Err(eof());
            }
            match editor.feed(byte[0])? {
                Action::Submit => return editor.take(),
                Action::Cancel => return Err(cancelled()),
                Action::Eof => return Err(eof()),
                Action::Changed => dirty = true,
                _ => {}
            }
            if dirty
                && (last_draw.elapsed() >= Duration::from_millis(16)
                    || !readable(libc::STDIN_FILENO, 0)?)
            {
                terminal.draw(&editor)?;
                last_draw = Instant::now();
                dirty = false;
            }
        }
    })();
    let finish = terminal.finish();
    let line = result?;
    finish?;
    Ok(line)
}

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "standard input closed")
}

pub(super) fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "secret entry cancelled")
}

fn plain_line(deadline: Option<Instant>) -> io::Result<String> {
    let mut bytes = Vec::new();
    loop {
        #[cfg(unix)]
        wait_readable(libc::STDIN_FILENO, deadline)?;
        #[cfg(not(unix))]
        if deadline.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "input deadlines require Unix",
            ));
        }
        let mut byte = [0];
        if read_stdin(&mut byte)? == 0 {
            if bytes.is_empty() {
                return Err(eof());
            }
            break;
        }
        if byte[0] == b'\n' {
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
            break;
        }
        bytes.push(byte[0]);
    }
    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "input is not UTF-8"))
}

#[cfg(unix)]
pub(super) fn read_stdin(bytes: &mut [u8]) -> io::Result<usize> {
    // SAFETY: read is given a live writable buffer; it cannot overrun it.
    let n = unsafe { libc::read(libc::STDIN_FILENO, bytes.as_mut_ptr().cast(), bytes.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

#[cfg(not(unix))]
fn read_stdin(bytes: &mut [u8]) -> io::Result<usize> {
    io::stdin().read(bytes)
}

#[cfg(unix)]
pub(super) fn readable(fd: libc::c_int, milliseconds: i32) -> io::Result<bool> {
    let mut fd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: poll receives one live pollfd for the duration of the call.
    let result = unsafe { libc::poll(&mut fd, 1, milliseconds) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if fd.revents & libc::POLLNVAL != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input descriptor is closed",
        ));
    }
    Ok(result > 0)
}

#[cfg(unix)]
fn wait_readable(fd: libc::c_int, deadline: Option<Instant>) -> io::Result<()> {
    loop {
        let milliseconds = if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "secret entry timed out",
                ));
            }
            remaining.as_millis().max(1).min(i32::MAX as u128) as i32
        } else {
            -1
        };
        match readable(fd, milliseconds) {
            Ok(true) => return Ok(()),
            Ok(false) => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
}

#[derive(Default)]
pub(super) struct Editor {
    bytes: Vec<u8>,
    escape: Vec<u8>,
    pasting: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Action {
    None,
    Changed,
    Submit,
    Cancel,
    Eof,
}

impl Editor {
    pub(super) fn count(&self) -> usize {
        self.bytes.iter().filter(|b| **b & 0xc0 != 0x80).count()
    }

    pub(super) fn take(&mut self) -> io::Result<String> {
        String::from_utf8(std::mem::take(&mut self.bytes))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "input is not UTF-8"))
    }

    pub(super) fn feed(&mut self, byte: u8) -> io::Result<Action> {
        if !self.escape.is_empty() {
            self.escape.push(byte);
            // CSI sequences end with a byte in 0x40..=0x7e. Wait past '['.
            if self.escape.len() == 2 && byte == b'[' {
                return Ok(Action::None);
            }
            if self.escape.len() == 2 || (0x40..=0x7e).contains(&byte) || self.escape.len() > 32 {
                match self.escape.as_slice() {
                    b"\x1b[200~" => self.pasting = true,
                    b"\x1b[201~" => self.pasting = false,
                    _ => {}
                }
                self.escape.clear();
            }
            return Ok(Action::None);
        }
        if byte == 0x1b {
            self.escape.push(byte);
            return Ok(Action::None);
        }
        if self.pasting && (byte < 0x20 || byte == 0x7f) {
            return Ok(Action::None);
        }
        let action = match byte {
            b'\r' | b'\n' => Action::Submit,
            3 => Action::Cancel,
            4 => Action::Eof,
            8 | 127 => {
                while self.bytes.pop().is_some_and(|b| b & 0xc0 == 0x80) {}
                Action::Changed
            }
            21 => {
                self.bytes.clear();
                Action::Changed
            }
            0..=31 => Action::None,
            _ => {
                // Generous bound prevents unbounded clipboard allocation.
                if self.bytes.len() >= 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "secret input is too long",
                    ));
                }
                self.bytes.push(byte);
                Action::Changed
            }
        };
        Ok(action)
    }
}

#[cfg(unix)]
pub(super) struct Terminal {
    saved: libc::termios,
    shown: bool,
}

#[cfg(unix)]
impl Terminal {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: tcgetattr initializes this termios before it is read.
        let mut saved = unsafe { std::mem::zeroed::<libc::termios>() };
        if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut saved) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut raw = saved;
        raw.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ECHONL | libc::ISIG | libc::IEXTEN);
        raw.c_iflag &= !(libc::ICRNL | libc::INLCR | libc::IGNCR | libc::IXON);
        raw.c_cc[libc::VMIN] = 1;
        raw.c_cc[libc::VTIME] = 0;
        // SAFETY: saved/raw are valid terminal attributes for stdin.
        if unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let terminal = Self {
            saved,
            shown: false,
        };
        let mut out = io::stdout().lock();
        out.write_all(b"\x1b[?2004h")?;
        out.flush()?;
        Ok(terminal)
    }

    pub(super) fn clear(&mut self) -> io::Result<()> {
        if self.shown {
            let mut out = io::stdout().lock();
            out.write_all(b"\r\x1b[2K")?;
            out.flush()?;
            self.shown = false;
        }
        Ok(())
    }

    pub(super) fn draw(&mut self, editor: &Editor) -> io::Result<()> {
        // Keep the feedback on one row, even on narrow terminals.
        let mut size = unsafe { std::mem::zeroed::<libc::winsize>() };
        // SAFETY: ioctl writes only the live winsize supplied here.
        let width = if unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) } == 0
            && size.ws_col > 0
        {
            size.ws_col as usize
        } else {
            80
        };
        let count = editor.count();
        let dots = "•".repeat(count.min(20).min(width.saturating_sub(22)));
        let mut text = format!("  {dots}  {count} chars");
        if width >= 80 {
            text.push_str(" · Enter submit · Ctrl+U clear · Ctrl+C cancel");
        }
        // Scalar count is also display width here: only ASCII and one-cell dots.
        let text: String = text.chars().take(width.saturating_sub(1)).collect();
        let mut out = io::stdout().lock();
        write!(out, "\r\x1b[2K{text}")?;
        out.flush()?;
        self.shown = true;
        Ok(())
    }

    pub(super) fn finish(&mut self) -> io::Result<()> {
        self.clear()?;
        writeln!(io::stdout())?;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for Terminal {
    fn drop(&mut self) {
        // Restore before writing: broken stdout must not leave input raw.
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.saved);
        }
        let mut out = io::stdout().lock();
        let _ = out.write_all(b"\x1b[?2004l");
        let _ = out.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_timeout_is_rejected_before_reading_input() {
        assert_eq!(
            read_secret_line("", Some(Duration::MAX))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn paste_is_incremental_mask_counted_and_never_submits() {
        let mut editor = Editor::default();
        for byte in b"\x1b[200~first\r\nsecond\x03\x1b[201~" {
            assert!(!matches!(
                editor.feed(*byte).unwrap(),
                Action::Submit | Action::Cancel
            ));
        }
        assert_eq!(editor.count(), 11);
        assert_eq!(editor.feed(b'\r').unwrap(), Action::Submit);
        assert_eq!(editor.take().unwrap(), "firstsecond");
    }

    #[test]
    fn unicode_backspace_clear_and_cancel() {
        let mut editor = Editor::default();
        for byte in "a中文🔒".bytes() {
            editor.feed(byte).unwrap();
        }
        assert_eq!(editor.count(), 4);
        editor.feed(127).unwrap();
        assert_eq!(editor.count(), 3);
        assert_eq!(editor.take().unwrap(), "a中文");
        editor.feed(b'x').unwrap();
        editor.feed(21).unwrap();
        assert_eq!(editor.count(), 0);
        assert_eq!(editor.feed(3).unwrap(), Action::Cancel);
        assert_eq!(editor.feed(4).unwrap(), Action::Eof);
    }

    #[test]
    fn navigation_and_split_paste_markers_are_not_input() {
        let mut editor = Editor::default();
        for byte in b"\x1b[A\x1b[D\x1b[200~abc\x1b[201~\x1b[3~" {
            editor.feed(*byte).unwrap();
        }
        assert_eq!(editor.take().unwrap(), "abc");
    }
}

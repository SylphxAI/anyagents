export type SignInEvent = { kind: string; text?: string };

/** Resolve or reject at the first terminal event, even if the transport stays open. */
export async function readSignInStream(body: ReadableStream<Uint8Array<ArrayBuffer>> | null, onEvent: (event: SignInEvent) => void) {
  if (!body) throw new Error('The sign-in stream is unavailable.');
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  const handle = (line: string) => {
    if (!line.trim()) return;
    const event = JSON.parse(line) as SignInEvent;
    if (event.kind === 'done') return event.text ?? '';
    if (event.kind === 'error') throw new Error(event.text || 'Sign-in failed.');
    onEvent(event);
  };
  try {
    let tail = '';
    while (true) {
      const { value, done } = await reader.read();
      const lines = (done ? tail : tail + value).split('\n');
      tail = done ? '' : lines.pop() ?? '';
      for (const line of lines) {
        const label = handle(line);
        if (label !== undefined) return label;
      }
      if (done) throw new Error('The sign-in stream ended before the provider confirmed the account.');
    }
  } finally {
    // Cancellation can wait on an upstream transport; never delay the UI on it.
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

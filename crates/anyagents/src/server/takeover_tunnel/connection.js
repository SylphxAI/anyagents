// WS and EventSource hide upgrade/response statuses. One authenticated HTTP
// probe arbitrates both retries without replacing the existing desktop page.
export function desktopTransport(connectRfb) {
  const base = new URL('./', location.href);
  const pill = document.createElement('p');
  pill.setAttribute('role', 'status');
  pill.textContent = 'Reconnecting…';
  pill.style.cssText = 'position:fixed;top:0.75rem;left:50%;transform:translateX(-50%);z-index:10;margin:0;padding:.4rem .8rem;border-radius:1rem;background:#20252e;color:white;pointer-events:none';
  pill.hidden = true;
  document.body.append(pill);

  const lostChannels = new Set();
  const delays = {rfb: 500, sse: 500};
  const due = {rfb: 0, sse: 0};
  let timer, probing = false, stopped = false, startEvents;
  const request = path => fetch(new URL(path, base), {
    credentials: 'same-origin', cache: 'no-store', redirect: 'error',
    signal: AbortSignal.timeout(12000),
  });
  const now = () => performance.now();

  function schedule() {
    clearTimeout(timer);
    if (stopped || probing || !lostChannels.size) return;
    const next = Math.min(...Array.from(lostChannels, channel => due[channel]));
    timer = setTimeout(retry, Math.max(0, next - now()));
  }

  // Keep showing the pill until the actual RFB handshake and/or SSE state frame,
  // not merely the HTTP probe. Advance backoff only for channels that failed.
  function lost(channel) {
    if (stopped) return;
    if (!lostChannels.has(channel)) {
      lostChannels.add(channel);
      due[channel] = now() + delays[channel];
      delays[channel] = Math.min(2000, 2 * delays[channel]);
    }
    pill.hidden = false;
    schedule();
  }

  let awaiting = new Set();
  function ready(channel) {
    awaiting.delete(channel);
    lostChannels.delete(channel);
    delays[channel] = 500;
    if (!lostChannels.size && !awaiting.size) pill.hidden = true;
    schedule();
  }

  async function retry() {
    probing = true;
    try {
      const response = await request('_connection');
      if ([401, 403, 404, 410].includes(response.status)) {
        stopped = true;
        // Use the existing returned receipt and expired/reset navigation. Never
        // infer successful handback from revocation or a broken connection.
        let returned = false;
        try { returned = (await request('_returned')).ok; } catch { /* show recovery */ }
        location.replace(new URL(returned ? '_returned' : './', base));
        return;
      }
      if (response.status !== 204) {
        const seconds = response.status === 503 ? Number(response.headers.get('Retry-After')) : 0;
        defer(Number.isFinite(seconds) ? seconds * 1000 : 0);
        return;
      }
      const time = now();
      for (const channel of lostChannels) {
        if (due[channel] > time) continue;
        lostChannels.delete(channel);
        awaiting.add(channel);
        try { (channel === 'rfb' ? connectRfb : startEvents)(); }
        catch { lost(channel); }
      }
    } catch {
      defer(0);
    } finally {
      probing = false;
      schedule();
    }
  }

  function defer(minimum) {
    const time = now();
    for (const channel of lostChannels) {
      due[channel] = Math.max(due[channel], time + Math.max(minimum, delays[channel]));
      delays[channel] = Math.min(2000, 2 * delays[channel]);
    }
  }

  function events(apply, suspend) {
    startEvents = () => {
      const stream = new EventSource(new URL('events', base));
      stream.addEventListener('state', event => {
        apply(JSON.parse(event.data));
        ready('sse');
      });
      stream.addEventListener('error', () => {
        stream.close(); // Do not let native SSE retries race the status probe.
        suspend();
        lost('sse');
      }, {once: true});
    };
    startEvents();
  }

  return {events, lost, ready};
}

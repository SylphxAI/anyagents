import { useCallback, useEffect, useState } from 'react';

export type RequestOptions = {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: object;
  signal?: AbortSignal;
  accept?: string;
};

export async function request(path: string, options: RequestOptions = {}): Promise<Response> {
  const { method = 'GET', body, signal, accept = 'application/json' } = options;
  const headers = new Headers({ accept });
  if (method !== 'GET') headers.set('x-anyagents-admin-csrf', '1');
  if (body !== undefined) headers.set('content-type', 'application/json');
  const response = await fetch(path, { method, credentials: 'same-origin', headers, signal,
    body: body === undefined ? undefined : JSON.stringify(body) });
  if (response.status === 401 && path !== '/admin/auth/me') window.dispatchEvent(new Event('session-expired'));
  if (!response.ok) {
    const error = await response.json().catch(() => null) as { error?: unknown; message?: unknown } | null;
    const message = error?.error ?? error?.message;
    throw new Error(typeof message === 'string' ? message : 'HTTP ' + response.status);
  }
  return response;
}

export async function api<T = unknown>(path: string, options?: RequestOptions): Promise<T> {
  const response = await request(path, options);
  const text = await response.text();
  return (text ? JSON.parse(text) : null) as T;
}

export function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Something went wrong. Try again.';
}

export function agentPath(id: string) { return '/v1/agents/' + encodeURIComponent(id); }

export function useResource<T>(load: (signal: AbortSignal) => Promise<T>, interval = 0) {
  const [snapshot, setSnapshot] = useState<{ load: typeof load; data?: T; error?: string; updated?: number }>({ load });
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision(value => value + 1), []);
  useEffect(() => {
    const controller = new AbortController();
    let timer: number | undefined;
    async function update() {
      try {
        const data = await load(controller.signal);
        if (!controller.signal.aborted) setSnapshot({ load, data, updated: Date.now() });
      } catch (error) {
        if (!controller.signal.aborted) setSnapshot(previous => ({
          ...(previous.load === load ? previous : { load }), error: errorMessage(error),
        }));
      } finally {
        if (interval && !controller.signal.aborted) timer = window.setTimeout(update, interval);
      }
    }
    void update();
    return () => { controller.abort(); window.clearTimeout(timer); };
  }, [load, interval, revision]);
  return { ...(snapshot.load === load ? snapshot : { load, data: undefined, error: undefined, updated: undefined }), refresh };
}

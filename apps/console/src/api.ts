import { createEffect, createSignal, onCleanup, untrack } from 'solid-js';

export type RequestOptions = {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: object;
  signal?: AbortSignal;
  accept?: string;
};

export class HttpError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}

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
    throw new HttpError(typeof message === 'string' ? message : 'Request failed (HTTP ' + response.status + ')', response.status);
  }
  return response;
}

export async function api<T = unknown>(path: string, options?: RequestOptions): Promise<T> {
  const response = await request(path, options);
  const text = await response.text();
  return (text ? JSON.parse(text) : null) as T;
}

/** True when the coordinator does not serve this endpoint yet (older build). */
export function isUnavailable(error: unknown) {
  return error instanceof HttpError && [404, 405, 501].includes(error.status);
}

/** Resolves to `null` when the endpoint does not exist on this coordinator. */
export async function optional<T>(path: string, options?: RequestOptions): Promise<T | null> {
  try { return await api<T>(path, options); } catch (error) { if (isUnavailable(error)) return null; throw error; }
}

export function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Something went wrong. Try again.';
}

export function agentPath(name: string) { return '/v1/agents/' + encodeURIComponent(name); }

export interface Resource<T> {
  data: () => T | undefined;
  error: () => string | undefined;
  updated: () => number | undefined;
  loading: () => boolean;
  refresh: () => void;
}

/**
 * Loads (and optionally polls) a JSON resource. `key` is tracked: when it
 * changes the previous data is dropped and the loader runs again. The loader
 * itself is untracked, so it may read any signal at call time.
 */
export function createPoll<T>(load: (signal: AbortSignal) => Promise<T>, options: { interval?: number; key?: () => unknown } = {}): Resource<T> {
  const [state, setState] = createSignal<{ data?: T; error?: string; updated?: number }>({});
  const [revision, setRevision] = createSignal(0);
  let lastKey: unknown = Symbol('initial');
  createEffect(() => {
    const key = options.key?.();
    revision();
    if (key !== lastKey) { lastKey = key; setState({}); }
    const controller = new AbortController();
    let timer: number | undefined;
    const update = async () => {
      try {
        const data = await untrack(() => load(controller.signal));
        if (!controller.signal.aborted) setState({ data, updated: Date.now() });
      } catch (error) {
        if (!controller.signal.aborted) setState(previous => ({ ...previous, error: errorMessage(error) }));
      } finally {
        if (options.interval && !controller.signal.aborted) timer = window.setTimeout(update, document.hidden ? options.interval * 3 : options.interval);
      }
    };
    void update();
    onCleanup(() => { controller.abort(); window.clearTimeout(timer); });
  });
  return {
    data: () => state().data,
    error: () => state().error,
    updated: () => state().updated,
    loading: () => state().data === undefined && !state().error,
    refresh: () => setRevision(value => value + 1),
  };
}

import type { Route, SettingsTab } from './types';

const settingsTabs: SettingsTab[] = ['channels', 'ingress', 'secrets', 'automations', 'usage'];

export function parseRoute(hash = window.location.hash): Route {
  const [section = '', raw] = hash.replace(/^#\/?/, '').split('/');
  let id: string | undefined;
  try { id = raw ? decodeURIComponent(raw) : undefined; } catch { /* Ignore a malformed link. */ }
  switch (section) {
    case 'agents': case 'fleet': return { page: 'agents', id };
    case 'nodes': return { page: 'nodes', id };
    case 'accounts': case 'profiles': return { page: 'accounts' };
    case 'knowledge': return { page: 'knowledge' };
    case 'versions': return { page: 'versions' };
    case 'issues': return { page: 'issues' };
    case 'settings': return { page: 'settings', tab: settingsTabs.find(tab => tab === id) ?? 'channels' };
    case 'channels': case 'secrets': case 'automations': case 'usage': return { page: 'settings', tab: section };
    case 'schedules': return { page: 'settings', tab: 'automations' };
    default: return { page: 'overview' };
  }
}

export function href(route: Route): string {
  switch (route.page) {
    case 'agents': return '#agents' + (route.id ? '/' + encodeURIComponent(route.id) : '');
    case 'nodes': return '#nodes' + (route.id ? '/' + encodeURIComponent(route.id) : '');
    case 'settings': return '#settings/' + route.tab;
    default: return '#' + route.page;
  }
}

export const agentHref = (name?: string) => href({ page: 'agents', id: name });
export function go(route: Route) { window.location.hash = href(route); }

export function pageHref(page: Route['page']) {
  return page === 'settings' ? href({ page, tab: 'channels' }) : page === 'agents' ? href({ page }) : '#' + page;
}

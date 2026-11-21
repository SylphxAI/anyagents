import { createContext, useContext } from 'solid-js';
import type { Resource } from './api';
import type { Forest } from './lib/tree';
import type { Account, ProviderGroup } from './lib/providers';
import type { RuntimeIssue } from './lib/issues';
import type { Agent, Overview, Profile, Session, VersionsReport } from './types';

export type AgentAction =
  | { kind: 'spawn'; parent?: Agent }
  | { kind: 'runtime' | 'pause' | 'wake' | 'archive' | 'grants'; agent: Agent };

export interface ConnectRequest { provider?: string; hint?: string; tab?: 'oauth' | 'key' }

export interface Fleet {
  agents: Agent[];
  live: Agent[];
  forest: Forest<Agent>;
  working: Agent[];
  errored: Agent[];
  archived: number;
}

export interface ConsoleState {
  session: Resource<Session>;
  roster: Resource<{ agents: Agent[] }>;
  overview: Resource<Overview>;
  profiles: Resource<Profile[]>;
  versions: Resource<VersionsReport | null>;
  issues: Resource<RuntimeIssue[] | null>;
  fleet: () => Fleet;
  groups: () => ProviderGroup<Profile>[];
  accounts: () => Account<Profile>[];
  revision: () => number;
  refreshAll: () => void;
  refreshProfiles: (fresh?: boolean) => void;
  act: (action: AgentAction) => void;
  connect: (request?: ConnectRequest) => void;
  openPalette: () => void;
}

export const ConsoleContext = createContext<ConsoleState>();

export function useConsole() {
  const value = useContext(ConsoleContext);
  if (!value) throw new Error('Console state is unavailable outside the app shell.');
  return value;
}

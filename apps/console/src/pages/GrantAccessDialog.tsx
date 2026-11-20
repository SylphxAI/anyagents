import { For, Show, createMemo, createSignal } from 'solid-js';
import { errorMessage } from '../api';
import { useConsole } from '../state';
import { Field, FormDialog, formText } from '../ui/primitives';

export const grantModes = ['read', 'write', 'propose'] as const;
export type GrantMode = typeof grantModes[number];

export function GrantAccessDialog(props: {
  close: () => void;
  grant: (agent: string, pattern: string, mode: GrantMode) => Promise<unknown>;
  refresh: () => void;
}) {
  const { fleet } = useConsole();
  const [query, setQuery] = createSignal('');
  const [recipient, setRecipient] = createSignal('');
  const [modes, setModes] = createSignal<GrantMode[]>([]);
  const live = createMemo(() => [...fleet().live].sort((a, b) => a.name.localeCompare(b.name)));
  const agents = createMemo(() => {
    const search = query().trim().toLowerCase();
    return live().filter(agent => agent.id === recipient() || agent.name.toLowerCase().includes(search) || agent.id.toLowerCase().includes(search));
  });

  return <FormDialog title="Grant access" description="Choose an agent, a namespace pattern, and the permissions to grant."
    action="Grant Access" close={props.close} submit={async form => {
      const choice = formText(form, 'recipient');
      const agent = choice === 'manual' ? formText(form, 'agent_id') : choice;
      const pattern = formText(form, 'namespace_pattern');
      const selected = grantModes.filter(mode => modes().includes(mode));
      if (!/^agt_\S+$/.test(agent)) throw new Error('Choose an agent or enter a valid agent ID (agt_…).');
      if (!pattern) throw new Error('Enter a namespace pattern.');
      if (!selected.length) throw new Error('Select at least one permission.');
      const applied: GrantMode[] = [];
      try {
        for (const mode of selected) {
          try { await props.grant(agent, pattern, mode); }
          catch (cause) {
            const prefix = applied.length ? 'Granted ' + applied.join(', ') + '. ' : '';
            throw new Error(prefix + 'Could not grant ' + mode + ': ' + errorMessage(cause));
          }
          applied.push(mode);
        }
      } finally {
        setModes(current => current.filter(mode => !applied.includes(mode)));
        props.refresh();
      }
    }}>
    <Field label="Find an agent">
      <input type="search" class="input" placeholder="Search by name or ID" value={query()} onInput={event => setQuery(event.currentTarget.value)} />
    </Field>
    <Field label="Agent" hint={!agents().length && query() ? 'No live agents match this search. You can enter an ID manually.' : undefined}>
      <select class="input" name="recipient" required value={recipient()} onChange={event => setRecipient(event.currentTarget.value)}>
        <option value="" disabled>Select an agent</option>
        <For each={agents()}>{agent => <option value={agent.id}>{agent.name} · {agent.id}</option>}</For>
        <option value="manual">Enter an agent ID manually</option>
      </select>
    </Field>
    <Show when={recipient() === 'manual'}><Field label="Agent ID">
      <input class="input mono" name="agent_id" placeholder="agt_…" required autocomplete="off" spellcheck={false} />
    </Field></Show>
    <Field label="Namespace pattern" hint={<>Use an exact namespace or a pattern like <code>domain:team:*</code>.</>}>
      <input class="input mono" name="namespace_pattern" placeholder="org:*" required autocomplete="off" spellcheck={false} />
    </Field>
    <fieldset class="field">
      <legend class="field-label">Permissions</legend>
      <div class="grant-modes"><For each={grantModes}>{mode => <label class="grant-mode">
        <input type="checkbox" checked={modes().includes(mode)} onChange={event => {
          const checked = event.currentTarget.checked;
          setModes(current => checked ? [...current, mode] : current.filter(value => value !== mode));
        }} />{mode}
      </label>}</For></div>
      <p class="field-hint">Permissions are independent. Select all that apply.</p>
    </fieldset>
  </FormDialog>;
}

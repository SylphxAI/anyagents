import { For, Show, createMemo, createSignal } from 'solid-js';
import { errorMessage } from '../api';
import { useConsole } from '../state';
import { Field, FormDialog, formText } from '../ui/primitives';

export const grantModes = ['read', 'write', 'propose'] as const;
export type GrantMode = typeof grantModes[number];
export type GrantRecipient = { agent_id: string } | { group_id: string };

export function GrantAccessDialog(props: {
  close: () => void;
  grant: (recipient: GrantRecipient, pattern: string, mode: GrantMode) => Promise<unknown>;
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

  return <FormDialog title="Grant access" description="Choose an agent or group, a namespace pattern, and the permissions to grant."
    action="Grant Access" close={props.close} submit={async form => {
      const choice = formText(form, 'recipient');
      const target: GrantRecipient = choice === 'group'
        ? { group_id: formText(form, 'group_id') }
        : { agent_id: choice === 'manual' ? formText(form, 'agent_id') : choice };
      if ('group_id' in target) {
        if (!/^[^:\s]+$/.test(target.group_id)) throw new Error('Enter the existing group name without the group: prefix.');
      } else if (!/^agt_\S+$/.test(target.agent_id)) {
        throw new Error('Choose an agent or enter a valid agent ID (agt_…).');
      }
      const pattern = formText(form, 'namespace_pattern');
      const selected = grantModes.filter(mode => modes().includes(mode));
      if (!pattern) throw new Error('Enter a namespace pattern.');
      if (!selected.length) throw new Error('Select at least one permission.');
      const applied: GrantMode[] = [];
      try {
        for (const mode of selected) {
          try { await props.grant(target, pattern, mode); }
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
    <Field label="Recipient" hint={!agents().length && query() ? 'No live agents match. You can enter an agent ID or group name.' : undefined}>
      <select class="input" name="recipient" required value={recipient()} onChange={event => setRecipient(event.currentTarget.value)}>
        <option value="" disabled>Select an agent or group</option>
        <For each={agents()}>{agent => <option value={agent.id}>{agent.name} · {agent.id}</option>}</For>
        <option value="group">Enter a group name</option>
        <option value="manual">Enter an agent ID manually</option>
      </select>
    </Field>
    <Show when={recipient() === 'manual'}><Field label="Agent ID">
      <input class="input mono" name="agent_id" placeholder="agt_…" required autocomplete="off" spellcheck={false} />
    </Field></Show>
    <Show when={recipient() === 'group'}><Field label="Group name" hint="All current and future members inherit this grant.">
      <input class="input mono" name="group_id" required autocomplete="off" spellcheck={false} />
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
      <p class="field-hint">Write can publish changes. Propose creates drafts only; live agents can already draft in organization namespaces.</p>
    </fieldset>
  </FormDialog>;
}

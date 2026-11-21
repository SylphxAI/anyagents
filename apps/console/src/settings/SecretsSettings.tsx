import { For, Show, createSignal } from 'solid-js';
import { KeyRound, Lock, Plus } from 'lucide-solid';
import { api, createPoll } from '../api';
import { ago } from '../format';
import { agentHref } from '../route';
import { useConsole } from '../state';
import { Empty, Field, FormDialog, Problem, Section, Skeleton, formText } from '../ui/primitives';

type Bundle = { name: string; env_keys?: string[]; granted_agents?: string[]; granted_agent_ids?: string[]; updated_at_ms?: number };

export function SecretsSettings() {
  const { fleet, act, revision } = useConsole();
  const secrets = createPoll(signal => api<{ secrets: Bundle[] }>('/v1/secrets', { signal }), { interval: 30000, key: revision });
  const [edit, setEdit] = createSignal<Bundle | 'new'>();
  const [remove, setRemove] = createSignal<Bundle>();
  const [target, setTarget] = createSignal('');
  const close = () => { setEdit(undefined); setRemove(undefined); };
  return <>
    <div class="notice"><Lock size={14} aria-hidden="true" /><span><span class="strong">Write-only values.</span> Bundle names and environment keys are visible; encrypted values can never be read back through the console.</span></div>
    <Section title="Deployment bundles" meta={<Show when={secrets.data()}><span class="num subtle">{secrets.data()!.secrets.length}</span></Show>}
      actions={<button type="button" class="btn primary sm" onClick={() => setEdit('new')}><Plus size={13} />Seal bundle</button>}>
      <Show when={secrets.error()}><Problem retry={secrets.refresh}>{secrets.error()}</Problem></Show>
      <Show when={secrets.loading()}><Skeleton rows={4} /></Show>
      <Show when={secrets.data()}>{data => <Show when={data().secrets.length}
        fallback={<Empty compact icon={<KeyRound size={18} />} title="No bundles sealed" action={<button type="button" class="btn sm" onClick={() => setEdit('new')}><Plus size={13} />Seal bundle</button>}>
          Seal API tokens and credentials once, then grant them to the agents that need them.</Empty>}>
        <div class="table-wrap"><table class="table">
          <thead><tr><th>Bundle</th><th>Environment keys</th><th>Granted to</th><th>Updated</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
          <tbody><For each={data().secrets}>{bundle => {
            const granted = bundle.granted_agents?.length ? bundle.granted_agents : bundle.granted_agent_ids ?? [];
            return <tr>
              <td class="mono strong">{bundle.name}</td>
              <td><span class="chips"><For each={bundle.env_keys}>{key => <span class="tag mono">{key}</span>}</For></span></td>
              <td><Show when={granted.length} fallback={<span class="subtle">No agents</span>}><span class="chips"><For each={granted.slice(0, 3)}>{name => <a class="tag link" href={agentHref(name)}>{name}</a>}</For>
                <Show when={granted.length > 3}><span class="subtle num" title={granted.join(', ')}>+{granted.length - 3}</span></Show></span></Show></td>
              <td class="num subtle">{ago(bundle.updated_at_ms)}</td>
              <td class="right nowrap"><button type="button" class="btn sm ghost" onClick={() => setEdit(bundle)}>Replace</button>
                <button type="button" class="btn sm ghost" onClick={() => setRemove(bundle)}>Delete</button></td>
            </tr>;
          }}</For></tbody></table></div>
      </Show>}</Show>
    </Section>
    <Section title="Agent grants">
      <div class="inline-form">
        <select class="input" onChange={event => setTarget(event.currentTarget.value)} aria-label="Agent">
          <option value="" selected={!target()}>Choose an agent…</option><For each={fleet().live}>{agent => <option value={agent.name} selected={agent.name === target()}>{agent.name}</option>}</For></select>
        <button type="button" class="btn" disabled={!target()} onClick={() => { const agent = fleet().live.find(candidate => candidate.name === target()); if (agent) act({ kind: 'grants', agent }); }}>Manage grants</button>
      </div>
    </Section>
    <Show when={edit()} keyed>{bundle => <FormDialog title={bundle === 'new' ? 'Seal a bundle' : 'Replace ' + bundle.name} description="Values are encrypted on the coordinator. Replacing a bundle overwrites every key."
      action="Seal bundle" close={close} submit={async (data, node) => {
        const name = bundle === 'new' ? formText(data, 'name') : bundle.name;
        const raw = String(data.get('env') ?? '');
        const input = node.elements.namedItem('env');
        if (input instanceof HTMLTextAreaElement) input.value = '';
        const env = Object.create(null) as Record<string, string>;
        for (const line of raw.split(/\r?\n/)) {
          if (!line.trim() || line.trimStart().startsWith('#')) continue;
          const equals = line.indexOf('=');
          const key = line.slice(0, equals).trim();
          if (equals < 1 || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) throw new Error('Use one KEY=VALUE pair per line.');
          env[key] = line.slice(equals + 1);
        }
        if (!Object.keys(env).length) throw new Error('Enter at least one KEY=VALUE pair.');
        await api('/v1/secrets/' + encodeURIComponent(name), { method: 'PUT', body: { env } }); secrets.refresh();
      }}>
      <Show when={bundle === 'new'}><Field label="Bundle name"><input class="input mono" required name="name" pattern="[a-zA-Z0-9][a-zA-Z0-9._\-]{0,63}" autofocus autocomplete="off" /></Field></Show>
      <Field label="Environment" hint="One KEY=VALUE per line."><textarea class="input mono" required name="env" rows={5} autocomplete="off" spellcheck={false} placeholder="SERVICE_TOKEN=…" /></Field>
    </FormDialog>}</Show>
    <Show when={remove()} keyed>{bundle => <FormDialog title={'Delete ' + bundle.name + '?'} description="This removes the encrypted bundle and every grant to it. This cannot be undone."
      action="Delete bundle" danger close={close} submit={async () => { await api('/v1/secrets/' + encodeURIComponent(bundle.name), { method: 'DELETE' }); secrets.refresh(); }} />}</Show>
  </>;
}

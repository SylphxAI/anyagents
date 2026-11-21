import { For, Show, createMemo, createSignal, onCleanup } from 'solid-js';
import { ArrowUpRight, CircleCheck, KeyRound, LoaderCircle, LogIn } from 'lucide-solid';
import { api, errorMessage, request } from '../api';
import { safeLink } from '../format';
import { OAUTH_PROVIDERS, providerName } from '../lib/providers';
import { readSignInStream, type SignInEvent } from '../lib/signin-stream';
import { useConsole } from '../state';
import type { ConnectRequest } from '../state';
import { CopyButton, Dialog, Field, Problem, Tabs, formText } from '../ui/primitives';

const KEY_PRESETS = ['deepseek', 'glm-code-plan', 'google-gemini', 'kimi-code-plan', 'minimax-code-plan', 'openrouter', 'qwen-token-plan', 'sylphx'];
type Phase = 'choose' | 'running' | 'done' | 'failed';

export function ConnectDialog(props: { request: ConnectRequest; close: () => void }) {
  const initial = props.request;
  const oauthDefault = OAUTH_PROVIDERS.some(option => option.provider === initial.provider);
  const [tab, setTab] = createSignal<'oauth' | 'key'>(initial.tab ?? (initial.provider && !oauthDefault ? 'key' : 'oauth'));
  const [busy, setBusy] = createSignal(false);
  return <Dialog title="Connect account" wide busy={busy()} close={props.close}
    description="Add a provider subscription or an API key. Agents pick it up automatically.">
    <div class="dialog-tabs"><Tabs label="Connection type" value={tab()} onChange={value => { if (!busy()) setTab(value); }} tabs={[
      { id: 'oauth', label: 'Sign in with provider' }, { id: 'key', label: 'API key' }]} /></div>
    <Show when={tab() === 'oauth'} fallback={<KeyForm initial={initial} close={props.close} />}>
      <OAuthFlow initial={initial} close={props.close} setBusy={setBusy} />
    </Show>
  </Dialog>;
}

function OAuthFlow(props: { initial: ConnectRequest; close: () => void; setBusy: (busy: boolean) => void }) {
  const { refreshProfiles } = useConsole();
  const [provider, setProvider] = createSignal<string>(OAUTH_PROVIDERS.find(option => option.provider === props.initial.provider)?.provider ?? OAUTH_PROVIDERS[0].provider);
  const [phase, setPhaseRaw] = createSignal<Phase>('choose');
  const [events, setEvents] = createSignal<SignInEvent[]>([]);
  const [session, setSession] = createSignal('');
  const [error, setError] = createSignal('');
  const [label, setLabel] = createSignal('');
  const [inputWanted, setInputWanted] = createSignal(false);
  const [paste, setPaste] = createSignal('');
  const [pasteState, setPasteState] = createSignal<{ busy?: boolean; ok?: boolean; error?: string }>({});
  let controller: AbortController | null = null;
  const setPhase = (next: Phase) => { setPhaseRaw(next); props.setBusy(next === 'running'); };
  const hint = () => props.initial.provider === provider() ? props.initial.hint : undefined;
  const option = () => OAUTH_PROVIDERS.find(candidate => candidate.provider === provider())!;
  onCleanup(() => controller?.abort());

  async function start() {
    if (controller) return;
    const run = new AbortController(); controller = run;
    setEvents([]); setError(''); setSession(''); setInputWanted(false); setPaste(''); setPasteState({}); setPhase('running');
    try {
      const login = hint();
      const response = await request('/v1/profiles/native-signin', { method: 'POST', signal: run.signal, accept: 'application/x-ndjson',
        body: { harness: option().harness, ...(login ? { login_hint: login } : {}) } });
      const connected = await readSignInStream(response.body, event => {
        if (run.signal.aborted) return;
        if (event.kind === 'session') { setSession(event.text ?? ''); return; }
        if (event.kind === 'input') { setInputWanted(true); return; }
        setEvents(previous => [...previous, event].slice(-60));
      });
      if (!run.signal.aborted) { setLabel(connected); setPhase('done'); refreshProfiles(true); }
    } catch (cause) {
      if (!run.signal.aborted) { setError(errorMessage(cause)); setPhase('failed'); }
    } finally { if (controller === run) controller = null; }
  }

  function cancel() { controller?.abort(); controller = null; setPhase('choose'); }

  async function submitPaste(event: SubmitEvent) {
    event.preventDefault();
    const value = paste().trim();
    if (!value || !session() || pasteState().busy) return;
    setPasteState({ busy: true });
    try {
      await api('/v1/profiles/native-signin/' + encodeURIComponent(session()) + '/input', { method: 'POST', body: { value } });
      setPasteState({ ok: true }); setPaste('');
    } catch (cause) { setPasteState({ error: errorMessage(cause) }); }
  }

  const link = createMemo(() => events().filter(event => event.kind === 'link').map(event => safeLink(event.text)).filter(Boolean).pop());
  const code = createMemo(() => [...events()].reverse().find(event => event.kind === 'code')?.text);
  const prompts = createMemo(() => events().filter(event => event.kind === 'prompt' && event.text));

  return <Show when={phase() !== 'done'} fallback={<div class="dialog-body">
    <div class="connect-done"><CircleCheck size={22} aria-hidden="true" /><div>
      <div class="strong">{option().name} account connected</div>
      <p class="subtle"><Show when={label()}>Saved as <span class="mono">{label()}</span>. </Show>AnyAgents matched the account your provider returned and refreshed or added its slot automatically.</p>
    </div></div>
    <footer class="dialog-foot flush"><button type="button" class="btn primary" onClick={() => props.close()}>Done</button></footer>
  </div>}>
    <div class="dialog-body">
      <Show when={phase() === 'choose'}>
        <fieldset class="provider-options">
          <legend class="field-label">Provider</legend>
          <For each={OAUTH_PROVIDERS}>{candidate => <label class={'provider-option' + (candidate.provider === provider() ? ' selected' : '')}>
            <input type="radio" name="provider" value={candidate.provider} checked={candidate.provider === provider()} onChange={() => setProvider(candidate.provider)} />
            <span class="provider-option-text"><span class="strong">{candidate.name}</span><span class="subtle">{candidate.detail}</span></span>
          </label>}</For>
        </fieldset>
        <p class="connect-note">Sign in with your provider — AnyAgents automatically detects your account email or ID, then refreshes the matching slot or adds a new one. There is nothing to name.</p>
        <Show when={hint()}><p class="connect-hint">Signing in again as <span class="mono strong">{hint()}</span></p></Show>
      </Show>
      <Show when={phase() !== 'choose'}>
        <div class="connect-steps" aria-live="polite">
          <div class="connect-provider"><span class="strong">{option().name}</span><Show when={hint()}><span class="subtle mono">{hint()}</span></Show>
            <Show when={phase() === 'running'}><span class="connect-status"><LoaderCircle size={14} class="spin" aria-hidden="true" />Waiting for the provider</span></Show></div>
          <ol class="steps">
            <li class={link() ? 'done' : 'current'}>
              <div class="step-title">Open the sign-in page</div>
              <Show when={link()} fallback={<div class="subtle">{phase() === 'running' ? 'Preparing an isolated sign-in…' : 'The provider did not return a sign-in link.'}</div>}>
                <div class="link-box">
                  <a class="btn primary sm" href={link()} target="_blank" rel="noopener noreferrer"><ArrowUpRight size={13} />Open sign-in page</a>
                  <CopyButton value={link()!} label="Copy link" text />
                  <span class="link-url mono truncate" title={link()}>{link()}</span>
                </div>
              </Show>
            </li>
            <Show when={code()}><li class="current">
              <div class="step-title">Enter this device code</div>
              <div class="device-code"><span class="mono">{code()}</span><CopyButton value={code()!} label="Copy code" text /></div>
            </li></Show>
            <Show when={phase() === 'running' && inputWanted()}><li class={pasteState().ok ? 'done' : 'current'}>
              <div class="step-title">Paste the callback URL or authorization code</div>
              <p class="subtle small">After approving access, copy the address your browser lands on (or the code shown) and paste it here.</p>
              <form class="paste-row" onSubmit={submitPaste}>
                <input class="input mono" value={paste()} onInput={event => setPaste(event.currentTarget.value)} placeholder="http://localhost:…/callback?code=… or code"
                  aria-label="Callback URL or authorization code" autocomplete="off" spellcheck={false} disabled={pasteState().busy || pasteState().ok} />
                <button class="btn primary" disabled={!paste().trim() || pasteState().busy || pasteState().ok}>{pasteState().busy ? 'Sending…' : 'Submit'}</button>
              </form>
              <Show when={pasteState().ok}><p class="tone-ok small">Received. Finishing sign-in…</p></Show>
              <Show when={pasteState().error}><Problem>{pasteState().error}</Problem></Show>
            </li></Show>
          </ol>
          <Show when={prompts().length}><details class="connect-log"><summary>Sign-in output <span class="num subtle">({prompts().length})</span></summary>
            <div class="log mono"><For each={prompts()}>{event => <div>{event.text}</div>}</For></div></details></Show>
        </div>
      </Show>
      <Show when={error()}><Problem>{error()}</Problem></Show>
      <footer class="dialog-foot flush">
        <Show when={phase() === 'running'} fallback={<button type="button" class="btn" onClick={() => props.close()}>Cancel</button>}>
          <button type="button" class="btn" onClick={cancel}>Cancel sign-in</button>
        </Show>
        <Show when={phase() !== 'running'}><button type="button" class="btn primary" onClick={() => void start()}><LogIn size={14} />{phase() === 'failed' ? 'Try again' : 'Continue with ' + option().name}</button></Show>
      </footer>
    </div>
  </Show>;
}

function KeyForm(props: { initial: ConnectRequest; close: () => void }) {
  const { refreshProfiles, accounts } = useConsole();
  const initial = props.initial;
  const known = accounts().filter(account => !account.oauth).map(account => account.profile.provider);
  const providers = [...new Set([...KEY_PRESETS, ...known, ...(initial.provider && !OAUTH_PROVIDERS.some(option => option.provider === initial.provider) ? [initial.provider] : [])])]
    .sort((a, b) => providerName(a).localeCompare(providerName(b)));
  const [provider, setProvider] = createSignal(initial.tab === 'key' && initial.provider ? initial.provider : providers[0]);
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal('');
  async function save(event: SubmitEvent & { currentTarget: HTMLFormElement }) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const chosen = provider() === 'custom' ? formText(form, 'custom') : provider();
    const label = formText(form, 'label'), key = formText(form, 'key');
    if (!chosen || !label || !key) { setError('Choose a provider and enter a label and key.'); return; }
    const input = event.currentTarget.elements.namedItem('key');
    if (input instanceof HTMLInputElement) input.value = '';
    setBusy(true); setError('');
    try { await api('/v1/profiles', { method: 'POST', body: { profile: chosen + '/' + label, key } }); refreshProfiles(true); props.close(); }
    catch (cause) { setError(errorMessage(cause)); } finally { setBusy(false); }
  }
  return <form class="dialog-form" onSubmit={save}>
    <fieldset class="dialog-body" disabled={busy()}>
      <div class="grid-2">
        <Field label="Provider"><select class="input" onChange={event => setProvider(event.currentTarget.value)}>
          <For each={providers}>{item => <option value={item} selected={item === provider()}>{providerName(item)}</option>}</For>
          <option value="custom" selected={provider() === 'custom'}>Other provider…</option></select></Field>
        <Show when={provider() === 'custom'} fallback={<Field label="Label" hint="A name you will recognize, e.g. team-key.">
          <input class="input" name="label" required value={initial.tab === 'key' ? initial.hint ?? '' : ''} placeholder="team-key" autocomplete="off" /></Field>}>
          <Field label="Provider ID"><input class="input mono" name="custom" required placeholder="provider-id" /></Field>
        </Show>
      </div>
      <Show when={provider() === 'custom'}><Field label="Label"><input class="input" name="label" required placeholder="team-key" autocomplete="off" /></Field></Show>
      <Field label="API key" hint="Stored encrypted on the coordinator. It is never shown again."><input class="input mono" name="key" type="password" required autocomplete="off" spellcheck={false} /></Field>
      <p class="connect-note"><KeyRound size={13} aria-hidden="true" /> Saving a label that already exists replaces its key.</p>
    </fieldset>
    <Show when={error()}><div class="dialog-error"><Problem>{error()}</Problem></div></Show>
    <footer class="dialog-foot"><button type="button" class="btn" onClick={() => props.close()} disabled={busy()}>Cancel</button>
      <button class="btn primary" disabled={busy()}>{busy() ? 'Saving…' : 'Save key'}</button></footer>
  </form>;
}

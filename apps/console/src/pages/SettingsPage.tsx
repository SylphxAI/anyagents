import { Match, Switch } from 'solid-js';
import type { SettingsTab } from '../types';
import { PageHeader, Tabs } from '../ui/primitives';
import { ChannelsSettings } from '../settings/ChannelsSettings';
import { SecretsSettings } from '../settings/SecretsSettings';
import { AutomationsSettings } from '../settings/AutomationsSettings';
import { UsageSettings } from '../settings/UsageSettings';

const DESCRIPTIONS: Record<SettingsTab, string> = {
  channels: 'Where agents talk to people: channel bindings and multi-agent groups.',
  secrets: 'Encrypted deployment bundles and which agents may read them.',
  automations: 'Schedules, held outbound deliveries, and coordinator maintenance.',
  usage: 'Turns, tokens, and reported cost by agent and model.',
};

export function SettingsPage(props: { tab: SettingsTab }) {
  return <div class="page">
    <PageHeader title="Settings" description={DESCRIPTIONS[props.tab]} />
    <Tabs label="Settings sections" value={props.tab} onChange={next => { window.location.hash = '#settings/' + next; }} tabs={[
      { id: 'channels', label: 'Channels' }, { id: 'secrets', label: 'Secrets' }, { id: 'automations', label: 'Automations' }, { id: 'usage', label: 'Usage' }]} />
    <div class="tab-panel">
      <Switch>
        <Match when={props.tab === 'channels'}><ChannelsSettings /></Match>
        <Match when={props.tab === 'secrets'}><SecretsSettings /></Match>
        <Match when={props.tab === 'automations'}><AutomationsSettings /></Match>
        <Match when={props.tab === 'usage'}><UsageSettings /></Match>
      </Switch>
    </div>
  </div>;
}

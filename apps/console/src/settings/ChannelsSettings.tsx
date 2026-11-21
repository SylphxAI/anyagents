import { For, Show } from 'solid-js';
import { MessageCircle, Users } from 'lucide-solid';
import { api, createPoll } from '../api';
import { plural } from '../format';
import { agentHref } from '../route';
import { Dot, Empty, Pill, Problem, Section, Skeleton } from '../ui/primitives';
import type { Tone } from '../ui/primitives';

interface Channel { id: string; kind: string; bot: string | null; agents: string[] }
interface GroupMember { id: string; name: string; harness: string; state: string }
interface Group { name: string; public: boolean; members: string[]; roster?: GroupMember[] }

const memberTone = (state: string): Tone => ({ working: 'ok', running: 'ok', paused: 'warn', faulted: 'bad', errored: 'bad', archived: 'muted' } as Record<string, Tone>)[state] ?? 'neutral';

export function ChannelsSettings() {
  const channels = createPoll(signal => api<{ channels: Channel[] }>('/v1/channels', { signal }), { interval: 30000 });
  const groups = createPoll(signal => api<{ groups: Group[] }>('/v1/groups', { signal }), { interval: 30000 });
  return <>
    <Section title="Channel bindings" meta={<Show when={channels.data()}><span class="num subtle">{channels.data()!.channels.length}</span></Show>}>
      <Show when={channels.error()}><Problem retry={channels.refresh}>{channels.error()}</Problem></Show>
      <Show when={channels.loading()}><Skeleton rows={3} /></Show>
      <Show when={channels.data()}>{data => <Show when={data().channels.length}
        fallback={<Empty compact icon={<MessageCircle size={18} />} title="No channels bound">Bind a chat channel with <code>anyagents channel add</code> to let agents talk to people.</Empty>}>
        <div class="table-wrap"><table class="table">
          <thead><tr><th>Channel</th><th>Kind</th><th>Bot</th><th>Bound agents</th></tr></thead>
          <tbody><For each={data().channels}>{channel => <tr>
            <td class="mono strong">{channel.id}</td>
            <td><span class="tag">{channel.kind || 'channel'}</span></td>
            <td class="mono subtle">{channel.bot || '—'}</td>
            <td><Show when={channel.agents.length} fallback={<span class="subtle">None</span>}><span class="chips"><For each={channel.agents}>{name => <a class="tag link" href={agentHref(name)}>{name}</a>}</For></span></Show></td>
          </tr>}</For></tbody></table></div>
      </Show>}</Show>
    </Section>
    <Section title="Groups" meta={<Show when={groups.data()}><span class="num subtle">{groups.data()!.groups.length}</span></Show>}>
      <Show when={groups.error()}><Problem retry={groups.refresh}>{groups.error()}</Problem></Show>
      <Show when={groups.loading()}><Skeleton rows={3} /></Show>
      <Show when={groups.data()}>{data => <Show when={data().groups.length}
        fallback={<Empty compact icon={<Users size={18} />} title="No groups">Groups let several agents share one conversation.</Empty>}>
        <div class="table-wrap"><table class="table">
          <thead><tr><th>Group</th><th>Visibility</th><th class="right">Members</th><th>Roster</th></tr></thead>
          <tbody><For each={data().groups}>{group => <tr>
            <td class="mono strong">{group.name}</td>
            <td><Pill tone={group.public ? 'info' : 'neutral'}>{group.public ? 'Public' : 'Private'}</Pill></td>
            <td class="right num">{group.members.length}</td>
            <td class="cell-wrap"><Show when={group.roster?.length} fallback={<span class="subtle mono">{group.members.join(', ') || 'Empty'}</span>}>
              <span class="chips"><For each={group.roster}>{member =>
                <a class="tag link" href={agentHref(member.name)} title={member.harness + ' · ' + member.state}><Dot tone={memberTone(member.state)} />{member.name}</a>}</For></span></Show></td>
          </tr>}</For></tbody></table></div>
        <p class="section-foot subtle">{plural(data().groups.reduce((sum, group) => sum + group.members.length, 0), 'membership')} across {plural(data().groups.length, 'group')}.</p>
      </Show>}</Show>
    </Section>
  </>;
}

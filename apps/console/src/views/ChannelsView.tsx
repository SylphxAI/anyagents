import { api, useResource } from '../api';
import { Badge, Empty, Heading, Panel, Problem, Refresh, Table } from '../ui';

interface Channel {
  id: string;
  kind: string;
  bot: string | null;
  agents: string[];
}
interface GroupMember {
  id: string;
  name: string;
  harness: string;
  state: string;
}
interface Group {
  name: string;
  public: boolean;
  members: string[];
  roster?: GroupMember[];
}

const loadChannels = (signal: AbortSignal) => api<{ channels: Channel[] }>('/v1/channels', { signal });
const loadGroups = (signal: AbortSignal) => api<{ groups: Group[] }>('/v1/groups', { signal });

function agentLink(name: string) {
  return '#fleet/' + encodeURIComponent(name);
}

function memberTone(state: string) {
  switch (state) {
    case 'working': case 'running': return 'success';
    case 'idle': return 'sky';
    case 'paused': return 'warning';
    case 'faulted': case 'errored': return 'danger';
    case 'archived': return 'muted';
    default: return 'accent';
  }
}

export function ChannelsView() {
  const channels = useResource(loadChannels);
  const groups = useResource(loadGroups);
  const channelList = channels.data?.channels ?? [];
  const groupList = groups.data?.groups ?? [];

  return <div className="flow">
    <Heading eyebrow="Messaging" title="Channels & Groups" blurb="Inspect channel bindings and multi-agent group rosters.">
      <Refresh onClick={() => { channels.refresh(); groups.refresh(); }} />
    </Heading>
    <div className="two-cols">
      <Panel title="Channel Bindings" actions={channels.data && <Badge>{channelList.length} channel{channelList.length === 1 ? '' : 's'}</Badge>}>
        {channels.error && <div className="panel-body"><Problem retry={channels.refresh}>Could not load channels: {channels.error}</Problem></div>}
        {!channels.data && !channels.error && <p className="panel-body muted" role="status">Loading channels…</p>}
        {channels.data && <Table headers={['ID', 'Kind', 'Bot', 'Bound Agents']} empty="No channel bindings configured."
          rows={channelList.map(channel => ({ key: channel.id, cells: [
            <span className="mono">{channel.id}</span>,
            <Badge tone="sky">{channel.kind || 'channel'}</Badge>,
            <span className="mono">{channel.bot || '—'}</span>,
            channel.agents.length ? <div className="actions">{channel.agents.map(name =>
              <a key={name} href={agentLink(name)} className="badge muted">{name}</a>)}</div> : '—',
          ] }))} />}
      </Panel>
      <Panel title="Multi-Agent Groups" actions={groups.data && <Badge>{groupList.length} group{groupList.length === 1 ? '' : 's'}</Badge>}>
        {groups.error && <div className="panel-body"><Problem retry={groups.refresh}>Could not load groups: {groups.error}</Problem></div>}
        {!groups.data && !groups.error && <p className="panel-body muted" role="status">Loading groups…</p>}
        {groups.data && (groupList.length ? <div className="panel-body stack">{groupList.map(group =>
          <section key={group.name} className="group-card stack">
            <div className="actions">
              <h3 className="mono">{group.name}</h3>
              <Badge tone={group.public ? 'success' : 'muted'}>{group.public ? 'public' : 'private'}</Badge>
              <Badge tone="sky">{group.members.length} member{group.members.length === 1 ? '' : 's'}</Badge>
            </div>
            {group.roster?.length ? <div className="actions">{group.roster.map(member =>
              <a key={member.id} href={agentLink(member.name)} className={'badge ' + memberTone(member.state)}
                aria-label={member.name + ' (' + member.harness + ', ' + member.state + ')'}>
                {member.name} ({member.harness})
              </a>)}</div> : <p className="mono muted">{group.members.join(', ') || 'Empty group'}</p>}
          </section>)}</div> : <Empty>No multi-agent groups configured.</Empty>)}
      </Panel>
    </div>
  </div>;
}

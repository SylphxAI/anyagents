export type BlockKind = 'sender' | 'chat';
export interface BlockTarget {
  channel: string;
  scope: { kind: 'sender'; sender_id: string } | { kind: 'chat'; chat_id: string };
}
export interface BlockRule {
  rule_id: string;
  target: BlockTarget;
  reason?: string;
  created_at_ms: number;
  expires_at_ms?: number;
}
export type LeaveChat = { status: 'skipped'; reason: string } |
  { status: 'attempted'; left: number; already_absent: number; failed: number };
export interface BlockReceipt {
  result: 'blocked';
  rule: BlockRule;
  leave_chat?: LeaveChat;
  suggested_channel_action?: { action: 'leave_chat'; channel: string; chat_id: string };
}

export const ingressPath = (agentId: string) => '/admin/api/agents/' + encodeURIComponent(agentId) + '/ingress-blocks';
export const blockId = ({ scope }: BlockTarget) => scope.kind === 'sender' ? scope.sender_id : scope.chat_id;
export const channelLabel = (channel: string) => channel === 'telegram' ? 'Telegram' : channel;

export function activeBlocks(rules: BlockRule[], now: number) {
  return rules.filter(rule => rule.expires_at_ms == null || rule.expires_at_ms > now)
    .sort((a, b) => b.created_at_ms - a.created_at_ms || a.rule_id.localeCompare(b.rule_id));
}

export function filterBlocks(rules: BlockRule[], query: string, kind: BlockKind | 'all') {
  const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return rules.filter(({ target }) => {
    const searchable = [target.channel.toLowerCase(), target.scope.kind, blockId(target).toLowerCase()];
    return (kind === 'all' || kind === target.scope.kind) && words.every(word => searchable.some(value => value.includes(word)));
  });
}

export function blockRequest(kind: BlockKind, id: string, reason: string, expires: string, now = Date.now()) {
  // Match canonical, nonzero signed 64-bit provider IDs without rounding them through Number.
  id = id.trim();
  const label = kind === 'sender' ? 'Sender' : 'Chat';
  const pattern = kind === 'sender' ? /^[1-9][0-9]{0,18}$/ : /^-?[1-9][0-9]{0,18}$/;
  if (!pattern.test(id) || BigInt(id) < -(1n << 63n) || BigInt(id) >= (1n << 63n)) {
    throw new Error(label + ' ID must be ' + (kind === 'sender' ? 'a positive' : 'a nonzero') + ' Telegram integer without leading zeros.');
  }
  if (reason.includes('\0')) throw new Error('Reason must not contain NUL characters.');
  const expiryMs = expires ? Date.parse(expires) : undefined;
  if (expiryMs !== undefined && (!Number.isFinite(expiryMs) || expiryMs <= now)) {
    throw new Error('Choose a future expiry, or leave it blank for a permanent block.');
  }
  const target: BlockTarget = { channel: 'telegram', scope: kind === 'sender' ? { kind, sender_id: id } : { kind, chat_id: id } };
  return { op: 'block_sender' as const, target, reason: reason.trim() || undefined, expires_at_ms: expiryMs };
}

export function leaveChatNote(receipt: BlockReceipt): string | undefined {
  const leave = receipt.leave_chat;
  if (leave?.status === 'skipped') return 'Telegram leaveChat skipped: ' + leave.reason + '.';
  if (leave?.status === 'attempted') {
    return 'Telegram leaveChat: ' + leave.left + ' left, ' + leave.already_absent + ' already absent, ' + leave.failed + ' failed.';
  }
  if (receipt.suggested_channel_action?.action === 'leave_chat') {
    return 'Telegram leaveChat was suggested; no outcome was reported.';
  }
}

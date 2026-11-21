import assert from 'node:assert/strict';
import test from 'node:test';
import { activeBlocks, blockRequest, filterBlocks, ingressPath, leaveChatNote } from './ingress.ts';
import type { BlockReceipt, BlockRule } from './ingress.ts';

const sender: BlockRule = { rule_id: '1', target: { channel: 'telegram', scope: { kind: 'sender', sender_id: '101' } }, created_at_ms: 1 };
const chat: BlockRule = { rule_id: '2', target: { channel: 'telegram', scope: { kind: 'chat', chat_id: '-101' } }, created_at_ms: 2, expires_at_ms: 500 };

test('selects unexpired rules in newest-first order; expiry is exclusive', () => {
  assert.deepEqual(activeBlocks([sender, chat], 499), [chat, sender]);
  assert.deepEqual(activeBlocks([sender, chat], 500), [sender]);
});

test('matches provider, kind and ID terms in combination, with an independent kind filter', () => {
  assert.deepEqual(filterBlocks([sender, chat], '  Telegram CHAT -101 ', 'all'), [chat]);
  assert.deepEqual(filterBlocks([sender, chat], '101', 'sender'), [sender]);
  assert.deepEqual(filterBlocks([sender, chat], 'other', 'all'), []);
  assert.equal(ingressPath('one/two'), '/admin/api/agents/one%2Ftwo/ingress-blocks');
});

test('validates canonical IDs as strings and submits expiry as milliseconds', () => {
  assert.deepEqual(blockRequest('chat', ' -9223372036854775808 ', ' test ', '2099-01-01T00:00Z', 100), {
    op: 'block_sender', target: { channel: 'telegram', scope: { kind: 'chat', chat_id: '-9223372036854775808' } }, reason: 'test', expires_at_ms: 4070908800000,
  });
  assert.equal(JSON.stringify(blockRequest('sender', '9223372036854775807', '', '')), '{"op":"block_sender","target":{"channel":"telegram","scope":{"kind":"sender","sender_id":"9223372036854775807"}}}');
  for (const id of ['@handle', '0002', '0', '-1', '+1', '9223372036854775808']) {
    assert.throws(() => blockRequest('sender', id, '', ''), /Sender ID must be/);
  }
  assert.throws(() => blockRequest('chat', '-9223372036854775809', '', ''), /Chat ID must be/);
  assert.throws(() => blockRequest('chat', '1', 'bad\0reason', ''), /must not contain NUL/);
  assert.throws(() => blockRequest('chat', '1', '', '1970-01-01T00:00:00Z', 0), /future expiry/);
});

test('distinguishes no provider effect, skipped action, and a partial leave failure', () => {
  const receipt: BlockReceipt = { result: 'blocked', rule: chat };
  assert.equal(leaveChatNote(receipt), undefined);
  assert.equal(leaveChatNote({ ...receipt, suggested_channel_action: { action: 'leave_chat', channel: 'telegram', chat_id: '-101' } }), 'Telegram leaveChat was suggested; no outcome was reported.');
  assert.equal(leaveChatNote({ ...receipt, leave_chat: { status: 'skipped', reason: 'temporary block' } }), 'Telegram leaveChat skipped: temporary block.');
  assert.equal(leaveChatNote({ ...receipt, leave_chat: { status: 'attempted', left: 1, already_absent: 2, failed: 1 } }), 'Telegram leaveChat: 1 left, 2 already absent, 1 failed.');
});

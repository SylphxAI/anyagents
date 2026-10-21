-- What a channel adapter remembers between runs: the next Telegram update to
-- ask for, the bot it last confirmed, the chats each attached agent answers
-- in, and the chat titles. No secret is stored here.
ALTER TABLE channels ADD COLUMN state TEXT NOT NULL DEFAULT '{}';

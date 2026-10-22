-- The trusted header a harness shows its model with each message: the
-- authenticated author, the channel, the chat and forum topic, the reply a bare
-- answer names, and the destination that answer goes to. Written by the server
-- from the record it received; a body never supplies it.
ALTER TABLE messages ADD COLUMN context TEXT;

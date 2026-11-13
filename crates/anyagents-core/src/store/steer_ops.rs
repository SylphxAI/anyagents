//! The mailbox read a pipelined steer needs: the line right behind one the
//! harness holds but has not yet proved consumed. The cursor itself still
//! moves only through `note_delivered`, one contiguous sequence at a time.
use super::{Message, Store, agent_exists, cursor, last_seq, message_at_on};
use crate::error::Error;
use crate::id::AgentId;

impl Store {
    /// The message one past `seq`, when it is past the delivery cursor: what a
    /// pass hands over behind a line the harness has in flight. `None` when
    /// the mailbox holds nothing after it.
    pub fn undelivered_after(
        &self,
        agent_id: &AgentId,
        seq: i64,
    ) -> Result<Option<Message>, Error> {
        let conn = self.read_conn();
        // One snapshot, as `next_undelivered`: a line committed between the
        // reads must not look like a hole.
        let snapshot = conn.unchecked_transaction()?;
        agent_exists(&snapshot, agent_id)?;
        let delivered = cursor(&snapshot, agent_id)?;
        let next = seq.max(delivered) + 1;
        if let Some(message) = message_at_on(&snapshot, agent_id, next, delivered)? {
            return Ok(Some(message));
        }
        if last_seq(&snapshot, agent_id)? >= next {
            return Err(Error::Message(format!(
                "mailbox sequence has a hole at {next}"
            )));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Incoming, NewAgent, ReceiveOutcome, Store};

    #[test]
    fn the_line_behind_one_in_flight_is_read_without_moving_the_cursor() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "spiron"))
            .unwrap();
        let seqs: Vec<i64> = ["one", "two", "three"]
            .into_iter()
            .map(
                |text| match store.receive(&agent.id, Incoming::new(text)).unwrap() {
                    ReceiveOutcome::New(message) => message.seq,
                    ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
                },
            )
            .collect();
        let behind = store
            .undelivered_after(&agent.id, seqs[0])
            .unwrap()
            .unwrap();
        assert_eq!(behind.seq, seqs[1]);
        assert_eq!(behind.text, "two");
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 0);
        assert!(
            store
                .undelivered_after(&agent.id, seqs[2])
                .unwrap()
                .is_none()
        );
        // A sequence already behind the cursor reads from the cursor on.
        store.note_delivered(&agent.id, seqs[0]).unwrap();
        store.note_delivered(&agent.id, seqs[1]).unwrap();
        let next = store.undelivered_after(&agent.id, 0).unwrap().unwrap();
        assert_eq!(next.seq, seqs[2]);
    }
}

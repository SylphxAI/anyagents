//! Render an authorized, bounded prompt section from the current store state.
use std::fmt::Write as _;

use super::{Actor, Error, Store};
use crate::AgentId;

const OPEN: &str = "<org_instructions>\n";
const CLOSE: &str = "</org_instructions>";
const MAX_BYTES: usize = 4096;

impl Store {
    /// Read for the actual recipient immediately before wake. Keep the result
    /// on the prompt path; never store it back in messages or workspace files.
    pub fn knowledge_wake_context(&self, recipient: &AgentId) -> Result<Option<String>, Error> {
        let docs = self.knowledge_instructions(&Actor::Agent(recipient.clone()))?;
        if docs.is_empty() {
            return Ok(None);
        }
        let mut out = String::from(OPEN);
        for doc in docs {
            let heading = format!(
                "<instruction topic=\"{}\" version=\"{}\">\n",
                doc.topic, doc.version
            );
            let end = "\n</instruction>\n";
            let remaining = MAX_BYTES - out.len() - CLOSE.len();
            if remaining <= heading.len() + end.len() {
                break;
            }
            out.push_str(&heading);
            let mut budget = remaining - heading.len() - end.len();
            for ch in doc.body.chars() {
                let escaped = match ch {
                    '&' => "&amp;",
                    '<' => "&lt;",
                    '>' => "&gt;",
                    _ => "",
                };
                let size = if escaped.is_empty() {
                    ch.len_utf8()
                } else {
                    escaped.len()
                };
                if size > budget {
                    break;
                }
                if escaped.is_empty() {
                    out.push(ch);
                } else {
                    out.push_str(escaped);
                }
                budget -= size;
            }
            out.push_str(end);
        }
        write!(out, "{CLOSE}").expect("writing to a string is infallible");
        Ok(Some(out))
    }
}

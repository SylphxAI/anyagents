//! A single HTTP owner, not one blocking thread/lock per input. Each mailbox
//! UUID has a durable request and an independently reconciled canonical receipt.
use super::*;
use std::collections::VecDeque;
use std::sync::mpsc;

const INITIAL_POLL: Duration = Duration::from_millis(250);
const MAX_POLL: Duration = Duration::from_secs(10);

pub(super) struct Input {
    pub(super) request: Pending,
    path: PathBuf,
    event: Value,
    proof: Option<Value>,
    echoed: bool,
    pub(super) failure: Option<String>,
    durable: bool,
    deadline: Instant,
    next_poll: Instant,
    interval: Duration,
}

impl Input {
    pub(super) fn prepare(
        session: &Session,
        config: &Path,
        path: PathBuf,
        key: Option<&str>,
        prompt: &str,
    ) -> Result<Self, String> {
        let body = Session::body(config, prompt)?;
        if let Some(key) = key {
            validate_id(key, "mailbox UUID")?;
        }
        let legacy = config.join("spiron-cloud-message.json");
        let mut imported = false;
        let mut request = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Pending>(&bytes)
                .map_err(|_| "invalid Spiron pending request")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if key.is_some()
                    && legacy
                        .try_exists()
                        .map_err(|_| "cannot inspect legacy Spiron state")?
                {
                    imported = true;
                    serde_json::from_slice::<Pending>(
                        &std::fs::read(&legacy).map_err(|_| "cannot read legacy Spiron state")?,
                    )
                    .map_err(|_| "invalid legacy Spiron pending request")?
                } else {
                    pending(&path, &session.scope, body.clone(), key)?
                }
            }
            Err(_) => return Err("cannot read Spiron pending request".into()),
        };
        let old_scope = format!("{}/{}", session.api.base, session.agent_id);
        let legacy_scope = (imported || path == legacy) && request.scope == old_scope;
        if request.scope != session.scope && !legacy_scope {
            return Err("Spiron pending request belongs to another API or Agent".into());
        }
        if request.body.get("content") != body.get("content") {
            return Err(
                "unfinished Spiron turn retained; mailbox UUID reused with different content"
                    .into(),
            );
        }
        if let Some(key) = key {
            // Legacy unknown outcomes keep their original key and exact body.
            // The new mailbox binding is durable before removing the old file.
            if !imported && request.key != key && request.mailbox_uuid.as_deref() != Some(key) {
                return Err("Spiron mailbox request key mismatch".into());
            }
            request.mailbox_uuid = Some(key.to_string());
        }
        if imported || legacy_scope {
            request.scope = session.scope.clone();
            save(
                &path,
                &serde_json::to_vec(&request).map_err(|e| e.to_string())?,
            )?;
        }
        // Recover a crash after saving the imported record but before unlinking.
        if key.is_some()
            && legacy
                .try_exists()
                .map_err(|_| "cannot inspect legacy Spiron state")?
        {
            let original: Pending = serde_json::from_slice(
                &std::fs::read(&legacy).map_err(|_| "cannot read legacy Spiron state")?,
            )
            .map_err(|_| "invalid legacy Spiron pending request")?;
            if original.key != request.key || original.body != request.body {
                return Err("unfinished legacy Spiron request retained; resume it first".into());
            }
            std::fs::remove_file(&legacy)
                .map_err(|_| "cannot finish legacy Spiron state migration")?;
        }
        let proof = request.input_delivery.clone();
        Ok(Self {
            request,
            path,
            event: Value::Null,
            proof,
            echoed: false,
            failure: None,
            durable: key.is_some(),
            deadline: Instant::now() + TURN_BUDGET,
            next_poll: Instant::now(),
            interval: INITIAL_POLL,
        })
    }

    pub(super) fn advance(
        &mut self,
        session: &Session,
        strict: bool,
    ) -> Result<Option<CloudTurnOutcome>, String> {
        let cached = self.request.completed.is_some();
        let receipt = if let Some(receipt) = &self.request.completed {
            receipt.clone()
        } else if let Some(task) = &self.request.receipt {
            session.api.get(&format!("/v1/message-receipts/{task}"))?
        } else {
            let receipt = session.api.post(
                &format!("/v1/agents/{}/messages", session.agent_id),
                &self.request,
            )?;
            if receipt.get("agent_id").and_then(Value::as_str) != Some(&session.agent_id) {
                return Err("Spiron receipt Agent mismatch; request retained".into());
            }
            self.request.receipt = Some(identifier(&receipt, "id")?);
            self.save()?;
            receipt
        };
        let task = self
            .request
            .receipt
            .as_deref()
            .ok_or("Spiron receipt missing")?;
        if identifier(&receipt, "id")? != task
            || receipt.get("agent_id").and_then(Value::as_str) != Some(&session.agent_id)
        {
            return Err("Spiron receipt identity mismatch; request retained".into());
        }
        if strict {
            if receipt.get("provider_message_id").and_then(Value::as_str) != Some(&self.request.key)
            {
                return Err("Spiron receipt request key mismatch; request retained".into());
            }
            if let Some(proof) = receipt.get("input_delivery").filter(|p| !p.is_null()) {
                for field in ["session_id", "entry_id", "turn_entry_id"] {
                    identifier(proof, field)?;
                }
                if proof
                    .get("recorded_at")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                {
                    return Err("Spiron input proof missing recorded_at".into());
                }
                if self
                    .proof
                    .as_ref()
                    .is_some_and(|previous| previous != proof)
                {
                    return Err("Spiron input proof changed; request retained".into());
                }
                if self.proof.is_none() {
                    self.proof = Some(proof.clone());
                    self.request.input_delivery = Some(proof.clone());
                    self.save()?;
                }
            } else if self.proof.is_some() {
                return Err("Spiron input proof disappeared; request retained".into());
            }
        }
        match receipt.get("status").and_then(Value::as_str) {
            Some("completed") => {
                if strict && self.proof.is_none() {
                    return Err(
                        "completed Spiron receipt lacks durable input proof; request retained"
                            .into(),
                    );
                }
                let reply = receipt
                    .get("reply")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty());
                let projection = if !strict || reply.is_none() {
                    conversation_reply(
                        &session.api,
                        &session.agent_id,
                        task,
                        &TurnCancel::default(),
                        self.deadline,
                    )
                } else {
                    Ok(None)
                };
                let reply = match reply {
                    Some(reply) => reply.to_string(),
                    None => projection?
                        .filter(|s| !s.is_empty())
                        .ok_or("completed Spiron receipt has no reply")?,
                };
                if self.durable {
                    self.request.completed = Some(receipt);
                    self.request
                        .body
                        .as_object_mut()
                        .unwrap()
                        .remove("mcp_servers");
                    if !cached {
                        self.save()?;
                    }
                } else {
                    std::fs::remove_file(&self.path).map_err(|e| e.to_string())?;
                }
                Ok(Some(CloudTurnOutcome {
                    session_id: session.agent_id.clone(),
                    reply_text: reply,
                    input_tokens: 0,
                    output_tokens: 0,
                }))
            }
            Some("failed" | "canceled") => {
                self.failure = Some(format!(
                    "Spiron turn {}",
                    receipt["status"].as_str().unwrap()
                ));
                if self.durable {
                    self.request.completed = Some(receipt);
                    self.request
                        .body
                        .as_object_mut()
                        .unwrap()
                        .remove("mcp_servers");
                    if !cached {
                        self.save()?;
                    }
                } else {
                    std::fs::remove_file(&self.path).map_err(|e| e.to_string())?;
                }
                Ok(None)
            }
            Some("submitted" | "working" | "input_required") => Ok(None),
            _ => Err("Spiron receipt has missing or unknown status; request retained".into()),
        }
    }

    fn save(&self) -> Result<(), String> {
        save(
            &self.path,
            &serde_json::to_vec(&self.request).map_err(|e| e.to_string())?,
        )
    }
}

fn declares_next_step(openapi: &Value) -> bool {
    let Some(cap) = openapi.get("x-spiron-input-delivery") else {
        return false;
    };
    cap.get("protocol").and_then(Value::as_str) == Some("session-entry-v1")
        && cap.get("level").and_then(Value::as_str) == Some("next-step")
        && cap.get("idempotent").and_then(Value::as_bool) == Some(true)
        && cap.get("proof").and_then(Value::as_str) == Some("input_delivery")
}

fn emit(writer: &mut impl Write, event: &Value) -> Result<(), String> {
    writeln!(writer, "{event}")
        .and_then(|()| writer.flush())
        .map_err(|e| e.to_string())
}

pub(crate) fn serve<R: BufRead + Send + 'static, W: Write + Send + 'static>(
    config: &Path,
    model: Option<&str>,
    resume: Option<&str>,
    reader: R,
    mut writer: W,
) -> Result<(), String> {
    let session = Session::open(config, model, resume)?;
    // A failed read is not an absent capability. Do not silently downgrade an
    // unreadable required contract, or advertise unverified stronger delivery.
    let strict = declares_next_step(&session.api.get("/v1/openapi.json")?);
    let delivery = if strict {
        json!({"level":"next-step", "idempotent":true,
            "proof":"session-entry-v1 input_delivery bound to API key, mailbox UUID and receipt",
            "peer":"Spiron Agent/message API; session-entry-v1"})
    } else {
        json!({"level":"turn-end", "idempotent":false,
            "proof":"terminal receipt only; peer lacks session-entry-v1", "peer":"Spiron Agent/message API"})
    };
    emit(
        &mut writer,
        &json!({"type":"system","subtype":"init", "session_id":session.agent_id,"delivery":delivery}),
    )?;
    let (tx, rx) = mpsc::sync_channel(64);
    std::thread::spawn(move || {
        for line in reader.lines() {
            if tx
                .send(Some(
                    line.map_err(|_| "cannot read cloud input".to_string()),
                ))
                .is_err()
            {
                return;
            }
        }
        let _ = tx.send(None);
    });
    let mut pending: VecDeque<Input> = VecDeque::new();
    let mut waiting: VecDeque<Value> = VecDeque::new();
    let mut eof = false;
    loop {
        // Drain available commands before waiting for another receipt. A busy
        // root never joins/blocks this input owner for the lifetime of its turn.
        loop {
            match rx.try_recv() {
                Ok(Some(line)) => {
                    let line = line?;
                    let Ok(event) = serde_json::from_str::<Value>(&line) else {
                        continue;
                    };
                    let kind = event.get("type").and_then(Value::as_str);
                    if kind == Some("user") {
                        waiting.push_back(event);
                    } else if kind == Some("cancel")
                        || (kind == Some("control_request")
                            && event
                                .pointer("/request/subtype")
                                .and_then(Value::as_str)
                                .is_some_and(|s| matches!(s, "cancel" | "interrupt")))
                    {
                        for input in &pending {
                            if let Some(task) = &input.request.receipt {
                                cancel_cloud_turn(
                                    &session.api.base,
                                    HARNESS,
                                    session.api.token.as_deref(),
                                    task,
                                )?;
                            }
                        }
                        return Err(
                            "Spiron cancellation requested; independent receipts retained".into(),
                        );
                    }
                }
                Ok(None) | Err(mpsc::TryRecvError::Disconnected) => {
                    eof = true;
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
            }
        }
        let mut added = 0;
        while (strict || pending.is_empty()) && !waiting.is_empty() {
            let event = waiting.pop_front().unwrap();
            let uuid = validate_id(
                event.get("uuid").and_then(Value::as_str).unwrap_or(""),
                "mailbox UUID",
            )?;
            let prompt = extract_user_prompt(&event);
            let directory = config.join("spiron-cloud-inputs");
            std::fs::create_dir_all(&directory).map_err(|_| "cannot create Spiron input state")?;
            let mut input = Input::prepare(
                &session,
                config,
                directory.join(format!("{uuid}.json")),
                Some(&uuid),
                &prompt,
            )?;
            if pending.iter().any(|p| p.request.key == uuid) {
                continue;
            }
            input.event = event;
            pending.push_back(input);
            added += 1;
        }
        // Newly arrived steering goes before old receipt polls.
        pending.rotate_right(added);
        let count = pending.len();
        for _ in 0..count {
            let mut input = pending.pop_front().unwrap();
            if Instant::now() >= input.deadline {
                return Err(
                    "Spiron reconciliation deadline reached; input receipts retained".into(),
                );
            }
            if Instant::now() < input.next_poll {
                pending.push_back(input);
                continue;
            }
            let outcome = input.advance(&session, strict)?;
            if !input.echoed && (input.proof.is_some() || (!strict && outcome.is_some())) {
                let mut echo = input.event.clone();
                echo["isReplay"] = json!(true);
                if let Some(proof) = &input.proof {
                    echo["input_delivery"] = proof.clone();
                }
                emit(&mut writer, &echo)?;
                input.echoed = true;
            }
            if let Some(reason) = input.failure.take() {
                let uuid = input.event["uuid"].clone();
                emit(
                    &mut writer,
                    &json!({"type":"result", "subtype":"error", "is_error":true,
                    "session_id":session.agent_id, "result":reason,"completed_message_uuids":[uuid]}),
                )?;
            } else if let Some(outcome) = outcome {
                let uuid = input.event["uuid"].clone();
                emit(
                    &mut writer,
                    &json!({"type":"assistant", "session_id":session.agent_id,
                    "message":{"role":"assistant","content":[{"type":"text","text":outcome.reply_text}]}}),
                )?;
                emit(
                    &mut writer,
                    &json!({"type":"result","subtype":"success","session_id":session.agent_id,
                    "result":outcome.reply_text,"user_message_uuids":[uuid],"completed_message_uuids":[uuid]}),
                )?;
            } else {
                input.next_poll = Instant::now() + input.interval;
                input.interval = (input.interval * 2).min(MAX_POLL);
                pending.push_back(input);
            }
        }
        if eof && pending.is_empty() && waiting.is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
mod tests;

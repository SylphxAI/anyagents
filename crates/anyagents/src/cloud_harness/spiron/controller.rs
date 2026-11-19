//! A single HTTP owner, not one blocking thread/lock per input. Each mailbox
//! UUID has a durable request and an independently reconciled canonical receipt.
use super::*;
use std::collections::VecDeque;
use std::sync::mpsc;

const INITIAL_POLL: Duration = Duration::from_millis(250);
const MAX_POLL: Duration = Duration::from_secs(5);
const MAX_TRANSIENT_POLL_ERRORS: u32 = 5;

pub(super) struct Input {
    pub(super) request: Pending,
    path: PathBuf,
    config_dir: PathBuf,
    event: Value,
    proof: Option<Value>,
    echoed: bool,
    pub(super) failure: Option<String>,
    pub(super) error_detail: Option<CloudErrorDetail>,
    durable: bool,
    deadline: Instant,
    next_poll: Instant,
    interval: Duration,
    transient_poll_errors: u32,
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
            config_dir: config.to_path_buf(),
            event: Value::Null,
            proof,
            echoed: false,
            failure: None,
            error_detail: None,
            durable: key.is_some(),
            deadline: Instant::now() + TURN_BUDGET,
            next_poll: Instant::now(),
            interval: INITIAL_POLL,
            transient_poll_errors: 0,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn advance(
        &mut self,
        session: &Session,
        strict: bool,
    ) -> Result<Option<CloudTurnOutcome>, String> {
        self.advance_detailed(session, strict, &TurnCancel::default())
            .map_err(|d| d.message)
    }

    pub(super) fn advance_detailed(
        &mut self,
        session: &Session,
        strict: bool,
        cancel: &TurnCancel,
    ) -> Result<Option<CloudTurnOutcome>, CloudErrorDetail> {
        let cached = self.request.completed.is_some();
        let receipt = if let Some(receipt) = &self.request.completed {
            receipt.clone()
        } else if let Some(task) = &self.request.receipt {
            match session
                .api
                .get_detailed(&format!("/v1/message-receipts/{task}"))
            {
                Ok(v) => {
                    self.transient_poll_errors = 0;
                    v
                }
                Err(d) if d.status.is_some_and(|s| (500..=599).contains(&s)) => {
                    self.transient_poll_errors += 1;
                    if self.transient_poll_errors <= MAX_TRANSIENT_POLL_ERRORS {
                        return Ok(None);
                    }
                    return Err(d);
                }
                Err(d) => return Err(d),
            }
        } else {
            match session.api.post_message_stream_or_json(
                &format!("/v1/agents/{}/messages", session.agent_id),
                &self.request,
                &session.agent_id,
                cancel,
            )? {
                PostMessageResponse::Stream(outcome) => {
                    clear_active_run(&self.config_dir);
                    if !self.durable {
                        let _ = std::fs::remove_file(&self.path);
                    }
                    return Ok(Some(outcome));
                }
                PostMessageResponse::Json(receipt) => {
                    if receipt.get("agent_id").and_then(Value::as_str) != Some(&session.agent_id) {
                        return Err(CloudErrorDetail::generic(
                            "Spiron receipt Agent mismatch; request retained",
                        ));
                    }
                    let receipt_id =
                        identifier(&receipt, "id").map_err(CloudErrorDetail::generic)?;
                    let run_id = receipt
                        .get("run_id")
                        .and_then(Value::as_str)
                        .and_then(|s| validate_id(s, "run_id").ok());
                    self.request.receipt = Some(receipt_id.clone());
                    self.request.run_id = run_id.clone();
                    self.save().map_err(CloudErrorDetail::generic)?;
                    let _ = persist_active_run(
                        &self.config_dir,
                        &ActiveRunState {
                            agent_id: session.agent_id.clone(),
                            run_id,
                            receipt_id: Some(receipt_id),
                            idempotency_key: Some(self.request.key.clone()),
                            content: self
                                .request
                                .body
                                .get("content")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            started_at_ms: now_epoch_ms(),
                        },
                    );
                    receipt
                }
            }
        };
        let task = self
            .request
            .receipt
            .as_deref()
            .ok_or_else(|| CloudErrorDetail::generic("Spiron receipt missing"))?;
        if identifier(&receipt, "id").map_err(CloudErrorDetail::generic)? != task
            || receipt.get("agent_id").and_then(Value::as_str) != Some(&session.agent_id)
        {
            return Err(CloudErrorDetail::generic(
                "Spiron receipt identity mismatch; request retained",
            ));
        }
        if strict {
            if receipt.get("provider_message_id").and_then(Value::as_str) != Some(&self.request.key)
            {
                return Err(CloudErrorDetail::generic(
                    "Spiron receipt request key mismatch; request retained",
                ));
            }
            if let Some(proof) = receipt.get("input_delivery").filter(|p| !p.is_null()) {
                for field in ["session_id", "entry_id", "turn_entry_id"] {
                    identifier(proof, field).map_err(CloudErrorDetail::generic)?;
                }
                if proof
                    .get("recorded_at")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                {
                    return Err(CloudErrorDetail::generic(
                        "Spiron input proof missing recorded_at",
                    ));
                }
                if self
                    .proof
                    .as_ref()
                    .is_some_and(|previous| previous != proof)
                {
                    return Err(CloudErrorDetail::generic(
                        "Spiron input proof changed; request retained",
                    ));
                }
                if self.proof.is_none() {
                    self.proof = Some(proof.clone());
                    self.request.input_delivery = Some(proof.clone());
                    self.save().map_err(CloudErrorDetail::generic)?;
                }
            } else if self.proof.is_some() {
                return Err(CloudErrorDetail::generic(
                    "Spiron input proof disappeared; request retained",
                ));
            }
        }
        match receipt.get("status").and_then(Value::as_str) {
            Some("completed") => {
                if strict && self.proof.is_none() {
                    return Err(CloudErrorDetail::generic(
                        "completed Spiron receipt lacks durable input proof; request retained",
                    ));
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
                    None => projection
                        .map_err(CloudErrorDetail::generic)?
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            CloudErrorDetail::generic("completed Spiron receipt has no reply")
                        })?,
                };
                if self.durable {
                    self.request.completed = Some(receipt);
                    self.request
                        .body
                        .as_object_mut()
                        .unwrap()
                        .remove("mcp_servers");
                    if !cached {
                        self.save().map_err(CloudErrorDetail::generic)?;
                    }
                } else {
                    std::fs::remove_file(&self.path)
                        .map_err(|e| CloudErrorDetail::generic(e.to_string()))?;
                }
                clear_active_run(&self.config_dir);
                Ok(Some(CloudTurnOutcome {
                    session_id: session.agent_id.clone(),
                    reply_text: reply,
                    input_tokens: 0,
                    output_tokens: 0,
                }))
            }
            Some("failed" | "canceled") => {
                let st = receipt["status"].as_str().unwrap_or("failed");
                let msg = format!("Spiron turn {st}");
                self.failure = Some(msg.clone());
                if let Some(err_obj) = receipt.get("error") {
                    let err_raw = err_obj.to_string();
                    if let Some(code_status) = err_obj
                        .get("status")
                        .or_else(|| err_obj.get("apiErrorStatus"))
                        .and_then(Value::as_u64)
                        .and_then(|n| u16::try_from(n).ok())
                    {
                        self.error_detail = Some(classify_http_error(
                            code_status,
                            None,
                            &err_raw,
                            HARNESS,
                            "spiron",
                        ));
                    }
                }
                if self.durable {
                    self.request.completed = Some(receipt);
                    self.request
                        .body
                        .as_object_mut()
                        .unwrap()
                        .remove("mcp_servers");
                    if !cached {
                        self.save().map_err(CloudErrorDetail::generic)?;
                    }
                } else {
                    std::fs::remove_file(&self.path)
                        .map_err(|e| CloudErrorDetail::generic(e.to_string()))?;
                }
                clear_active_run(&self.config_dir);
                Ok(None)
            }
            Some("submitted" | "working" | "input_required") => Ok(None),
            _ => Err(CloudErrorDetail::generic(
                "Spiron receipt has missing or unknown status; request retained",
            )),
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
                                if let Some(run_id) = input
                                    .request
                                    .run_id
                                    .as_deref()
                                    .filter(|r| *r != task.as_str())
                                {
                                    cancel_spiron_run(
                                        &session.api.base,
                                        session.api.token.as_deref(),
                                        &session.agent_id,
                                        run_id,
                                        Some(task),
                                    )?;
                                } else {
                                    cancel_cloud_turn(
                                        &session.api.base,
                                        HARNESS,
                                        session.api.token.as_deref(),
                                        task,
                                    )?;
                                }
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
            let outcome = match input.advance_detailed(&session, strict, &TurnCancel::default()) {
                Ok(outcome) => outcome,
                Err(detail) => {
                    if matches!(detail.status, Some(401 | 429 | 503)) {
                        let uuid = input.event["uuid"].clone();
                        emit_structured_cloud_error(
                            &mut writer,
                            &session.agent_id,
                            &detail,
                            Some(&[uuid]),
                        );
                    }
                    return Err(detail.message);
                }
            };
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
                if let Some(detail) = input.error_detail.take() {
                    emit_structured_cloud_error(
                        &mut writer,
                        &session.agent_id,
                        &detail,
                        Some(&[uuid]),
                    );
                } else {
                    emit(
                        &mut writer,
                        &json!({"type":"result", "subtype":"error", "is_error":true,
                        "session_id":session.agent_id, "result":reason,"completed_message_uuids":[uuid]}),
                    )?;
                }
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
                let next_wait = if input.interval > INITIAL_POLL {
                    apply_poll_jitter(input.interval)
                } else {
                    input.interval
                };
                input.next_poll = Instant::now() + next_wait;
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

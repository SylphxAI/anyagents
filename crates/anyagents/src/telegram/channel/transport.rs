//! Telegram Bot API transport and redacted provider errors.

use std::time::Duration;

use anyagents_channel_telegram::BotCall;
use anyagents_core::{ChannelId, Error};

use super::{MediaUpload, SHORT_TIMEOUT_S, api_base};

const SET_CHAT_PHOTO: &str = "setChatPhoto";

pub(super) fn api_error(
    method: &str,
    channel: Option<&anyagents_core::ChannelId>,
    class: &str,
    status: Option<u16>,
    code: Option<i64>,
    description: &str,
    retry_after: Option<u64>,
) -> anyagents_core::Error {
    // BotCall is internal today, but do not make that a redaction dependency.
    let method = match method {
        "sendMessage"
        | "sendRichMessage"
        | "editMessageText"
        | "sendPhoto"
        | "sendVideo"
        | "sendAudio"
        | "sendVoice"
        | "sendDocument"
        | "sendAnimation"
        | "sendLocation"
        | "sendVenue"
        | "sendPoll"
        | "getFile"
        | "getUpdates"
        | "getMe"
        | "getChat"
        | "getChatMember"
        | "setChatTitle"
        | "setChatDescription"
        | SET_CHAT_PHOTO
        | "setWebhook"
        | "deleteWebhook"
        | "setMyName"
        | "setMyShortDescription"
        | "setMyDescription"
        | "getManagedBotToken"
        | "replaceManagedBotToken" => method,
        _ => "unknown",
    };
    anyagents_core::Error::Message(format!(
        "telegram {}",
        serde_json::json!({
            "method": method, "channel_id": channel.map(|id| id.as_str()),
            "class": class, "status": status,
            "error_code": code, "description": description, "retry_after": retry_after,
        })
    ))
}

fn is_chat_admin_method(method: &str) -> bool {
    matches!(
        method,
        "setChatTitle" | "setChatDescription" | SET_CHAT_PHOTO
    )
}

fn provider_description<'a>(method: &str, description: Option<&'a str>) -> (&'static str, &'a str) {
    // Allow known provider text verbatim, never arbitrary responses that could
    // echo credentials, a private source URL, or the submitted chat metadata.
    if is_chat_admin_method(method)
        && let Some(text) = description
    {
        match text.to_ascii_lowercase().as_str() {
            "bad request: not enough rights to change chat title"
            | "bad request: not enough rights to change chat photo"
            | "bad request: not enough rights to change chat description"
            | "bad request: not enough rights to set chat description"
            | "bad request: not enough rights to change chat info"
            | "bad request: chat_admin_required"
            | "forbidden: chat_admin_required" => return ("chat_admin_required", text),
            "bad request: chat_not_modified"
            | "bad request: chat_title_not_modified"
            | "bad request: chat_about_not_modified"
            | "bad request: photo_invalid_dimensions"
            | "bad request: wrong file type"
            | "bad request: there is no photo in the request"
            | "bad request: chat title can't be changed"
            | "bad request: can't change chat photo"
            | "bad request: photo must be uploaded as an inputfile" => {
                return ("provider_refused", text);
            }
            _ => {}
        }
    }
    let normalized = description.map(str::to_ascii_lowercase);
    match normalized.as_deref() {
        Some("forbidden: bot was blocked by the user") => {
            ("bot_blocked", "Forbidden: bot was blocked by the user")
        }
        Some("forbidden: bot can't initiate conversation with a user") => (
            "human_start_required",
            "Forbidden: bot can't initiate conversation with a user",
        ),
        Some("bad request: chat not found") => ("chat_not_found", "Bad Request: chat not found"),
        Some("forbidden: user is deactivated") => {
            ("user_deactivated", "Forbidden: user is deactivated")
        }
        Some("forbidden: bot can't send messages to bots") => (
            "bot_destination_forbidden",
            "Forbidden: bot can't send messages to bots",
        ),
        Some("forbidden: bot is not a member of the supergroup chat") => (
            "not_member",
            "Forbidden: bot is not a member of the supergroup chat",
        ),
        Some("forbidden: bot is not a member of the group chat") => (
            "not_member",
            "Forbidden: bot is not a member of the group chat",
        ),
        Some("forbidden: bot is not a member of the channel chat") => (
            "not_member",
            "Forbidden: bot is not a member of the channel chat",
        ),
        Some("forbidden: bot was kicked from the group chat") => (
            "not_member",
            "Forbidden: bot was kicked from the group chat",
        ),
        Some("forbidden: bot was kicked from the supergroup chat") => (
            "not_member",
            "Forbidden: bot was kicked from the supergroup chat",
        ),
        Some("forbidden: bot was kicked from the channel chat") => (
            "not_member",
            "Forbidden: bot was kicked from the channel chat",
        ),
        Some("bad request: message thread not found" | "bad request: message_thread_invalid") => {
            ("invalid_topic", "Bad Request: message thread not found")
        }
        Some("bad request: topic_closed") => ("topic_closed", "Bad Request: topic closed"),
        Some(
            "bad request: message to be replied not found"
            | "bad request: reply message not found"
            | "bad request: reply_message_not_found",
        ) => (
            "invalid_reply",
            "Bad Request: message to be replied not found",
        ),
        Some(
            "bad request: not enough rights to send text messages to the chat"
            | "bad request: chat_write_forbidden"
            | "bad request: have no rights to send a message"
            | "forbidden: not enough rights to send text messages to the chat",
        ) => (
            "send_forbidden",
            "Bad Request: not enough rights to send text messages to the chat",
        ),
        Some("bad request: message is too long") => {
            ("message_too_long", "Bad Request: message is too long")
        }
        Some("bad request: message text is empty" | "bad request: text must be non-empty") => {
            ("empty_message", "Bad Request: message text is empty")
        }
        Some(s) if s.starts_with("bad request: can't parse entities") => {
            ("invalid_entities", "Bad Request: can't parse entities")
        }
        Some("bad request: group chat was upgraded to a supergroup chat") => (
            "chat_migrated",
            "Bad Request: group chat was upgraded to a supergroup chat",
        ),
        Some("bad request: peer_id_invalid" | "bad request: user not found") => {
            ("chat_not_found", "Bad Request: peer_id_invalid")
        }
        Some(
            "conflict: terminated by other getupdates request; make sure that only one bot instance is running",
        ) => (
            "poll_conflict",
            "Conflict: another getUpdates request is active",
        ),
        Some(
            "conflict: can't use getupdates method while webhook is active; use deletewebhook to delete the webhook first",
        ) => ("poll_conflict", "Conflict: webhook is active"),
        Some("unauthorized") => ("unauthorized", "Unauthorized"),
        _ => ("provider_refused", "provider description redacted"),
    }
}

pub(super) fn call_api(
    token: &str,
    call: &BotCall,
    timeout_s: u64,
    channel: Option<&anyagents_core::ChannelId>,
) -> Result<String, anyagents_core::Error> {
    let url = format!("{}/bot{token}/{}", api_base(), call.method);
    // Share the pool, not the request deadline: getUpdates and outbound sends
    // have different budgets. Redirect refusal protects the bot token/body.
    static HTTP: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
    let agent = HTTP.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(5)))
            .http_status_as_error(false)
            .max_redirects(0)
            .max_redirects_will_error(false)
            .build()
            .new_agent()
    });
    let response = agent
        .post(&url)
        .config()
        .timeout_global(Some(Duration::from_secs(timeout_s)))
        .build()
        .send_json(&call.body)
        .map_err(|err| {
            let (class, description) = match err {
                ureq::Error::Timeout(_) => (
                    "timeout",
                    "Telegram request timed out; delivery may be uncertain",
                ),
                ureq::Error::HostNotFound => ("network", "Telegram hostname could not be resolved"),
                ureq::Error::Io(_) | ureq::Error::ConnectionFailed => (
                    "network",
                    "Telegram connection failed; delivery may be uncertain",
                ),
                _ => (
                    "transport",
                    "Telegram transport failed; delivery may be uncertain",
                ),
            };
            api_error(&call.method, channel, class, None, None, description, None)
        })?;
    read_response(response, &call.method, channel)
}

fn read_response(
    response: ureq::http::Response<ureq::Body>,
    method: &str,
    channel: Option<&ChannelId>,
) -> Result<String, Error> {
    let status = response.status().as_u16();
    let body = response.into_body().read_to_string().map_err(|_| {
        api_error(
            method,
            channel,
            "response_read",
            Some(status),
            None,
            "Telegram response could not be read; delivery may be uncertain",
            None,
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| {
        api_error(
            method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram response is not JSON; delivery may be uncertain",
            None,
        )
    })?;
    let ok = value.get("ok").and_then(serde_json::Value::as_bool);
    if (200..300).contains(&status) && ok.is_none() {
        return Err(api_error(
            method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram response has no acknowledgement; delivery may be uncertain",
            None,
        ));
    }
    if !(200..300).contains(&status) || ok != Some(true) {
        let code = value.get("error_code").and_then(serde_json::Value::as_i64);
        let (mut class, mut description) = provider_description(
            method,
            value.get("description").and_then(serde_json::Value::as_str),
        );
        let retry_after = value
            .pointer("/parameters/retry_after")
            .and_then(serde_json::Value::as_u64);
        if status == 429 || code == Some(429) {
            class = "rate_limited";
            description = "Telegram rate limit exceeded";
        }
        return Err(api_error(
            method,
            channel,
            class,
            Some(status),
            code,
            description,
            retry_after,
        ));
    }
    if (method == "sendMessage" || method == "sendRichMessage")
        && value
            .pointer("/result/message_id")
            .and_then(serde_json::Value::as_i64)
            .is_none()
    {
        return Err(api_error(
            method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram response has no message receipt; delivery may be uncertain",
            None,
        ));
    }
    if is_chat_admin_method(method) && value["result"].as_bool() != Some(true) {
        return Err(api_error(
            method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram did not acknowledge the chat change; outcome may be uncertain",
            None,
        ));
    }
    Ok(body)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn call_api_multipart(
    token: &str,
    channel_id: &anyagents_core::ChannelId,
    chat_id: i64,
    kind: &str,
    upload: &MediaUpload,
    caption: Option<&str>,
    title: Option<&str>,
    performer: Option<&str>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<String, anyagents_core::Error> {
    let (method, field) = match kind {
        "photo" | "image" => ("sendPhoto", "photo"),
        "video" => ("sendVideo", "video"),
        "audio" | "music" => ("sendAudio", "audio"),
        "voice" => ("sendVoice", "voice"),
        "animation" | "gif" => ("sendAnimation", "animation"),
        _ => ("sendDocument", "document"),
    };
    let mut fields = vec![("chat_id", chat_id.to_string())];
    if let Some(cap) = caption.filter(|c| !c.trim().is_empty()) {
        let html = anyagents_channel_telegram::markdown_to_telegram_html(cap);
        fields.push(("caption", html));
        fields.push(("parse_mode", "HTML".into()));
    }
    if field == "audio" {
        if let Some(t) = title.map(str::trim).filter(|s| !s.is_empty()) {
            fields.push(("title", t.to_string()));
        }
        if let Some(p) = performer.map(str::trim).filter(|s| !s.is_empty()) {
            fields.push(("performer", p.to_string()));
        }
    }
    if let Some(id) = reply_to {
        fields.push(("reply_to_message_id", id.to_string()));
    }
    if let Some(id) = thread {
        fields.push(("message_thread_id", id.to_string()));
    }
    upload_multipart(
        token,
        channel_id,
        method,
        fields,
        (field, "application/octet-stream", upload),
    )
}

pub(super) fn set_chat_photo(
    token: &str,
    channel_id: &ChannelId,
    chat_id: i64,
    mime: &str,
    upload: &MediaUpload,
) -> Result<String, Error> {
    upload_multipart(
        token,
        channel_id,
        SET_CHAT_PHOTO,
        [("chat_id", chat_id.to_string())],
        ("photo", mime, upload),
    )
}

fn upload_multipart<'a>(
    token: &str,
    channel_id: &ChannelId,
    method: &str,
    fields: impl IntoIterator<Item = (&'a str, String)>,
    (field, mime, upload): (&str, &str, &MediaUpload),
) -> Result<String, Error> {
    let file_bytes = &upload.bytes;
    let file_name = &upload.file_name;
    let boundary = format!("----anyagents-tg-{}", anyagents_core::MessageId::generate());
    let mut payload: Vec<u8> = Vec::with_capacity(file_bytes.len() + 1024);
    let mut add_field = |name: &str, val: &str| {
        payload.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{val}\r\n"
            )
            .as_bytes(),
        );
    };
    for (name, value) in fields {
        add_field(name, &value);
    }
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{file_name}\"\r\nContent-Type: {mime}\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(file_bytes);
    payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let url = format!("{}/bot{token}/{method}", api_base());
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(SHORT_TIMEOUT_S)))
        .http_status_as_error(false)
        .max_redirects(0)
        .max_redirects_will_error(false)
        .build()
        .new_agent();
    let content_type = format!("multipart/form-data; boundary={boundary}");
    let response = agent
        .post(&url)
        .header("content-type", &content_type)
        .send(&payload[..])
        .map_err(|_| {
            api_error(
                method,
                Some(channel_id),
                "transport",
                None,
                None,
                "Telegram multipart upload failed; delivery may be uncertain",
                None,
            )
        })?;
    read_response(response, method, Some(channel_id))
}

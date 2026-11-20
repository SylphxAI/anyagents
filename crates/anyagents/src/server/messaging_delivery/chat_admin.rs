//! Chat-wide actions use a chat ID, never message or topic coordinates.

use std::path::Path;

use anyagents_core::Store;
use axum::http::StatusCode;

use crate::agent_file::FileAuthority;
use crate::api::{InteractRequest, InteractResponse, SendMedia};
use crate::auth::Principal;
use crate::telegram::chat_admin::{Change, ChatAdmin, Photo};

use super::ApiError;

#[cfg(test)]
#[path = "chat_admin/tests.rs"]
mod tests;

pub(super) fn apply(
    store: &Store,
    principal: &Principal,
    action: String,
    body: InteractRequest,
) -> Result<InteractResponse, ApiError> {
    let chat_id = chat_id(&body.target)?;
    let Principal::Agent { agent_id, .. } = principal else {
        return Err(ApiError::forbidden(
            "chat administration requires an agent's own Telegram bot",
        ));
    };
    let admin =
        ChatAdmin::for_agent(store, agent_id, body.bot.as_deref()).map_err(ApiError::from_store)?;
    let change = match action.as_str() {
        "set_chat_title" => Change::Title(title(body.text.as_deref())?),
        "set_chat_description" => Change::Description(description(body.text.as_deref())?),
        "set_chat_photo" => Change::Photo(photo(
            &FileAuthority::of_agent_id(store, agent_id),
            body.media.as_ref(),
        )?),
        _ => {
            return Err(bad_request(
                "unsupported Telegram chat administration action",
            ));
        }
    };
    admin.apply(chat_id, change).map_err(ApiError::from_store)?;
    let destination = format!("telegram:chat:{chat_id}");
    let detail = format!("{action} on {destination}");
    // Record the effect, not the local path or a possibly signed source URL.
    store
        .record_outbound(agent_id, &destination, None, &detail)
        .map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Telegram applied {detail}, but recording it in outbound history failed"),
            )
        })?;
    Ok(InteractResponse {
        ok: true,
        action,
        target: destination,
        detail: Some(detail),
    })
}

fn bad_request(message: &'static str) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, message)
}

fn chat_id(target: &str) -> Result<i64, ApiError> {
    target.trim().strip_prefix("telegram:chat:")
        .and_then(|id| id.parse::<i64>().ok())
        .filter(|id| *id < 0)
        .ok_or_else(|| bad_request("Telegram chat administration requires telegram:chat:<chat_id> with a negative group or channel ID and no topic or message suffix"))
}

fn title(text: Option<&str>) -> Result<&str, ApiError> {
    text.filter(|value| !value.trim().is_empty() && value.chars().count() <= 128)
        .ok_or_else(|| bad_request("set_chat_title requires non-blank text of 1–128 characters"))
}

fn description(text: Option<&str>) -> Result<&str, ApiError> {
    text.filter(|value| value.chars().count() <= 255)
        .ok_or_else(|| bad_request("set_chat_description requires text of 0–255 characters (use an empty string to clear it)"))
}

fn photo(authority: &FileAuthority, media: Option<&SendMedia>) -> Result<Photo, ApiError> {
    let source = media.filter(|media| media.kind.trim().eq_ignore_ascii_case("photo"))
        .map(|media| media.source.trim()).filter(|source| !source.is_empty())
        .ok_or_else(|| bad_request("set_chat_photo requires media.kind=photo and media.source=local JPEG/PNG path or public HTTP(S) image URL"))?;
    if let Ok(url) = url::Url::parse(source) {
        return Photo::download(&url).map_err(ApiError::from_store);
    }
    let bytes = authority
        .read(Path::new(source), Photo::MAX_BYTES)
        .map_err(|why| ApiError::forbidden(format!("chat photo not read: {why}")))?;
    Photo::from_bytes(bytes).map_err(ApiError::from_store)
}

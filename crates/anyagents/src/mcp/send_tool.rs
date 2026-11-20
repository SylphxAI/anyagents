//! Unified send dispatch: normalize destinations and route interaction actions.

use super::*;
use rmcp::handler::server::wrapper::Json;

pub(super) async fn send(
    client: &AgentMcp,
    args: SendArgs,
) -> Result<Json<serde_json::Value>, String> {
    let mut args = args;
    // Relation words (`parent`, `children`, ...) read the same in `to` and
    // in `reply_to`, whatever their case or `agent:` prefix.
    for target in &mut args.to {
        if let Some(word) = relation_word(target) {
            *target = word.to_string();
        }
    }
    if args.to.is_empty()
        && let Some(word) = args
            .reply_to
            .as_deref()
            .and_then(relation_word)
            .filter(|word| matches!(*word, "parent" | "children"))
    {
        args.to = vec![word.to_string()];
        args.reply_to = None;
    }
    let inferred_interact = args
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("send"))
        .map(str::to_ascii_lowercase)
        .or_else(|| {
            if args.emoji.is_some()
                && args.to.is_empty()
                && args
                    .text
                    .as_deref()
                    .is_none_or(|text| text.trim().is_empty())
            {
                Some("react".to_string())
            } else {
                None
            }
        });
    if let Some(interact_action) = inferred_interact {
        let body = interaction_request(args, interact_action)?;
        let res = post_interaction(client, body).await?;
        return Ok(rmcp::handler::server::wrapper::Json(
            serde_json::to_value(res).map_err(|e| e.to_string())?,
        ));
    }

    let quote = args
        .quote
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|t| anyagents_core::Quote {
            text: t.to_string(),
            ..anyagents_core::Quote::default()
        });
    let body = SendRequest {
        // The server sends as this agent whatever this says, and refuses a
        // body that names anyone else.
        sender: Some(format!("agent:{}", client.agent.name)),
        to: args.to,
        text: args.text.unwrap_or_default(),
        reply_to: args.reply_to,
        quote,
        media: args.media,
        location: args.location,
        poll: args.poll,
        checklist: args.checklist,
        article: args.article,
        bot: args.bot,
        idempotency_key: args.idempotency_key,
    };
    let closing = body.is_turn_closing_sentinel();
    let response: SendResponse = client
        .client
        .get(
            "/v1/send",
            Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
        )
        .await?;
    Ok(rmcp::handler::server::wrapper::Json(send_turn_result(
        response, closing,
    )?))
}

fn interaction_request(args: SendArgs, action: String) -> Result<InteractRequest, String> {
    let chat_action = crate::api::is_telegram_chat_admin_action(&action);
    let target = if chat_action {
        match (args.target, args.to.as_slice()) {
            (Some(target), []) => Some(target),
            (None, [target]) => Some(target.clone()),
            _ => return Err("a Telegram chat action requires exactly one telegram:chat:<chat_id> in target or to".into()),
        }
    } else {
        args.target
            .or(args.reply_to)
            .or_else(|| args.to.into_iter().next())
    };
    let target = target
        .map(|target| target.trim().to_string())
        .filter(|target| !target.is_empty())
        .ok_or_else(|| format!("send action {action} requires a target"))?;
    let text = args
        .text
        .filter(|text| chat_action || !text.trim().is_empty());
    Ok(InteractRequest {
        action,
        target,
        emoji: args.emoji,
        options: args.options,
        text,
        media: args.media.filter(|_| chat_action),
        bot: args.bot,
        ..Default::default()
    })
}

pub(super) async fn post_interaction(
    client: &AgentMcp,
    mut body: InteractRequest,
) -> Result<InteractResponse, String> {
    body.sender = Some(format!("agent:{}", client.agent.name));
    client
        .client
        .get(
            "/v1/interact",
            Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
        )
        .await
}

#[cfg(test)]
mod chat_admin_tests {
    use serde_json::json;

    use super::*;

    fn request(input: serde_json::Value) -> Result<InteractRequest, String> {
        let args: SendArgs = serde_json::from_value(input).unwrap();
        let action = args.action.clone().unwrap();
        interaction_request(args, action)
    }

    #[test]
    fn chat_admin_actions_are_advertised_and_forwarded() {
        let schema = serde_json::to_value(schemars::schema_for!(SendArgs)).unwrap();
        for action in [
            "set_chat_title",
            "set_chat_description",
            "set_chat_photo",
            "pin",
        ] {
            assert!(
                schema["properties"]["action"]["enum"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(action))
            );
        }
        let body = request(json!({"action":"set_chat_description", "to":["telegram:chat:-41"], "text":"", "bot":"@chosen_bot"})).unwrap();
        assert_eq!(body.text.as_deref(), Some(""));
        assert_eq!(body.target, "telegram:chat:-41");
        assert_eq!(body.bot.as_deref(), Some("@chosen_bot"));
        let body =
            request(json!({"action":"set_chat_title","target":"telegram:chat:-41","text":"null"}))
                .unwrap();
        assert_eq!(body.text.as_deref(), Some("null"));
        let body = request(json!({"action":"set_chat_photo","target":"telegram:chat:-41","media":{"kind":"photo","source":"https://example.org/a.png"}})).unwrap();
        assert_eq!(body.media.unwrap().source, "https://example.org/a.png");
    }

    #[test]
    fn chat_admin_requires_one_explicit_chat_but_pin_still_accepts_message_replies() {
        assert!(
            request(
                json!({"action":"set_chat_title","to":["telegram:chat:-1","telegram:chat:-2"]})
            )
            .is_err()
        );
        assert!(
            request(json!({"action":"set_chat_title","reply_to":"anyagents:reply:msg_1"})).is_err()
        );
        assert_eq!(
            request(json!({"action":"pin","reply_to":"anyagents:reply:msg_1"}))
                .unwrap()
                .target,
            "anyagents:reply:msg_1"
        );
    }
}

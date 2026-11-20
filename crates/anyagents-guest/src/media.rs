//! Sandbox media directory setup (`/home/user/agent-data/media`) and inbound
//! attachment hydration (including Telegram photos and `document` attachments
//! such as `.docx` / `.pdf`) with prompt path rewriting.

use std::path::{Path, PathBuf};

use crate::guest::GuestConfig;

pub(crate) fn sandbox_data_dir(state_dir: &Path, env: &[(String, String)]) -> PathBuf {
    for key in ["ANYAGENTS_AGENT_DATA_DIR", "ANYAGENTS_SANDBOX_DATA_DIR"] {
        if let Some(val) = lookup_env(key, env) {
            return PathBuf::from(val);
        }
    }
    if state_dir.file_name().and_then(|n| n.to_str()) == Some("state")
        && let Some(parent) = state_dir.parent()
    {
        return parent.to_path_buf();
    }
    PathBuf::from(crate::DEFAULT_DATA_MOUNT_DIR)
}

pub(crate) fn enrich_guest_env(
    state_dir: &Path,
    file_cfg: Option<&GuestConfig>,
    env: &mut Vec<(String, String)>,
) {
    let data_dir = sandbox_data_dir(state_dir, env);
    let media_dir = ensure_guest_media_dir(state_dir, env);
    let mut extras = vec![
        ("ANYAGENTS_AGENT_DATA_DIR", data_dir.display().to_string()),
        ("ANYAGENTS_SANDBOX_DATA_DIR", data_dir.display().to_string()),
        ("ANYAGENTS_MEDIA_DIR", media_dir.display().to_string()),
        (
            "ANYAGENTS_BROWSER_SCREENSHOT_DIR",
            data_dir.join("tmp/screenshots").display().to_string(),
        ),
    ];
    if let Some(id) =
        lookup_env("ANYAGENTS_AGENT_ID", env).or_else(|| file_cfg.and_then(|c| c.agent_id.clone()))
    {
        extras.push(("ANYAGENTS_AGENT_ID", id));
    }
    for (k, v) in extras {
        if !env.iter().any(|(ek, ev)| ek == k && !ev.trim().is_empty()) {
            env.retain(|(ek, _)| ek != k);
            env.push((k.to_string(), v));
        }
    }
    if let Some(url) = resolve_api_base(env, file_cfg) {
        env.retain(|(ek, _)| ek != "ANYAGENTS_API_URL");
        env.push(("ANYAGENTS_API_URL".to_string(), url));
    }
}

pub(crate) fn ensure_guest_media_dir(state_dir: &Path, env: &[(String, String)]) -> PathBuf {
    let data_dir = sandbox_data_dir(state_dir, env);
    let media_dir = lookup_env("ANYAGENTS_MEDIA_DIR", env)
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("media"));
    for dir in [&media_dir, &data_dir.join("tmp/screenshots")] {
        let _ = std::fs::create_dir_all(dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755));
        }
    }
    let state_media = state_dir.join("media");
    if state_media.is_dir()
        && state_media != media_dir
        && let Ok(entries) = std::fs::read_dir(&state_media)
    {
        for entry in entries.flatten() {
            let src = entry.path();
            if src.is_file()
                && let Some(name) = src.file_name()
            {
                let dst = media_dir.join(name);
                if !dst.is_file() {
                    let _ = std::fs::copy(&src, &dst);
                }
            }
        }
    }
    media_dir
}

/// Hydrate inbound media references (`/v1/media/...` and `[Local media (<kind>) file: <path>]`,
/// including Telegram `document` attachments) into `<data-dir>/media/` and rewrite host paths.
pub(crate) fn hydrate_turn_media_refs(
    text: &str,
    state_dir: &Path,
    env: &[(String, String)],
    file_cfg: Option<&GuestConfig>,
) -> String {
    let media_dir = ensure_guest_media_dir(state_dir, env);
    if !text.contains("media") && !text.contains("Local media") {
        return text.to_string();
    }
    let api_base = resolve_api_base(env, file_cfg);
    let token = resolve_auth_token(state_dir, env, file_cfg);
    let agent_id =
        lookup_env("ANYAGENTS_AGENT_ID", env).or_else(|| file_cfg.and_then(|c| c.agent_id.clone()));

    let mut out = Vec::new();
    for line in text.lines() {
        out.push(rewrite_media_line(
            line,
            &media_dir,
            state_dir,
            api_base.as_deref(),
            token.as_deref(),
            agent_id.as_deref(),
        ));
    }
    let mut joined = out.join("\n");
    if text.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

fn rewrite_media_line(
    line: &str,
    media_dir: &Path,
    state_dir: &Path,
    api_base: Option<&str>,
    token: Option<&str>,
    agent_id: Option<&str>,
) -> String {
    // 1. `[Local media (<kind>) file: <path>]` (photos, documents `.docx`/`.pdf`, audio, video).
    if let Some(start) = line.find("[Local media (")
        && let Some(marker) = line[start..].find("file:")
        && let Some(end_rel) = line[start + marker..].find(']')
    {
        let path_start = start + marker + "file:".len();
        let path_end = start + marker + end_rel;
        let raw_path = line[path_start..path_end].trim();
        if let Some(local) =
            materialize_media_ref(raw_path, media_dir, state_dir, api_base, token, agent_id)
        {
            return format!(
                "{} {}{}",
                &line[..path_start],
                local.display(),
                &line[path_end..]
            );
        }
    }

    // 2. Direct `/v1/media/<id>` or `/var/lib/anyagents/media/<name>` references.
    let mut updated = line.to_string();
    for tok in line.split_whitespace() {
        let clean = tok.trim_matches(|c: char| {
            matches!(
                c,
                '(' | ')' | '[' | ']' | ',' | ';' | '"' | '\'' | '<' | '>'
            )
        });
        let is_media_ref =
            clean.contains("/v1/media/") || clean.starts_with("/var/lib/anyagents/media/");
        if is_media_ref
            && let Some(local) =
                materialize_media_ref(clean, media_dir, state_dir, api_base, token, agent_id)
        {
            updated = updated.replace(clean, &local.display().to_string());
        }
    }
    updated
}

fn materialize_media_ref(
    raw_ref: &str,
    media_dir: &Path,
    state_dir: &Path,
    api_base: Option<&str>,
    token: Option<&str>,
    agent_id: Option<&str>,
) -> Option<PathBuf> {
    let raw_path = Path::new(raw_ref);
    if raw_path.is_file() {
        if raw_path.starts_with(media_dir) {
            return Some(raw_path.to_path_buf());
        }
        let name = sanitize_filename(raw_path.file_name()?.to_str()?);
        let dst = media_dir.join(name);
        if std::fs::copy(raw_path, &dst).is_ok() {
            return Some(dst);
        }
    }
    let basename = raw_ref
        .split('?')
        .next()
        .unwrap_or(raw_ref)
        .rsplit('/')
        .next()
        .map(sanitize_filename)
        .filter(|s| !s.is_empty())?;
    let existing = media_dir.join(&basename);
    if existing.is_file() {
        return Some(existing);
    }
    let in_state = state_dir.join("media").join(&basename);
    if in_state.is_file() && std::fs::copy(&in_state, &existing).is_ok() {
        return Some(existing);
    }
    let url = if raw_ref.starts_with("http://") || raw_ref.starts_with("https://") {
        raw_ref.to_string()
    } else if raw_ref.starts_with("/v1/media/") {
        format!("{}{raw_ref}", api_base?)
    } else {
        format!("{}/v1/media/{basename}", api_base?)
    };
    download_media(&url, &basename, media_dir, token, agent_id)
}

fn download_media(
    url: &str,
    fallback_name: &str,
    media_dir: &Path,
    token: Option<&str>,
    agent_id: Option<&str>,
) -> Option<PathBuf> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(30)))
        .build()
        .new_agent();
    let mut req = agent.get(url);
    if let Some(tok) = token {
        req = req.header("Authorization", &format!("Bearer {tok}"));
    }
    if let Some(id) = agent_id {
        req = req.header("x-anyagents-agent", id);
    }
    let resp = req.call().ok()?;
    let filename = resp
        .headers()
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_disposition_filename)
        .map(sanitize_filename)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| fallback_name.to_string());
    let bytes = resp.into_body().read_to_vec().ok()?;
    if bytes.is_empty() {
        return None;
    }
    let dst = media_dir.join(filename);
    anyagents_harness::atomic_write_nonempty_file(&dst, &bytes, 0o644).ok()?;
    Some(dst)
}

fn parse_disposition_filename(header: &str) -> Option<&str> {
    let idx = header.to_ascii_lowercase().find("filename=")?;
    let rest = &header[idx + "filename=".len()..];
    let val = rest.split(';').next()?.trim().trim_matches('"');
    (!val.is_empty()).then_some(val)
}

fn sanitize_filename(raw: &str) -> String {
    raw.rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        .collect::<String>()
        .trim_start_matches('.')
        .to_string()
}

fn lookup_env(key: &str, env: &[(String, String)]) -> Option<String> {
    env.iter()
        .rev()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .or_else(|| std::env::var(key).ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(crate) fn normalize_api_base_url(raw: &str) -> String {
    let mut url = raw.trim().trim_end_matches('/');
    for suffix in ["/v1/webhooks/mcp", "/v1/mcp", "/mcp", "/v1/webhooks"] {
        if let Some(stripped) = url.strip_suffix(suffix) {
            url = stripped.trim_end_matches('/');
            break;
        }
    }
    url.to_string()
}

pub(crate) fn resolve_api_base(
    env: &[(String, String)],
    file_cfg: Option<&GuestConfig>,
) -> Option<String> {
    let raw = lookup_env("ANYAGENTS_API_URL", env)
        .or_else(|| lookup_env("ANYAGENTS_MCP_URL", env))
        .or_else(|| file_cfg.and_then(|c| c.mcp_url.clone()))?;
    let base = normalize_api_base_url(&raw);
    (!base.is_empty()).then_some(base)
}

pub(crate) fn resolve_auth_token(
    state_dir: &Path,
    env: &[(String, String)],
    file_cfg: Option<&GuestConfig>,
) -> Option<String> {
    if let Some(t) = lookup_env("ANYAGENTS_AGENT_TOKEN", env) {
        return Some(t);
    }
    let path = lookup_env("ANYAGENTS_MCP_TOKEN_FILE", env)
        .map(PathBuf::from)
        .or_else(|| file_cfg.and_then(|c| c.mcp_token_file.clone()))
        .unwrap_or_else(|| state_dir.join(".mcp-token"));
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;

    #[test]
    fn hydrates_telegram_document_attachment_via_http_and_rewrites_prompt_path() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let resp = "HTTP/1.1 200 OK\r\nContent-Disposition: attachment; filename=\"brief.docx\"\r\nContent-Length: 4\r\n\r\nPK\x03\x04";
            let _ = s.write_all(resp.as_bytes());
            let _ = s.flush();
        });
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        std::fs::create_dir_all(&state).unwrap();
        let env = vec![
            (
                "ANYAGENTS_AGENT_DATA_DIR".into(),
                tmp.path().display().to_string(),
            ),
            ("ANYAGENTS_API_URL".into(), format!("http://{addr}")),
            ("ANYAGENTS_AGENT_TOKEN".into(), "tok-1".into()),
        ];
        let prompt =
            "Please review:\n[Local media (document) file: /var/lib/anyagents/media/att_1.docx]";
        let rewritten = hydrate_turn_media_refs(prompt, &state, &env, None);
        srv.join().unwrap();
        let expected = tmp.path().join("media/brief.docx");
        assert!(
            expected.is_file(),
            "expected downloaded document at {}",
            expected.display()
        );
        assert!(
            rewritten.contains(&expected.display().to_string()),
            "{rewritten}"
        );
    }

    #[test]
    fn resolve_api_base_strips_webhook_and_mcp_suffixes_for_api_and_mcp_urls() {
        for raw in [
            "https://coord.example/v1/webhooks/mcp",
            "https://coord.example/v1/webhooks/mcp/",
            "https://coord.example/v1/webhooks",
            "https://coord.example/v1/mcp",
            "https://coord.example/mcp",
            "https://coord.example/",
        ] {
            assert_eq!(
                normalize_api_base_url(raw),
                "https://coord.example",
                "{raw}"
            );
            let api_env = vec![("ANYAGENTS_API_URL".into(), raw.into())];
            assert_eq!(
                resolve_api_base(&api_env, None).as_deref(),
                Some("https://coord.example"),
                "ANYAGENTS_API_URL={raw}"
            );
            let mcp_env = vec![("ANYAGENTS_MCP_URL".into(), raw.into())];
            assert_eq!(
                resolve_api_base(&mcp_env, None).as_deref(),
                Some("https://coord.example"),
                "ANYAGENTS_MCP_URL={raw}"
            );
        }

        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        std::fs::create_dir_all(&state).unwrap();
        let mut env = vec![
            (
                "ANYAGENTS_AGENT_DATA_DIR".into(),
                tmp.path().display().to_string(),
            ),
            (
                "ANYAGENTS_API_URL".into(),
                "https://coord.example/v1/webhooks".into(),
            ),
        ];
        enrich_guest_env(&state, None, &mut env);
        assert_eq!(
            lookup_env("ANYAGENTS_API_URL", &env).as_deref(),
            Some("https://coord.example")
        );
    }
}

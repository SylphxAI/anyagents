//! Match only the returned account, never the profile card or a caller-provided name.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;

use anyagents_core::Store;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::sandbox::Spec;
use super::serverless;

static FINALIZE: Mutex<()> = Mutex::new(());
const UNVERIFIED: &str = "The provider did not return an authenticated account identity. No profile was changed. Retry the sign-in.";
const READ_FAILED: &str = "Could not safely read existing profiles. No account was selected. Retry after checking profile storage.";

#[derive(Default)]
struct Identity {
    email: Option<String>,
    subject: Option<String>,
}

impl Identity {
    fn present(&self) -> bool {
        self.email.is_some() || self.subject.is_some()
    }

    fn matches(&self, other: &Self) -> bool {
        // Explicit contradictions beat matches. In particular the same Codex
        // workspace can have different users, and is not a user identity.
        if let (Some(a), Some(b)) = (&self.subject, &other.subject) {
            return a == b;
        }
        matches!((&self.email, &other.email), (Some(a), Some(b)) if a == b)
    }

    fn combine(&mut self, other: Self) -> bool {
        if matches!((&self.subject, &other.subject), (Some(a), Some(b)) if a != b)
            || matches!((&self.email, &other.email), (Some(a), Some(b)) if a != b)
        {
            return false;
        }
        self.subject = self.subject.take().or(other.subject);
        self.email = self.email.take().or(other.email);
        true
    }

    fn name(&self, reserved: &BTreeSet<String>) -> String {
        let key = self
            .subject
            .as_deref()
            .or(self.email.as_deref())
            .expect("nonempty identity");
        let label = self
            .email
            .as_deref()
            .and_then(|email| email.split_once('@'))
            .map_or(key, |(local, _)| local);
        let slug: String = label
            .chars()
            .take(38)
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        let slug = slug.trim_matches('-');
        let slug = if slug.is_empty() { "account" } else { slug };
        // Use a stable digest even for a first connection. Concurrent distinct
        // identities with the same display names cannot select the same slot.
        let digest = format!("{:x}", Sha256::digest(key));
        let base = format!("{slug}-{}", &digest[..24]);
        if !reserved.contains(&base) {
            return base;
        }
        (2..)
            .map(|n| format!("{base}-{n}"))
            .find(|name| !reserved.contains(name))
            .unwrap()
    }
}

fn text(doc: &Value, key: &str) -> Option<String> {
    doc.pointer(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty() && v.len() <= 256 && !v.contains(char::is_control))
        .map(ToOwned::to_owned)
}

fn email(doc: &Value, key: &str) -> Option<String> {
    text(doc, key)
        .filter(|v| {
            v.split_once('@').is_some_and(|(user, host)| {
                !user.is_empty() && !host.is_empty() && !host.contains('@')
            }) && !v.contains(char::is_whitespace)
        })
        .map(|v| v.to_ascii_lowercase())
}

fn parse(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}

fn credential_identity(provider: &str, credential: &Value, existing: bool) -> Option<Identity> {
    Some(match provider {
        "anthropic" => {
            let mut identity = Identity {
                email: email(credential, "/claudeAiOauth/email"),
                subject: text(credential, "/claudeAiOauth/accountUuid"),
            };
            if !identity.combine(Identity {
                email: email(credential, "/claudeAiOauth/tokenAccount/emailAddress"),
                subject: text(credential, "/claudeAiOauth/tokenAccount/uuid"),
            }) {
                return None;
            }
            identity
        }
        "openai" => {
            // This file was written by the vendor CLI in a private, newly
            // created home, not obtained as an unverified token from the form.
            let claims = credential
                .pointer("/tokens/id_token")
                .and_then(Value::as_str)
                .and_then(|jwt| jwt.split('.').nth(1))
                .and_then(|pay| URL_SAFE_NO_PAD.decode(pay).ok())
                .map(|bytes| parse(&bytes))
                .unwrap_or(Value::Null);
            Identity {
                email: if claims.get("email_verified") == Some(&Value::Bool(false)) {
                    None
                } else {
                    email(&claims, "/email")
                }
                .or_else(|| {
                    existing
                        .then(|| email(credential, "/id_token_email"))
                        .flatten()
                }),
                subject: text(&claims, "/sub"),
            }
        }
        "google-antigravity" if existing => Identity {
            email: email(credential, "/email").or_else(|| email(credential, "/id_token_email")),
            subject: text(credential, "/account_id"),
        },
        _ => Identity::default(),
    })
}

fn claude_account(doc: &Value) -> Identity {
    Identity {
        email: email(doc, "/oauthAccount/emailAddress"),
        subject: text(doc, "/oauthAccount/accountUuid"),
    }
}

fn google_account(credential: &Value) -> Option<Identity> {
    let token = credential
        .pointer("/token/access_token")
        .or_else(|| credential.get("access_token"))?
        .as_str()?;
    let doc: Value = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(8)))
        .max_redirects(0)
        .build()
        .new_agent()
        .get("https://www.googleapis.com/oauth2/v2/userinfo")
        .header("authorization", &format!("Bearer {token}"))
        .call()
        .ok()?
        .body_mut()
        .with_config()
        .limit(65536)
        .read_json()
        .ok()?;
    Some(Identity {
        email: (doc.get("verified_email") == Some(&Value::Bool(true)))
            .then(|| email(&doc, "/email"))
            .flatten(),
        subject: text(&doc, "/id"),
    })
}

pub(super) fn finish(
    store: &Store,
    spec: &Spec,
    mut files: BTreeMap<String, Vec<u8>>,
) -> Result<String, String> {
    let credential = files.get(spec.credential).filter(|bytes| serverless::is_incoming_auth_fresher(spec.credential, None, bytes))
        .ok_or("Native sign-in did not produce a valid OAuth credential file. Check the sandbox image and try again.")?;
    let doc = parse(credential);
    let mut identity = if spec.provider == "google-antigravity" {
        google_account(&doc)
    } else {
        credential_identity(spec.provider, &doc, false)
    }
    .ok_or(UNVERIFIED)?;
    for account in spec
        .account_files
        .iter()
        .filter_map(|path| files.get(*path))
    {
        if !identity.combine(claude_account(&parse(account))) {
            return Err(UNVERIFIED.into());
        }
    }
    if !identity.present() {
        return Err(UNVERIFIED.into());
    }
    files.retain(|path, _| path == spec.credential);
    finish_verified(store, spec, &identity, &files)
}

fn credential_paths(spec: &Spec) -> &[&str] {
    match spec.provider {
        "openai" => &["auth.json", ".codex/auth.json"],
        "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
        _ => std::slice::from_ref(&spec.credential),
    }
}

fn finish_verified(
    store: &Store,
    spec: &Spec,
    identity: &Identity,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<String, String> {
    let _guard = FINALIZE.lock().map_err(|_| READ_FAILED)?;
    let blobs = store.blob_store();
    let provider = spec.provider;
    let prefix = format!("{provider}/");
    let root = store.data_dir().join("profiles").join(provider);
    let paths: BTreeSet<_> = blobs
        .list_deployment_blobs("profiles")
        .map_err(|_| READ_FAILED)?
        .into_iter()
        .filter_map(|entry| entry.path.starts_with(&prefix).then_some(entry.path))
        .collect();
    let mut slots: BTreeSet<String> = paths
        .iter()
        .filter_map(|path| {
            path[prefix.len()..]
                .split_once('/')
                .map(|(name, _)| name.to_owned())
        })
        .collect();
    match std::fs::read_dir(&root) {
        Ok(entries) => {
            for entry in entries {
                if let Some(name) = entry.map_err(|_| READ_FAILED)?.file_name().to_str() {
                    slots.insert(name.to_owned());
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return Err(READ_FAILED.into()),
    }
    let mut matching = Vec::new();
    for slot in &slots {
        if slot.starts_with('.')
            || !slot
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        {
            continue;
        }
        if std::fs::symlink_metadata(root.join(slot)).is_ok_and(|m| m.is_symlink()) {
            continue;
        }
        let read = |rel: &str| -> Result<Value, String> {
            let key = format!("{prefix}{slot}/{rel}");
            if paths.contains(&key) {
                return blobs
                    .get_deployment_blob("profiles", &key)
                    .map_err(|_| READ_FAILED)?
                    .map(|entry| parse(&entry.content))
                    .ok_or_else(|| READ_FAILED.into());
            }
            match std::fs::read(root.join(slot).join(rel)) {
                Ok(bytes) => Ok(parse(&bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Value::Null),
                Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => Ok(Value::Null),
                Err(_) => Err(READ_FAILED.into()),
            }
        };
        let meta = read("profile.json")?;
        if text(&meta, "/kind").is_some_and(|kind| kind != "subscription") {
            continue;
        }
        let mut old = Identity::default();
        let mut valid = true;
        for &rel in credential_paths(spec) {
            let doc = read(rel)?;
            if provider == "openai" && text(&doc, "/OPENAI_API_KEY").is_some() {
                valid = false;
                break;
            }
            valid &=
                credential_identity(provider, &doc, true).is_some_and(|claim| old.combine(claim));
        }
        if provider == "anthropic" && !old.present() {
            for &rel in spec.account_files {
                valid &= old.combine(claude_account(&read(rel)?));
            }
        }
        let meta_id = text(&meta, "/oauth_subject").or_else(|| {
            (provider != "openai")
                .then(|| text(&meta, "/account_id"))
                .flatten()
        });
        // Credential claims beat old display metadata. Legacy cards sometimes
        // have only metadata, with no account claim in their native file.
        old.email = old.email.or_else(|| email(&meta, "/account"));
        old.subject = old.subject.or(meta_id);
        if valid && identity.matches(&old) {
            matching.push((slot, meta));
        }
    }
    let (name, mut meta) = match matching.as_slice() {
        [] => (identity.name(&slots), Value::Null),
        [(name, meta)] => ((*name).clone(), meta.clone()),
        _ => return Err("More than one profile matches this authenticated account. Resolve the duplicate profiles before retrying. No credentials were replaced.".into()),
    };
    if !meta.is_object() {
        meta = serde_json::json!({});
    }
    meta["provider"] = provider.into();
    meta["kind"] = "subscription".into();
    meta["compatible_harnesses"] = serde_json::json!([spec.harness]);
    if let Some(email) = &identity.email {
        meta["account"] = email.as_str().into();
    }
    if let Some(subject) = &identity.subject {
        meta["oauth_subject"] = subject.as_str().into();
        if provider != "openai" {
            meta["account_id"] = subject.as_str().into();
        }
    }
    let saved = serverless::persist_signed_in_profile_auth_files(
        store, provider, &name, &meta, files,
    )
    .map_err(
        |_| "Could not save the authenticated profile. Check profile storage and try again.",
    )?;
    if saved == 0 {
        return Err("No valid OAuth credentials were saved. Start a new sign-in.".into());
    }
    Ok(format!("{provider}/{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(email: &str, subject: &str, value: &str) -> BTreeMap<String, Vec<u8>> {
        BTreeMap::from([
            (".claude/.credentials.json".into(), serde_json::to_vec(&serde_json::json!({"claudeAiOauth":{"accessToken":value}})).unwrap()),
            (".claude.json".into(), serde_json::to_vec(&serde_json::json!({"oauthAccount":{"emailAddress":email,"accountUuid":subject}})).unwrap()),
        ])
    }

    #[test]
    fn authenticated_identity_refreshes_only_its_own_profile_and_preserves_slot_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let spec = Spec::for_harness("claude-code").unwrap();
        let blobs = store.blob_store();
        let meta_path = "anthropic/existing/profile.json";
        blobs.put_deployment_blob("profiles", meta_path, br#"{"kind":"subscription","account":"first@example.test","account_id":"first","label":"Keep me"}"#, false).unwrap();
        let first = finish(
            &store,
            &spec,
            fixture("first@example.test", "first", "fixture-first"),
        )
        .unwrap();
        assert_eq!(first, "anthropic/existing");
        let second = finish(
            &store,
            &spec,
            fixture("first@example.test", "different", "fixture-second"),
        )
        .unwrap();
        assert_ne!(
            first, second,
            "a conflicting subject must never overwrite the other account"
        );
        let first_auth = blobs
            .get_deployment_blob("profiles", "anthropic/existing/.credentials.json")
            .unwrap()
            .unwrap();
        assert_eq!(
            parse(&first_auth.content)["claudeAiOauth"]["accessToken"],
            "fixture-first"
        );
        let meta = blobs
            .get_deployment_blob("profiles", meta_path)
            .unwrap()
            .unwrap();
        assert_eq!(parse(&meta.content)["label"], "Keep me");
        assert!(
            blobs
                .get_deployment_blob("profiles", "anthropic/existing/.claude.json")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn claude_identity_accepts_token_account_and_config_dir_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let spec = Spec::for_harness("claude-code").unwrap();
        let mut files = fixture("reader@example.test", "fixture-subject", "fixture-token");
        let config = files.remove(".claude.json").unwrap();
        files.insert(".claude/.claude.json".into(), config);
        let profile = finish(&store, &spec, files).unwrap();

        let files = BTreeMap::from([(
            spec.credential.into(),
            br#"{
            "claudeAiOauth": {"accessToken": "fixture-refresh", "tokenAccount": {
                "emailAddress": "Reader@example.test", "uuid": "fixture-subject"
            }}
        }"#
            .to_vec(),
        )]);
        assert_eq!(finish(&store, &spec, files).unwrap(), profile);
        let blobs = store.blob_store();
        let meta = blobs
            .get_deployment_blob("profiles", &format!("{profile}/profile.json"))
            .unwrap()
            .unwrap();
        assert_eq!(parse(&meta.content)["account"], "reader@example.test");
        assert_eq!(parse(&meta.content)["oauth_subject"], "fixture-subject");
        let saved = blobs
            .get_deployment_blob("profiles", &format!("{profile}/.credentials.json"))
            .unwrap()
            .unwrap();
        assert_eq!(
            parse(&saved.content)["claudeAiOauth"]["accessToken"],
            "fixture-refresh"
        );
        assert!(
            blobs
                .get_deployment_blob("profiles", &format!("{profile}/.claude/.claude.json"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn contradictory_claude_claims_fail_closed_across_all_sources() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let spec = Spec::for_harness("claude-code").unwrap();
        let nested = |email, uuid| {
            serde_json::to_vec(&serde_json::json!({
            "claudeAiOauth": {"accessToken": "fixture-token", "email": "reader@example.test",
                "accountUuid": "fixture-subject", "tokenAccount": {"emailAddress": email, "uuid": uuid}}
        })).unwrap()
        };
        for (email, uuid) in [
            ("other@example.test", "fixture-subject"),
            ("reader@example.test", "other-subject"),
        ] {
            let mut files = fixture("reader@example.test", "fixture-subject", "fixture-token");
            files.insert(spec.credential.into(), nested(email, uuid));
            assert_eq!(finish(&store, &spec, files).unwrap_err(), UNVERIFIED);

            let mut files = fixture("reader@example.test", "fixture-subject", "fixture-token");
            let mut conflict = fixture(email, uuid, "fixture-token");
            files.insert(
                ".claude/.claude.json".into(),
                conflict.remove(".claude.json").unwrap(),
            );
            assert_eq!(
                finish(&store, &spec, files.clone()).unwrap_err(),
                UNVERIFIED
            );

            files.remove(".claude.json");
            files.insert(
                spec.credential.into(),
                nested("reader@example.test", "fixture-subject"),
            );
            assert_eq!(finish(&store, &spec, files).unwrap_err(), UNVERIFIED);
        }
        assert!(
            store
                .blob_store()
                .list_deployment_blobs("profiles")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn missing_and_ambiguous_identities_leave_profiles_unmodified() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let spec = Spec::for_harness("claude-code").unwrap();
        let blobs = store.blob_store();
        let mut files = fixture("first@example.test", "first", "fixture-first");
        files.remove(".claude.json");
        assert_eq!(finish(&store, &spec, files).unwrap_err(), UNVERIFIED);
        assert!(blobs.list_deployment_blobs("profiles").unwrap().is_empty());
        for name in ["one", "two"] {
            blobs
                .put_deployment_blob(
                    "profiles",
                    &format!("anthropic/{name}/profile.json"),
                    br#"{"kind":"subscription","account_id":"first"}"#,
                    false,
                )
                .unwrap();
        }
        assert!(
            finish(
                &store,
                &spec,
                fixture("first@example.test", "first", "fixture-first")
            )
            .unwrap_err()
            .contains("More than one")
        );
        assert_eq!(blobs.list_deployment_blobs("profiles").unwrap().len(), 2);
    }
}

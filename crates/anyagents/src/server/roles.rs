//! Operator API routes for managing custom role templates backed by
//! PostgreSQL `deployment_blobs(namespace = 'roles')` and `<data>/roles/`.

use axum::body::Bytes;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::auth::Principal;
use crate::create;

use super::types::{ApiError, AppState, require_admin_or_operator, spawn_store};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/roles", get(list_roles_route))
        .route(
            "/v1/roles/{name}",
            get(get_role_route)
                .put(put_role_route)
                .delete(delete_role_route),
        )
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct RoleView {
    pub name: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PutRolePayload {
    #[serde(default, alias = "markdown", alias = "body")]
    content: Option<String>,
}

fn build_role_view(
    data_dir: &std::path::Path,
    slug: &str,
    tpl: create::ArchetypeTemplate,
    source: create::ArchetypeSource,
    include_content: bool,
) -> RoleView {
    let source_label = match source {
        create::ArchetypeSource::Disk => "disk",
        create::ArchetypeSource::Builtin => "builtin",
    };
    let content = if include_content {
        std::fs::read_to_string(data_dir.join("roles").join(format!("{slug}.md"))).ok()
    } else {
        None
    };
    RoleView {
        name: slug.to_string(),
        source: source_label.to_string(),
        description: tpl.description,
        scope: tpl.scope,
        harness: tpl.harness,
        model: tpl.model,
        profile: tpl.profile,
        body: tpl.body,
        content,
    }
}

fn extract_role_markdown(headers: &HeaderMap, body: &Bytes) -> Result<String, ApiError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "role template must be valid UTF-8"))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "role template content must not be empty",
        ));
    }
    let is_json_ct = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.to_ascii_lowercase().contains("application/json"));
    if is_json_ct || trimmed.starts_with('{') {
        let parsed: PutRolePayload = serde_json::from_str(trimmed).map_err(|err| {
            ApiError::new(StatusCode::BAD_REQUEST, format!("invalid role JSON: {err}"))
        })?;
        let content = parsed
            .content
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "role JSON payload requires non-empty `content`",
                )
            })?;
        return Ok(content.to_string());
    }
    Ok(text.to_string())
}

/// `GET /v1/roles`: list all built-in and deployment/hydrated role templates.
async fn list_roles_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let data_dir = state.store.data_dir();
        create::hydrate_roles_from_store_if_postgres(&state.store, data_dir);
        let slugs = create::discover_available_archetype_slugs(None, None, data_dir);
        let mut roles = Vec::with_capacity(slugs.len());
        for slug in slugs {
            if let Ok(Some((tpl, source))) =
                create::resolve_archetype_template_with_source(&slug, None, &[], data_dir)
            {
                roles.push(build_role_view(data_dir, &slug, tpl, source, false));
            }
        }
        Ok(Json(serde_json::json!({
            "roles": roles,
            "count": roles.len(),
        })))
    })
    .await
}

/// `GET /v1/roles/{name}`: inspect one role template by slug.
async fn get_role_route(
    _principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<RoleView>, ApiError> {
    spawn_store(move || {
        let slug = name.trim().to_string();
        if !create::valid_template_stem(&slug) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("invalid role template name `{slug}`"),
            ));
        }
        let data_dir = state.store.data_dir();
        create::hydrate_roles_from_store_if_postgres(&state.store, data_dir);
        let Some((tpl, source)) =
            create::resolve_archetype_template_with_source(&slug, None, &[], data_dir)
                .map_err(ApiError::from_store)?
        else {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("role template `{slug}` not found"),
            ));
        };
        Ok(Json(build_role_view(data_dir, &slug, tpl, source, true)))
    })
    .await
}

/// `PUT /v1/roles/{name}`: validate and persist a custom role template into
/// PostgreSQL `deployment_blobs(namespace = 'roles')` and local `<data>/roles/`.
async fn put_role_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<RoleView>, ApiError> {
    let raw = extract_role_markdown(&headers, &body)?;
    spawn_store(move || {
        let slug = name.trim().to_string();
        require_admin_or_operator(&state.store, &principal, "roles.put", &slug)?;
        if !create::valid_template_stem(&slug) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("invalid role template name `{slug}`; expected ASCII letters, digits, `-`, or `_`"),
            ));
        }
        let tpl = create::parse_archetype_markdown(&raw)
            .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
        let filename = format!("{slug}.md");
        let normalized = if raw.ends_with('\n') {
            raw
        } else {
            format!("{raw}\n")
        };
        if state.store.is_postgres() {
            state
                .store
                .blob_store()
                .put_deployment_blob("roles", &filename, normalized.as_bytes(), false)
                .map_err(ApiError::from_store)?;
        }
        let dest = state.store.data_dir().join("roles").join(&filename);
        anyagents_harness::atomic_write_nonempty_file(&dest, normalized.as_bytes(), 0o644)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        Ok(Json(build_role_view(
            state.store.data_dir(),
            &slug,
            tpl,
            create::ArchetypeSource::Disk,
            true,
        )))
    })
    .await
}

/// `DELETE /v1/roles/{name}`: remove a custom role template from PostgreSQL
/// `deployment_blobs(namespace = 'roles')` and local `<data>/roles/`.
async fn delete_role_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let slug = name.trim().to_string();
        require_admin_or_operator(&state.store, &principal, "roles.delete", &slug)?;
        if !create::valid_template_stem(&slug) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("invalid role template name `{slug}`"),
            ));
        }
        let filename = format!("{slug}.md");
        let mut deleted = false;
        if state.store.is_postgres() {
            deleted |= state
                .store
                .blob_store()
                .delete_deployment_blob("roles", &filename)
                .map_err(ApiError::from_store)?;
        }
        let dest = state.store.data_dir().join("roles").join(&filename);
        match std::fs::remove_file(&dest) {
            Ok(()) => deleted = true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                ));
            }
        }
        Ok(Json(serde_json::json!({
            "name": slug,
            "deleted": deleted,
        })))
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyagents_core::Store;
    use anyagents_supervisor::HealthBoard;

    use super::super::ProcessRunner;
    use super::*;
    use crate::auth;
    use crate::identity::ServingIdentity;

    fn test_state(data: &std::path::Path) -> AppState {
        let store = Arc::new(Store::open(data.join("state.db")).unwrap());
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            auth: auth::Mode::Trusted {
                data_dir: data.to_path_buf(),
            },
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
        }
    }

    #[tokio::test]
    async fn roles_api_crud_hydrates_custom_role_and_fails_closed_on_unknown_role() {
        let dir = crate::test_tempdir();
        let state = test_state(dir.path());

        // 1. Unknown explicit role fails closed even when no custom roles exist yet.
        let mut unknown_req: crate::api::CreateAgentRequest =
            serde_json::from_value(serde_json::json!({"name": "bad-spawn", "role": "auditor"}))
                .unwrap();
        let err = create::apply_archetype_defaults(
            Some(&state.store),
            dir.path(),
            None,
            &mut unknown_req,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown role 'auditor'"), "{err}");
        assert!(err.contains("available roles: [general]"), "{err}");

        // 2. Non-admin agent cannot upload a role template.
        let non_admin = state
            .store
            .create_agent(anyagents_core::NewAgent::new("worker", "codex"))
            .unwrap();
        let forbidden = put_role_route(
            Principal::Agent {
                agent_id: non_admin.id.clone(),
                name: non_admin.name.clone(),
            },
            State(state.clone()),
            AxumPath("auditor".into()),
            HeaderMap::new(),
            Bytes::from_static(b"---\ndescription: Unauthorized\n---\nNo."),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);

        // 3. Operator uploads a custom role template via JSON payload.
        let md = "---\nname: auditor\ndescription: Security auditor\nscope: private\n---\nReview every decision for compliance and risk.\n";
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        let Json(created) = put_role_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("auditor".into()),
            headers,
            Bytes::from(serde_json::json!({"content": md}).to_string()),
        )
        .await
        .unwrap();
        assert_eq!(created.name, "auditor");
        assert_eq!(created.description.as_deref(), Some("Security auditor"));
        assert_eq!(
            created.body,
            "Review every decision for compliance and risk."
        );
        assert!(dir.path().join("roles/auditor.md").is_file());

        // 4. GET /v1/roles and GET /v1/roles/auditor expose the uploaded role.
        let Json(listed) = list_roles_route(Principal::Operator, State(state.clone()))
            .await
            .unwrap();
        let names: Vec<&str> = listed["roles"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v["name"].as_str())
            .collect();
        assert_eq!(names, vec!["general", "auditor"]);

        let Json(fetched) = get_role_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("auditor".into()),
        )
        .await
        .unwrap();
        assert_eq!(
            fetched.body,
            "Review every decision for compliance and risk."
        );

        // 5. Spawning with `role: "auditor"` resolves and injects verbatim template instructions.
        let mut spawn_req: crate::api::CreateAgentRequest =
            serde_json::from_value(serde_json::json!({"name": "advisor-1", "role": "auditor"}))
                .unwrap();
        create::apply_archetype_defaults(Some(&state.store), dir.path(), None, &mut spawn_req)
            .unwrap();
        assert_eq!(spawn_req.role.as_deref(), Some("auditor"));
        assert_eq!(spawn_req.description.as_deref(), Some("Security auditor"));
        let mut preview = non_admin.clone();
        preview.role = spawn_req.role.clone();
        preview.instructions = spawn_req.instructions.clone();
        let effective =
            create::effective_agent_instructions(Some(&state.store), dir.path(), &preview).unwrap();
        assert!(
            effective.contains("Review every decision for compliance and risk."),
            "{effective}"
        );

        // 6. Malformed frontmatter is rejected with 400 Bad Request.
        let malformed = put_role_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("broken".into()),
            HeaderMap::new(),
            Bytes::from_static(b"---\nunknown_key: bad\n---\nBody."),
        )
        .await
        .unwrap_err();
        assert_eq!(malformed.status, StatusCode::BAD_REQUEST);

        // 7. DELETE /v1/roles/auditor removes it and subsequent spawn fails closed again.
        let Json(del) = delete_role_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("auditor".into()),
        )
        .await
        .unwrap();
        assert_eq!(del["deleted"], true);
        assert!(!dir.path().join("roles/auditor.md").exists());

        let mut after_del_req: crate::api::CreateAgentRequest =
            serde_json::from_value(serde_json::json!({"name": "bad-spawn-2", "role": "auditor"}))
                .unwrap();
        let err_after = create::apply_archetype_defaults(
            Some(&state.store),
            dir.path(),
            None,
            &mut after_del_req,
        )
        .unwrap_err()
        .to_string();
        assert!(err_after.contains("unknown role 'auditor'"), "{err_after}");
    }
}

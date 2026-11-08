//! Fail-closed boundaries shared by local execution and cloud transfer.
use serde_json::Value;
use std::io::Write;
use std::path::Path;

pub(crate) fn read_optional(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("reading {}: {e}", path.display())),
    }
}
pub(crate) fn read_json(path: &Path) -> Result<Option<Value>, String> {
    read_optional(path)?
        .map(|raw| {
            let value: Value = serde_json::from_str(&raw)
                .map_err(|e| format!("invalid {}: {e}", path.display()))?;
            if !value.is_object() {
                return Err(format!("{} must contain an object", path.display()));
            }
            Ok(value)
        })
        .transpose()
}
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("missing parent directory")?;
    let mut file = tempfile::NamedTempFile::new_in(parent)
        .map_err(|e| format!("staging {}: {e}", path.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.as_file().sync_all())
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
    file.persist(path)
        .map_err(|e| format!("persisting {}: {e}", path.display()))?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("syncing {}: {e}", parent.display()))
}
pub(crate) fn remove_optional(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("removing {}: {e}", path.display())),
    }
}
pub(crate) fn required_text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("missing or invalid `{key}`"))
}
pub(crate) fn tool_arguments(raw: &str) -> Result<Value, String> {
    let value: Value =
        serde_json::from_str(raw).map_err(|e| format!("invalid tool arguments: {e}"))?;
    if !value.is_object() {
        return Err("tool arguments must be an object".into());
    }
    Ok(value)
}
pub(crate) fn sequence(value: Option<&Value>, allow_zero: bool) -> Result<i64, String> {
    let n = value
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .filter(|n| *n >= if allow_zero { 0 } else { 1 });
    n.ok_or_else(|| "missing or invalid cloud event sequence".into())
}
pub(crate) fn post_required(url: &str, key: &str, body: &Value) -> Result<(), String> {
    let (status, _) = crate::auth::post_cloud_json(url, Some(key), body)?;
    if !(200..300).contains(&status) {
        return Err(format!("required cloud operation HTTP {status}: {url}"));
    }
    Ok(())
}
/// Only an acknowledged delivery to the exact retained address settles a reply.
pub(crate) fn reply_delivered(output: &str, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return false;
    };
    let Ok(receipt) = serde_json::from_str::<Value>(output) else {
        return false;
    };
    receipt
        .get("deliveries")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("to").and_then(Value::as_str) == Some(expected)
                    && item
                        .get("id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| !id.is_empty())
                    && item.get("error").is_none()
            })
        })
}

pub(crate) fn validate_schema(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    if let Some(kind) = schema.get("type") {
        let matches = |kind: &str| match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.is_i64() || value.is_u64(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => false,
        };
        let valid = kind.as_str().is_some_and(matches)
            || kind
                .as_array()
                .is_some_and(|kinds| kinds.iter().filter_map(Value::as_str).any(matches));
        if !valid {
            return Err(format!("invalid type for {path}"));
        }
    }
    if let Some(options) = schema.get("enum").and_then(Value::as_array)
        && !options.contains(value)
    {
        return Err(format!("invalid value for {path}"));
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    return Err(format!("missing {path}.{key}"));
                }
            }
        }
        for (key, value) in object {
            if let Some(child) = schema.get("properties").and_then(|p| p.get(key)) {
                validate_schema(value, child, &format!("{path}.{key}"))?;
            }
        }
    }
    if let Some(items) = value.as_array()
        && let Some(item_schema) = schema.get("items")
    {
        for item in items {
            validate_schema(item, item_schema, path)?;
        }
    }
    Ok(())
}

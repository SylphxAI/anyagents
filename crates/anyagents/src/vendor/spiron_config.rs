use std::path::Path;

pub(super) fn spiron_execution_mode_local_in_toml(data: &Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(data.join(crate::remote::CONFIG_FILE)) else {
        return false;
    };
    let Ok(val) = toml::from_str::<toml::Value>(&raw) else {
        return false;
    };
    let check_eq = |v: Option<&toml::Value>| {
        v.and_then(toml::Value::as_str)
            .is_some_and(|s| s.trim().eq_ignore_ascii_case("local"))
    };
    check_eq(val.get("execution_mode"))
        || check_eq(val.get("spiron").and_then(|v| v.get("execution_mode")))
        || check_eq(
            val.get("harness")
                .and_then(|v| v.get("spiron"))
                .and_then(|v| v.get("execution_mode")),
        )
        || check_eq(val.get("server").and_then(|v| v.get("execution_mode")))
        || check_eq(
            val.get("server")
                .and_then(|v| v.get("spiron_execution_mode")),
        )
}

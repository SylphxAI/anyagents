//! Agent Skill (`SKILL.md`) frontmatter parser, discovery over [`WorkspaceBackend`],
//! and XML catalog rendering.

use serde::{Deserialize, Serialize};

use crate::WorkspaceBackend;

/// Summary of a discovered `SKILL.md` package.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSummary {
    pub name: String,
    pub description: String,
    pub path: String,
}

/// Parses YAML frontmatter delimited by `---` from a `SKILL.md` document.
/// Returns `Some((name, description))` when both fields are present and non-empty.
pub fn parse_skill_frontmatter(content: &str) -> Option<(String, String)> {
    let trimmed = content.trim_start_matches('\u{feff}').trim_start();
    let rest = trimmed
        .strip_prefix("---\n")
        .or_else(|| trimmed.strip_prefix("---\r\n"))?;
    let end_idx = rest.find("\n---")?;
    let yaml_block = &rest[..end_idx];

    let mut name = String::new();
    let mut description = String::new();
    let lines: Vec<&str> = yaml_block.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() || trimmed_line.starts_with('#') {
            i += 1;
            continue;
        }
        if let Some((raw_key, raw_val)) = trimmed_line.split_once(':') {
            let key = raw_key.trim();
            let val = clean_yaml_scalar(raw_val.trim());
            if key == "name" && !val.is_empty() {
                name = val;
            } else if key == "description" {
                if matches!(raw_val.trim(), "|" | ">" | "|-" | ">-") {
                    let mut multiline = Vec::new();
                    i += 1;
                    while i < lines.len() {
                        let next_line = lines[i];
                        if next_line.starts_with(' ') || next_line.starts_with('\t') {
                            let part = next_line.trim();
                            if !part.is_empty() {
                                multiline.push(part);
                            }
                            i += 1;
                        } else if next_line.trim().is_empty() {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    description = multiline.join(" ");
                    continue;
                } else if !val.is_empty() {
                    description = val;
                }
            }
        }
        i += 1;
    }

    if name.is_empty() || description.is_empty() {
        None
    } else {
        Some((name, description))
    }
}

fn clean_yaml_scalar(raw: &str) -> String {
    let s = raw.trim();
    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
    {
        s[1..s.len() - 1].trim().to_string()
    } else {
        s.to_string()
    }
}

/// Discovers `SKILL.md` skills inside `skills_dir` across either `LocalHost` or `RemoteSandbox`.
pub fn discover_skills(
    backend: &WorkspaceBackend,
    skills_dir: &str,
) -> Result<Vec<SkillSummary>, String> {
    let listing = match backend.list_dir(Some(skills_dir), false, 250) {
        Ok(val) => val,
        Err(_) => return Ok(Vec::new()),
    };
    let Some(entries) = listing.get("entries").and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };

    let base = skills_dir.trim_end_matches('/');
    let mut skills = Vec::new();
    for entry in entries {
        let Some(entry_name) = entry.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let kind = entry.get("kind").and_then(|v| v.as_str()).unwrap_or("dir");
        if kind != "dir" && kind != "symlink" {
            continue;
        }
        let candidate_rel = if base.is_empty() {
            format!("{entry_name}/SKILL.md")
        } else {
            format!("{base}/{entry_name}/SKILL.md")
        };
        let Ok((resolved_path, bytes)) = backend.read_file_bytes(Some(&candidate_rel)) else {
            continue;
        };
        let content = String::from_utf8_lossy(&bytes);
        if let Some((name, description)) = parse_skill_frontmatter(&content) {
            skills.push(SkillSummary {
                name,
                description,
                path: resolved_path,
            });
        }
    }
    skills.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(skills)
}

/// Renders discovered skills into an `<available_skills>` XML block suitable for system prompts.
pub fn render_available_skills_xml(skills: &[SkillSummary]) -> String {
    let mut out = String::from("<available_skills>\n");
    for skill in skills {
        out.push_str("  <skill>\n");
        out.push_str(&format!("    <name>{}</name>\n", escape_xml(&skill.name)));
        out.push_str(&format!(
            "    <description>{}</description>\n",
            escape_xml(&skill.description)
        ));
        out.push_str(&format!(
            "    <location>{}</location>\n",
            escape_xml(&skill.path)
        ));
        out.push_str("  </skill>\n");
    }
    out.push_str("</available_skills>");
    out
}

fn escape_xml(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_frontmatter_discovers_skills_and_renders_xml() {
        let dir = tempfile::tempdir().unwrap();
        let backend = WorkspaceBackend::LocalHost {
            root: dir.path().to_path_buf(),
        };
        backend
            .write_file_bytes(
                Some("skills/verify-build/SKILL.md"),
                b"---\nname: verify-build\ndescription: \"Run compiler and test suite verification\"\n---\n# Body\n",
                false,
                true,
            )
            .unwrap();
        backend
            .write_file_bytes(
                Some("skills/audit-deps/SKILL.md"),
                b"---\nname: audit-deps\ndescription: >\n  Inspect dependency tree\n  for advisories.\n---\n# Body\n",
                false,
                true,
            )
            .unwrap();

        let discovered = discover_skills(&backend, "skills").unwrap();
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].name, "audit-deps");
        assert_eq!(
            discovered[0].description,
            "Inspect dependency tree for advisories."
        );
        assert_eq!(discovered[1].name, "verify-build");

        let xml = render_available_skills_xml(&discovered);
        assert!(xml.contains("<available_skills>"));
        assert!(xml.contains("<name>verify-build</name>"));
        assert!(
            xml.contains("<description>Run compiler and test suite verification</description>")
        );
    }
}

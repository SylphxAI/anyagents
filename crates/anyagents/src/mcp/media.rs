//! Multimedia MCP content blocks, MIME sniffing, image dimension parsing, and attachment reading.

use super::*;

#[derive(Debug, Clone)]
pub struct MultimediaJson(pub serde_json::Value, pub Vec<rmcp::model::ContentBlock>);

impl rmcp::handler::server::tool::IntoCallToolResult for MultimediaJson {
    /// The JSON as text, then each media block. With media the JSON is not
    /// also sent as `structuredContent`: a client that prefers structured
    /// content gives its model that alone and drops the media (Codex does),
    /// and no tool that returns media declares an output schema that would
    /// require it.
    fn into_call_tool_result(self) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
        let mut res = rmcp::model::CallToolResult::structured(self.0);
        if !self.1.is_empty() {
            res.structured_content = None;
            res.content.extend(self.1);
        }
        Ok(res.into())
    }
}

pub(super) const MAX_MULTIMEDIA_READ_BYTES: usize = 20 * 1024 * 1024;

pub(super) fn detect_multimedia_mime(path: &Path, bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some("image/jpeg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    if bytes.starts_with(b"%PDF-") {
        return Some("application/pdf");
    }
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return Some("audio/wav");
    }
    if bytes.starts_with(b"ID3") {
        return Some("audio/mpeg");
    }
    if bytes.starts_with(b"OggS") {
        return Some("audio/ogg");
    }
    if bytes.starts_with(b"fLaC") {
        return Some("audio/flac");
    }
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        let brand = &bytes[8..12];
        if brand == b"avif" || brand == b"avis" {
            return Some("image/avif");
        }
        if brand == b"M4A " || brand == b"M4B " {
            return Some("audio/mp4");
        }
        if brand == b"qt  " {
            return Some("video/quicktime");
        }
        return match ext.as_deref() {
            Some("m4a") => Some("audio/mp4"),
            Some("mov") => Some("video/quicktime"),
            Some("avif") => Some("image/avif"),
            _ => Some("video/mp4"),
        };
    }
    if bytes.starts_with(b"\x1a\x45\xdf\xa3") {
        return match ext.as_deref() {
            Some("webm") => Some("video/webm"),
            _ => Some("video/x-matroska"),
        };
    }
    match ext.as_deref() {
        Some("png") => Some("image/png"),
        Some("jpg" | "jpeg") => Some("image/jpeg"),
        Some("gif") => Some("image/gif"),
        Some("webp") => Some("image/webp"),
        Some("bmp") => Some("image/bmp"),
        Some("avif") => Some("image/avif"),
        Some("ico") => Some("image/x-icon"),
        Some("pdf") => Some("application/pdf"),
        Some("wav" | "wave") => Some("audio/wav"),
        Some("mp3") => Some("audio/mpeg"),
        Some("ogg" | "oga" | "opus") => Some("audio/ogg"),
        Some("flac") => Some("audio/flac"),
        Some("m4a") => Some("audio/mp4"),
        Some("aac") => Some("audio/aac"),
        Some("mp4" | "m4v") => Some("video/mp4"),
        Some("webm") => Some("video/webm"),
        Some("mov") => Some("video/quicktime"),
        _ => None,
    }
}

pub(super) fn parse_image_dimensions(mime_type: &str, bytes: &[u8]) -> Option<(u32, u32)> {
    match mime_type {
        "image/png" => crate::computer::parse_png_dimensions(bytes),
        "image/gif" => {
            if bytes.len() >= 10 && (&bytes[0..6] == b"GIF87a" || &bytes[0..6] == b"GIF89a") {
                let w = u32::from(u16::from_le_bytes([bytes[6], bytes[7]]));
                let h = u32::from(u16::from_le_bytes([bytes[8], bytes[9]]));
                Some((w, h))
            } else {
                None
            }
        }
        "image/bmp" => {
            if bytes.len() >= 26 && &bytes[0..2] == b"BM" {
                let w =
                    i32::from_le_bytes([bytes[18], bytes[19], bytes[20], bytes[21]]).unsigned_abs();
                let h =
                    i32::from_le_bytes([bytes[22], bytes[23], bytes[24], bytes[25]]).unsigned_abs();
                Some((w, h))
            } else {
                None
            }
        }
        "image/webp" => {
            if bytes.len() >= 30 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
                if &bytes[12..16] == b"VP8 " {
                    let w = u32::from(u16::from_le_bytes([bytes[26], bytes[27]]) & 0x3fff);
                    let h = u32::from(u16::from_le_bytes([bytes[28], bytes[29]]) & 0x3fff);
                    return Some((w, h));
                } else if &bytes[12..16] == b"VP8L" {
                    let b0 = u32::from(bytes[21]);
                    let b1 = u32::from(bytes[22]);
                    let b2 = u32::from(bytes[23]);
                    let b3 = u32::from(bytes[24]);
                    let w = 1 + (((b1 & 0x3f) << 8) | b0);
                    let h = 1 + (((b3 & 0x0f) << 10) | (b2 << 2) | ((b1 & 0xc0) >> 6));
                    return Some((w, h));
                } else if &bytes[12..16] == b"VP8X" {
                    let w = 1
                        + (u32::from(bytes[24])
                            | (u32::from(bytes[25]) << 8)
                            | (u32::from(bytes[26]) << 16));
                    let h = 1
                        + (u32::from(bytes[27])
                            | (u32::from(bytes[28]) << 8)
                            | (u32::from(bytes[29]) << 16));
                    return Some((w, h));
                }
            }
            None
        }
        "image/jpeg" => {
            if bytes.len() < 4 || !bytes.starts_with(b"\xff\xd8") {
                return None;
            }
            let mut pos = 2usize;
            while pos + 1 < bytes.len() {
                if bytes[pos] != 0xff {
                    pos += 1;
                    continue;
                }
                while pos < bytes.len() && bytes[pos] == 0xff {
                    pos += 1;
                }
                if pos >= bytes.len() {
                    break;
                }
                let marker = bytes[pos];
                pos += 1;
                if marker == 0xd8
                    || marker == 0xd9
                    || marker == 0x01
                    || (0xd0..=0xd7).contains(&marker)
                {
                    continue;
                }
                if pos + 2 > bytes.len() {
                    break;
                }
                let seg_len = usize::from(u16::from_be_bytes([bytes[pos], bytes[pos + 1]]));
                if seg_len < 2 || pos + seg_len > bytes.len() {
                    break;
                }
                if matches!(
                    marker,
                    0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf
                ) && seg_len >= 7
                {
                    let h = u32::from(u16::from_be_bytes([bytes[pos + 3], bytes[pos + 4]]));
                    let w = u32::from(u16::from_be_bytes([bytes[pos + 5], bytes[pos + 6]]));
                    return Some((w, h));
                }
                pos += seg_len;
            }
            None
        }
        _ => None,
    }
}

pub(super) struct ScopedTempDir(PathBuf);

impl ScopedTempDir {
    pub(super) fn new() -> std::io::Result<Self> {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "anyagents-media-{}-{}-{}",
            std::process::id(),
            anyagents_core::now_ms(),
            seq
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    pub(super) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScopedTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn transcode_image_to_png_bytes(path: &Path) -> Option<Vec<u8>> {
    let dir = ScopedTempDir::new().ok()?;
    let out_png = dir.path().join("converted.png");
    let convert = anyagents_toolchain::tool("convert", "mcp.read").ok()?;
    let status = convert
        .command()
        .arg(format!("{}[0]", path.display()))
        .arg(&out_png)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()?;
    if status.success() {
        std::fs::read(&out_png).ok()
    } else {
        None
    }
}

pub(super) fn extract_media_blocks_from_tool_value(
    tool_name: &str,
    val: &mut serde_json::Value,
) -> Vec<rmcp::model::ContentBlock> {
    let mut blocks = Vec::new();
    let Some(obj) = val.as_object_mut() else {
        return blocks;
    };

    // 1. Direct `image_base64` (computer.screenshot, computer.zoom, device.screenshot, browser.screenshot)
    if let Some(serde_json::Value::String(b64)) = obj.remove("image_base64")
        && !b64.is_empty()
    {
        let mime = obj
            .get("mime_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("image/png")
            .to_string();
        blocks.push(rmcp::model::ContentBlock::image(b64, mime));
        obj.insert("image_attached".to_string(), serde_json::Value::Bool(true));
    }

    // 2. Browser screenshot path fallback (e.g. takeover screenshot_path or local path)
    if blocks.is_empty() && tool_name == "browser" {
        for key in ["screenshot_path", "path"] {
            if let Some(p_str) = obj.get(key).and_then(serde_json::Value::as_str) {
                let p = Path::new(p_str);
                if p.is_file()
                    && let Ok(bytes) = std::fs::read(p)
                    && let Some(mime) = detect_multimedia_mime(p, &bytes)
                    && mime.starts_with("image/")
                {
                    let b64 = crate::computer::base64_encode(&bytes);
                    blocks.push(rmcp::model::ContentBlock::image(b64, mime));
                    obj.insert("image_attached".to_string(), serde_json::Value::Bool(true));
                    break;
                }
            }
        }
    }

    // 3. `audio_base64` (media.speech)
    if let Some(serde_json::Value::String(b64)) = obj.remove("audio_base64")
        && !b64.is_empty()
    {
        let fmt = obj
            .get("response_format")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("mp3");
        let mime = match fmt.to_ascii_lowercase().as_str() {
            "wav" | "wave" => "audio/wav",
            "ogg" | "opus" => "audio/ogg",
            "flac" => "audio/flac",
            "aac" => "audio/aac",
            "m4a" | "mp4" => "audio/mp4",
            _ => "audio/mpeg",
        };
        blocks.push(rmcp::model::ContentBlock::audio(b64, mime));
        obj.insert("audio_attached".to_string(), serde_json::Value::Bool(true));
    }

    // 4. `data: [{"b64_json": ...}]` (media.image_generate)
    if let Some(serde_json::Value::Array(items)) = obj.get_mut("data") {
        for item in items.iter_mut() {
            if let Some(item_obj) = item.as_object_mut()
                && let Some(serde_json::Value::String(b64)) = item_obj.remove("b64_json")
                && !b64.is_empty()
            {
                blocks.push(rmcp::model::ContentBlock::image(b64, "image/png"));
                item_obj.insert("image_attached".to_string(), serde_json::Value::Bool(true));
            }
        }
    }

    blocks
}

pub(super) fn video_attachment(
    receipt: &serde_json::Value,
    bytes: &[u8],
) -> Option<rmcp::model::ContentBlock> {
    if bytes.len() > MAX_MULTIMEDIA_READ_BYTES {
        return None;
    }
    let path = receipt.get("path")?.as_str()?;
    let uri = url::Url::from_file_path(path).ok()?.to_string();
    let mime_type = receipt.get("mime_type")?.as_str()?.to_owned();
    Some(rmcp::model::ContentBlock::resource(
        rmcp::model::ResourceContents::BlobResourceContents {
            uri,
            mime_type: Some(mime_type),
            blob: crate::computer::base64_encode(bytes),
            meta: None,
        },
    ))
}

pub(super) fn read_multimedia_file(
    resolved: &Path,
    full_bytes: &[u8],
    mime_type: &'static str,
    sha256: String,
    args: &FilesArgs,
) -> Result<(serde_json::Value, Vec<rmcp::model::ContentBlock>), String> {
    let total_bytes = full_bytes.len();
    if total_bytes > MAX_MULTIMEDIA_READ_BYTES {
        return Err(format!(
            "multimedia file {} is {} bytes, exceeding maximum of {} bytes",
            resolved.display(),
            total_bytes,
            MAX_MULTIMEDIA_READ_BYTES
        ));
    }

    let mut media_blocks = Vec::new();
    let uri = format!("file://{}", resolved.display());

    if mime_type.starts_with("image/") {
        let dims = parse_image_dimensions(mime_type, full_bytes);
        // What a model is given: within every provider's limits, scaled or
        // converted here when it is not (a format only the toolchain's
        // converter reads goes through it first).
        let prepared = anyagents_harness::media::model_image(full_bytes).or_else(|err| {
            transcode_image_to_png_bytes(resolved)
                .ok_or(err)
                .and_then(|png| anyagents_harness::media::model_image(&png))
        });
        let (width, height) = dims.unwrap_or((0, 0));
        let dim_str = if width > 0 && height > 0 {
            format!("{width}x{height}, ")
        } else {
            String::new()
        };
        let content = match &prepared {
            Ok(image) if image.converted => format!(
                "[Native {mime_type} image ({dim_str}{total_bytes} bytes) attached as MCP image content block, as {} {}x{} ({} bytes)]",
                image.mime,
                image.width,
                image.height,
                image.data.len()
            ),
            Ok(_) => format!(
                "[Native {mime_type} image ({dim_str}{total_bytes} bytes) attached as MCP image content block]"
            ),
            Err(err) => {
                format!("[{mime_type} image ({dim_str}{total_bytes} bytes) not attached: {err}]")
            }
        };
        if let Ok(image) = &prepared {
            media_blocks.push(rmcp::model::ContentBlock::image(image.base64(), image.mime));
        }
        return Ok((
            serde_json::json!({
                "action": "read",
                "path": resolved.display().to_string(),
                "mime_type": mime_type,
                "media_kind": "image",
                "width": width,
                "height": height,
                "bytes_read": total_bytes,
                "total_bytes": total_bytes,
                "truncated": false,
                "sha256": sha256,
                "image_attached": prepared.is_ok(),
                "content": content,
            }),
            media_blocks,
        ));
    }

    if mime_type == "application/pdf" {
        let b64_pdf = crate::computer::base64_encode(full_bytes);
        media_blocks.push(rmcp::model::ContentBlock::resource(
            rmcp::model::ResourceContents::BlobResourceContents {
                uri,
                mime_type: Some("application/pdf".to_string()),
                blob: b64_pdf,
                meta: None,
            },
        ));

        let start_page = args.start_line.unwrap_or(1).max(1);
        let end_page = args
            .end_line
            .unwrap_or(start_page + 4)
            .max(start_page)
            .min(start_page + 9);

        let extracted_text = anyagents_toolchain::tool("pdftotext", "mcp.read")
            .ok()
            .and_then(|tool| {
                tool.command()
                    .args([
                        "-f",
                        &start_page.to_string(),
                        "-l",
                        &end_page.to_string(),
                        &resolved.display().to_string(),
                        "-",
                    ])
                    .output()
                    .ok()
            })
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .unwrap_or_default();

        let mut pages_rendered = 0usize;
        if let Ok(tmp_dir) = ScopedTempDir::new() {
            let prefix = tmp_dir.path().join("page");
            if let Ok(pdftoppm) = anyagents_toolchain::tool("pdftoppm", "mcp.read")
                && let Ok(status) = pdftoppm
                    .command()
                    .args([
                        "-jpeg",
                        "-r",
                        "100",
                        "-f",
                        &start_page.to_string(),
                        "-l",
                        &end_page.to_string(),
                        &resolved.display().to_string(),
                        &prefix.display().to_string(),
                    ])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                && status.success()
                && let Ok(entries) = std::fs::read_dir(tmp_dir.path())
            {
                let mut jpg_paths: Vec<PathBuf> = entries
                    .filter_map(Result::ok)
                    .map(|e| e.path())
                    .filter(|p| {
                        p.extension()
                            .and_then(|e| e.to_str())
                            .is_some_and(|e| e.eq_ignore_ascii_case("jpg"))
                    })
                    .collect();
                jpg_paths.sort();
                for jpg_path in jpg_paths {
                    if let Ok(jpg_bytes) = std::fs::read(&jpg_path) {
                        media_blocks.push(rmcp::model::ContentBlock::image(
                            crate::computer::base64_encode(&jpg_bytes),
                            "image/jpeg",
                        ));
                        pages_rendered += 1;
                    }
                }
            }
        }

        let max_bytes = args
            .max_bytes
            .unwrap_or(DEFAULT_READ_MAX_BYTES)
            .clamp(1, HARD_READ_MAX_BYTES);
        let text_end = floor_char_boundary_str(&extracted_text, max_bytes);
        let text_slice = &extracted_text[..text_end];
        let content_summary = if text_slice.is_empty() {
            format!(
                "[Native application/pdf ({total_bytes} bytes, pages {start_page}-{end_page}, {pages_rendered} page image(s)) attached as MCP content blocks]"
            )
        } else {
            text_slice.to_string()
        };

        return Ok((
            serde_json::json!({
                "action": "read",
                "path": resolved.display().to_string(),
                "mime_type": "application/pdf",
                "media_kind": "pdf",
                "start_page": start_page,
                "end_page": end_page,
                "pages_rendered": pages_rendered,
                "bytes_read": total_bytes,
                "total_bytes": total_bytes,
                "truncated": text_end < extracted_text.len(),
                "sha256": sha256,
                "resource_attached": true,
                "content": content_summary,
            }),
            media_blocks,
        ));
    }

    if mime_type.starts_with("audio/") {
        let b64 = crate::computer::base64_encode(full_bytes);
        media_blocks.push(rmcp::model::ContentBlock::audio(b64, mime_type));
        return Ok((
            serde_json::json!({
                "action": "read",
                "path": resolved.display().to_string(),
                "mime_type": mime_type,
                "media_kind": "audio",
                "bytes_read": total_bytes,
                "total_bytes": total_bytes,
                "truncated": false,
                "sha256": sha256,
                "audio_attached": true,
                "content": format!(
                    "[Native {mime_type} audio ({total_bytes} bytes) attached as MCP audio content block]"
                ),
            }),
            media_blocks,
        ));
    }

    let b64 = crate::computer::base64_encode(full_bytes);
    media_blocks.push(rmcp::model::ContentBlock::resource(
        rmcp::model::ResourceContents::BlobResourceContents {
            uri,
            mime_type: Some(mime_type.to_string()),
            blob: b64,
            meta: None,
        },
    ));
    let media_kind = if mime_type.starts_with("video/") {
        "video"
    } else {
        "binary"
    };
    Ok((
        serde_json::json!({
            "action": "read",
            "path": resolved.display().to_string(),
            "mime_type": mime_type,
            "media_kind": media_kind,
            "bytes_read": total_bytes,
            "total_bytes": total_bytes,
            "truncated": false,
            "sha256": sha256,
            "resource_attached": true,
            "content": format!(
                "[Native {mime_type} {media_kind} ({total_bytes} bytes) attached as MCP resource content block]"
            ),
        }),
        media_blocks,
    ))
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

pub(super) fn floor_char_boundary_str(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

//! Stdio MCP frame size capping and artifact offloading.

use super::*;

pub(crate) const MCP_TEXT_SPILL_BYTES: usize = 56 * 1024;

pub(super) fn agent_spill_dir(config: &Path, agent: &AgentId) -> PathBuf {
    match anyagents_core::configured_scratch_dir() {
        Some(scratch) => anyagents_core::agent_scratch_tmp_dir(&scratch, agent),
        None => config.join("tmp"),
    }
}

/// Exclusive, private artifacts. Compact JSON without a trailing newline is
/// also byte-exact through workspace.read's text pagination.
pub(super) fn spill_file(
    prefix: &str,
    bytes: &[u8],
    dir: &Path,
    seq: u64,
) -> std::io::Result<PathBuf> {
    use std::io::Write as _;
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(dir)?;
    let dir = std::fs::canonicalize(dir)?;
    let path = dir.join(format!("{prefix}-{}-{seq}.json", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(path)
}

pub(super) fn artifact_info(path: &Path, bytes: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "path": path, "bytes": bytes.len(), "sha256": sha256_hex(bytes),
        "format": "json", "mode": "0600",
        "read": "Read the JSON file at `path` directly and verify `bytes` and `sha256` before parsing."
    })
}

/// Preserve the complete envelope, including errors and structured content.
/// The caller falls back to the original frame if artifact creation fails.
pub(super) fn capped_frame(frame: &[u8], dir: &Path, seq: u64) -> std::io::Result<Vec<u8>> {
    let original: serde_json::Value = serde_json::from_slice(frame)?;
    let path = spill_file("anyagents-mcp-stdio-frame", frame, dir, seq)?;
    let artifact = artifact_info(&path, frame);
    let text = serde_json::to_string(&serde_json::json!({"offloaded":true,"artifact":artifact}))?;
    let mut replacement = serde_json::json!({"jsonrpc":"2.0","id":original.get("id")});
    if original.get("error").is_some() {
        replacement["error"] = serde_json::json!({
            "code": original["error"]["code"],
            "message": "Oversized JSON-RPC error preserved in artifact", "data": artifact
        });
    } else {
        replacement["result"] = serde_json::json!({
            "content":[{"type":"text","text":text}],
            "isError": original["result"]["isError"].as_bool().unwrap_or(false)
        });
    }
    let mut bytes = serde_json::to_vec(&replacement)?;
    bytes.push(b'\n');
    if bytes.len() > MCP_TEXT_SPILL_BYTES {
        return Err(std::io::Error::other(
            "artifact reference exceeds stdio frame limit; original preserved",
        ));
    }
    Ok(bytes)
}

// Media may be native MCP content or encoded inside code/structured results.
// Never replace either representation with a text-only artifact pointer.
pub(super) fn contains_media(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            map.get("type")
                .and_then(|v| v.as_str())
                .is_some_and(|t| matches!(t, "image" | "audio" | "resource" | "resource_link"))
                || map.contains_key("visionImage")
                || map.get("image_attached").and_then(|v| v.as_bool()) == Some(true)
                || (map.contains_key("mimeType") && map.contains_key("data"))
                || map.values().any(contains_media)
        }
        serde_json::Value::Array(values) => values.iter().any(contains_media),
        serde_json::Value::String(text) => serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .filter(|v| v.is_object() || v.is_array())
            .is_some_and(|v| contains_media(&v)),
        _ => false,
    }
}

/// Buffers partial stdio lines and offloads large textual responses.
/// Native MCP content must remain inline for the consumer's media decoder.
pub(crate) struct FrameArtifactStdout<W> {
    pub(super) inner: W,
    pub(super) pending: Vec<u8>,
    pub(super) out: Vec<u8>,
    pub(super) out_pos: usize,
    pub(super) spill_dir: PathBuf,
    pub(super) seq: u64,
}

impl<W> FrameArtifactStdout<W> {
    pub(super) fn with_spill_dir(inner: W, spill_dir: PathBuf) -> Self {
        Self {
            inner,
            pending: Vec::new(),
            out: Vec::new(),
            out_pos: 0,
            spill_dir,
            seq: 0,
        }
    }

    pub(super) fn transform_line(&mut self, line: &[u8]) {
        let frame = line.strip_suffix(b"\n").unwrap_or(line);
        let textual = serde_json::from_slice::<serde_json::Value>(frame)
            .ok()
            .is_some_and(|value| {
                !contains_media(&value)
                    && (value.get("error").is_some()
                        || value.get("result").is_some_and(|result| {
                            result
                                .get("content")
                                .and_then(serde_json::Value::as_array)
                                .is_none_or(|blocks| {
                                    blocks.iter().all(|block| block["type"] == "text")
                                })
                        }))
            });
        if frame.len() + 1 > MCP_TEXT_SPILL_BYTES && textual {
            self.seq += 1;
            if let Ok(replacement) = capped_frame(frame, &self.spill_dir, self.seq) {
                self.out.extend_from_slice(&replacement);
                return;
            }
            // Offloading is optional: never lose data or reject already accepted
            // bytes when storage is unavailable. No retry of tool execution.
        }
        self.out.extend_from_slice(line);
    }

    pub(super) fn absorb_pending_from(&mut self, mut scan_from: usize) {
        while let Some(rel) = self.pending[scan_from..]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            let pos = scan_from + rel;
            let line = self.pending[..=pos].to_vec();
            self.transform_line(&line);
            self.pending.drain(..=pos);
            scan_from = 0;
        }
    }
}

impl<W: tokio::io::AsyncWrite + Unpin> FrameArtifactStdout<W> {
    pub(super) fn drain_out(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        while self.out_pos < self.out.len() {
            match std::pin::Pin::new(&mut self.inner).poll_write(cx, &self.out[self.out_pos..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the stdio frame",
                    )));
                }
                Poll::Ready(Ok(written)) => {
                    self.out_pos += written;
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.out.clear();
        self.out_pos = 0;
        Poll::Ready(Ok(()))
    }
}

impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for FrameArtifactStdout<W> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        use std::task::Poll;
        if !this.out.is_empty() {
            match this.drain_out(cx) {
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
        }
        let scan_from = this.pending.len();
        this.pending.extend_from_slice(buf);
        this.absorb_pending_from(scan_from);
        // Transformed bytes are buffered; the caller reports the input as
        // consumed and the next write/flush drains the queue.
        // Drain on the next write/flush so no post-acceptance error is hidden.
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        use std::task::Poll;
        if !this.pending.is_empty() {
            let line = this.pending.clone();
            this.transform_line(&line);
            this.pending.clear();
        }
        match this.drain_out(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        std::pin::Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        use std::task::Poll;
        match std::pin::Pin::new(&mut *this).poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        std::pin::Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod stdio_frame_cap_tests {
    use super::*;
    use tokio::io::AsyncWriteExt as _;

    #[test]
    fn tools_list_frame_stays_below_48_kib() {
        let tools = tool_definitions();
        assert!(!tools.is_empty());
        let frame = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "tools": tools },
        }))
        .unwrap();
        assert!(
            frame.len() + 1 < 48 * 1024,
            "tools/list frame is {} bytes, over the 48 KiB budget",
            frame.len() + 1
        );
    }

    #[tokio::test]
    async fn rpc_error_and_structured_envelopes_preserve_bytes_and_status() {
        let dir = tempfile::tempdir().unwrap();
        let frames = [
            serde_json::json!({"jsonrpc":"2.0","id":"failure","error":{"code":-32001,"message":"\u{0001}é\"\\".repeat(100_000),"data":{"retry":false}}}),
            serde_json::json!({"jsonrpc":"2.0","id":43,"result":{"content":[{"type":"text","text":"\u{0001}".repeat(400_000)}],"isError":true,"structuredContent":{"nested":{"exact":true}}}}),
        ];
        let mut gate = FrameArtifactStdout::with_spill_dir(Vec::new(), dir.path().to_path_buf());
        for frame in &frames {
            let mut bytes = serde_json::to_vec(frame).unwrap();
            bytes.push(b'\n');
            gate.write_all(&bytes).await.unwrap();
        }
        gate.flush().await.unwrap();
        let references: Vec<serde_json::Value> = gate
            .inner
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                assert!(line.len() < MCP_TEXT_SPILL_BYTES);
                serde_json::from_slice(line).unwrap()
            })
            .collect();
        assert_eq!(references.len(), 2);
        assert_eq!(references[0]["error"]["code"], -32001);
        assert_eq!(references[1]["result"]["isError"], true);
        let artifacts = [
            references[0]["error"]["data"].clone(),
            serde_json::from_str::<serde_json::Value>(
                references[1]["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap()["artifact"]
                .clone(),
        ];
        assert_ne!(artifacts[0]["path"], artifacts[1]["path"]);
        for ((original, reference), artifact) in frames.iter().zip(&references).zip(&artifacts) {
            assert_eq!(reference["id"], original["id"]);
            let bytes = serde_json::to_vec(original).unwrap();
            let retained = std::fs::read(artifact["path"].as_str().unwrap()).unwrap();
            assert_eq!(retained, bytes);
            assert_eq!(artifact["bytes"].as_u64().unwrap(), bytes.len() as u64);
            assert_eq!(artifact["sha256"], sha256_hex(&bytes));
        }
    }

    #[tokio::test]
    async fn oversize_frame_spills_to_0600_file_and_stays_single_line() {
        // 128 KiB payload mixing multi-byte UTF-8 with quotes and backslashes,
        // so JSON string escaping inflates the wire size further.
        let payload = "é漢字\"\\".repeat(16 * 1024);
        let frame = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 42,
            "result": {
                "content": [{"type": "text", "text": payload}],
                "isError": false,
            }
        }))
        .unwrap();
        assert!(frame.len() > 128 * 1024, "frame is {} bytes", frame.len());

        let dir = tempfile::tempdir().unwrap();
        let mut gated = FrameArtifactStdout::with_spill_dir(Vec::new(), dir.path().to_path_buf());
        let mut wire = frame.clone();
        wire.push(b'\n');
        // Write in 8 KiB chunks so partial-line buffering is exercised.
        for chunk in wire.chunks(8 * 1024) {
            gated.write_all(chunk).await.unwrap();
        }
        gated.flush().await.unwrap();
        let out = gated.inner;

        assert!(
            out.len() <= MCP_TEXT_SPILL_BYTES,
            "capped frame is {} bytes",
            out.len()
        );
        assert!(out.len() < 64 * 1024);
        assert_eq!(out.iter().filter(|byte| **byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], 42);
        let summary = value["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(summary.contains("0600"), "summary names the spill mode");
        let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
        let spill_path = PathBuf::from(summary["artifact"]["path"].as_str().unwrap());
        assert_eq!(
            spill_path.parent(),
            Some(std::fs::canonicalize(dir.path()).unwrap().as_path())
        );
        let spilled = std::fs::read(&spill_path).unwrap();
        assert_eq!(spilled, frame, "spilled file must be byte-identical");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&spill_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn native_and_nested_media_pass_above_text_budget() {
        let mut image = image::RgbaImage::new(768, 768);
        let mut state = 1u32;
        for byte in image.as_flat_samples_mut().samples {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            *byte = state as u8;
        }
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(image)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        let png = png.into_inner();
        assert!(png.len() > 2_200_000);
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png);
        let media = serde_json::json!({"type":"image","mimeType":"image/png","data":encoded});
        let nested = serde_json::json!({"result":{"visionImage": media.clone()}});
        for content in [
            serde_json::json!([media.clone()]),
            serde_json::json!([{"type":"text","text":"caption"},media]),
            serde_json::json!([{"type":"text","text":nested.to_string()}]),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut gated = FrameArtifactStdout::with_spill_dir(Vec::new(), dir.path().into());
            let mut frame = serde_json::to_vec(
                &serde_json::json!({"jsonrpc":"2.0","id":9,"result":{"content":content}}),
            )
            .unwrap();
            frame.push(b'\n');
            for part in frame.chunks(1023) {
                gated.write_all(part).await.unwrap();
            }
            gated.flush().await.unwrap();
            assert_eq!(gated.inner, frame);
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn unavailable_storage_preserves_original_frame() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, "x").unwrap();
        let mut gated = FrameArtifactStdout::with_spill_dir(Vec::new(), file);
        let mut frame = serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"x".repeat(MCP_TEXT_SPILL_BYTES)}]}})).unwrap();
        frame.push(b'\n');
        gated.write_all(&frame).await.unwrap();
        gated.flush().await.unwrap();
        assert_eq!(gated.inner, frame);
    }

    #[derive(Default)]
    struct ShortSink {
        bytes: Vec<u8>,
        pending: bool,
        fail: Option<std::io::ErrorKind>,
        zero: bool,
    }

    impl tokio::io::AsyncWrite for ShortSink {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.pending {
                self.pending = false;
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            if let Some(kind) = self.fail {
                return std::task::Poll::Ready(Err(kind.into()));
            }
            if self.zero {
                return std::task::Poll::Ready(Ok(0));
            }
            let n = buf.len().min(7);
            self.bytes.extend_from_slice(&buf[..n]);
            self.pending = true;
            std::task::Poll::Ready(Ok(n))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn short_writes_pending_and_sink_failures_are_lossless() {
        let dir = tempfile::tempdir().unwrap();
        let frame = b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n";
        let mut gate = FrameArtifactStdout::with_spill_dir(ShortSink::default(), dir.path().into());
        gate.write_all(frame).await.unwrap();
        gate.write_all(frame).await.unwrap();
        gate.shutdown().await.unwrap();
        assert_eq!(
            gate.inner.bytes,
            [frame.as_slice(), frame.as_slice()].concat()
        );
        for (sink, kind) in [
            (
                ShortSink {
                    zero: true,
                    ..Default::default()
                },
                std::io::ErrorKind::WriteZero,
            ),
            (
                ShortSink {
                    fail: Some(std::io::ErrorKind::BrokenPipe),
                    ..Default::default()
                },
                std::io::ErrorKind::BrokenPipe,
            ),
        ] {
            let mut gate = FrameArtifactStdout::with_spill_dir(sink, dir.path().into());
            gate.write_all(frame).await.unwrap();
            assert_eq!(gate.flush().await.unwrap_err().kind(), kind);
            assert_eq!(gate.out[gate.out_pos..], frame[..]);
        }
    }

    #[tokio::test]
    async fn small_frame_passes_through_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let mut gated = FrameArtifactStdout::with_spill_dir(Vec::new(), dir.path().to_path_buf());
        gated
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n")
            .await
            .unwrap();
        gated.flush().await.unwrap();
        assert_eq!(
            gated.inner,
            b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n".to_vec()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn chunked_multi_mb_media_and_trailing_frame_boundaries_scan_linearly() {
        let dir = tempfile::tempdir().unwrap();
        let mut gated = FrameArtifactStdout::with_spill_dir(Vec::new(), dir.path().into());
        let media = serde_json::json!({
            "type": "image",
            "mimeType": "image/png",
            "data": "A".repeat(2_500_000)
        });
        let mut payload = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 11,
            "result": {"content": [media]}
        }))
        .unwrap();
        payload.push(b'\n');
        let tail = b"{\"jsonrpc\":\"2.0\",\"id\":12,\"result\":{\"ok\":true}}\n";
        payload.extend_from_slice(tail);

        let started = std::time::Instant::now();
        for part in payload.chunks(1023) {
            gated.write_all(part).await.unwrap();
        }
        gated.flush().await.unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "chunked frame buffering should stay O(N), took {:?}",
            started.elapsed()
        );
        assert_eq!(gated.inner, payload);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

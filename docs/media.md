# Media

A file an agent is sent stays a file. The message names it by its path, and
the agent decides whether to look at it, as a modern harness does with a
file in its workspace: an image nobody asks about costs no context and
misleads no model. This page says where the file lives, and, for each
harness, how an agent that chooses to look at an image gets it in front of
its model.

## A file in the mailbox

A channel that receives a file keeps the server's own copy directly inside
`<data>/media` and names it in the message body with one line
(`anyagents_harness::media::local_media_line`):

```
[Local media (photo) file: <data>/media/complete_photo_<id>.jpg]
```

That line is all the message carries. No adapter attaches the file to the
input it writes, idle or mid-turn; the conformance suite holds every adapter
to it (`a photo in the mailbox reaches the model as its path`,
[delivery.md](delivery.md#the-conformance-suite)). The directory is `0711`
(a seat opens a file it was told about and lists none) and every file `0644`,
whatever the server's umask (`telegram/channel.rs`).

## Looking at it

An agent reads the file with `code` (`workspace.read({ path })`), which
returns the image as an MCP `image` content block after the result's JSON
text block. The image is prepared for every provider the fleet reaches
(`anyagents_harness::media::model_image`): a PNG, JPEG, GIF or WebP whose
longest edge is at most 2,000 px and whose bytes are at most 3.9 MB (5 MiB
of base64, Anthropic's limit), and which decodes whole, goes as it is;
anything larger is scaled to fit, with its EXIF orientation applied;
another format is converted (BMP here, TIFF, HEIC and the like through the
toolchain's converter first); a file that does not decode is not attached,
and the text says why. A result that carries media does not also carry
`structuredContent`: Codex gives its model the structured content alone when
there is any, and drops every image.

## The table

How an agent that chooses to look at an image it was given gets it to its
model, read from each vendor's source or bundled code (never by running it).
`tests/invariants/fakes/mcp_client.py` states each vendor's rule for an MCP
tool result, with its source, and
`an_image_an_agent_reads_from_the_media_directory_reaches_each_model`
(`mcp.rs`) holds what `code` returns to them.

| Harness | How the agent looks | What the model gets |
| --- | --- | --- |
| `claude-code` 2.1.325 | `code` | The image (JPEG, PNG, GIF or WebP), resized by Claude Code to 2,000 px. `audio` never reaches the model; a PDF `resource` is saved to disk and named. |
| `codex` 0.169.0 | `code` | The image, as an `input_image`. An embedded `resource` blob reaches it as JSON text. |
| `muse` 1.4.20 | `code` | The image, as it is. |
| `omp` 18.20.0 | `code` | The image, as it is; `audio` and `resource` blobs are dropped. |
| `opencode` 2.x | `code`, inside OpenCode's own code mode, which copies images onto its result | The image, as media; a PDF blob too. |
| `agy` 1.5.8 | `code`, then AGY's native `read_file` on the path AGY names | An image of at most 10,240 bytes, inline; a larger one (any photo) is offloaded to `brain/<cascade>/.system_generated/steps/<n>/media_<i>` and the model is told only that path, which `read_file` opens as an image. The native-tool guard allows `read_file` for media in the workspace, `<data>/media` and those steps, and nothing else. |
| `spiron` e05781b9e | Its own `file_read` | Nothing yet: `file_read` resolves every path under the workdir, so the media path does not open (below). |
| `grok`, `dsh`, cloud harnesses | | Not checked. |

### Antigravity

AGY's `call_mcp_tool` (`(*mcpToolCall).callTool`) turns an `image` block, or
an embedded `resource` with a blob and no text, into media, writes every one
to the step's directory, and gives it to the model only when its decoded
bytes are at most the cascade's `tool_config.mcp.max_output_bytes`, 10,240
by default; the rest it names as `[Resource offloaded to file://...]`. No MIME
type, annotation, setting or environment variable changes this. Its native
`read_file` (`{"Path"}`) gives the model an image, a PDF or audio as media,
scaled to 2,000 px and 5 MiB. So the guard (`anyagents-native-tool-guard` in
`hooks.json`) lets `read_file` open a file whose bytes say it is an image, a
PDF or audio, at a real path in the workspace, in `<data>/media`, or under
AGY's own offloaded steps.

### What Spiron needs

The deployed Spiron (e05781b9e) gives its model an image its own `file_read`
returns, but only inside the workdir: `file_read` joins an absolute path
beneath the workdir, and Code Mode's `read()` refuses it as escaping the
root, so a `[Local media ...]` path never opens. AnyAgents mounts only its
coordination tools into Spiron, so `code` is not there either. For a Spiron
agent to look at a file it was sent, the Spiron repository needs:

- A read root for `<data>/media` beside the workdir, in `file_read` and in
  Code Mode's `read()` (for example `--read-root <path>`, which AnyAgents
  would pass at launch), reading that directory by absolute path.
- An image its file tool returns given to the model as vision, scaled to the
  serving model's limits (today it sends the bytes as they are, any type,
  so a large PNG or a HEIC fails the request or eats the context window).
- An MCP `image` block given to the model as vision, rather than serialized
  into the tool's text and cut at 32,000 characters, for any mounted tool
  that returns one.

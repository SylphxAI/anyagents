"""What each vendor's MCP client gives its model for one tool result.

    mcp_client.py HARNESS < CallToolResult JSON

prints `{"images": [{"mime", "sha256"}], "text": [...], "offloaded": [...]}`:
the images the model is given as images, the text it is given, and the files
the vendor wrote a media block to instead of giving it (it tells the model
only their paths).

Each rule is the vendor's own, read from its source or bundled code (never
run), at the version the fleet runs:

claude-code 2.1.325  An `image` block of a JPEG, PNG, GIF or WebP
    (`image/jpg` and parameters normalised) is given as an image, resized
    to its limits; any other image, and every `audio`, is saved to disk and
    named in text. A `resource` with text is text; one with an image blob
    is an image; any other blob is saved and named. `structuredContent`
    goes after the blocks as text and drops none of them.
codex 0.169.0  A non-null `structuredContent` replaces the whole output with
    its JSON as text: every image is dropped (`models.rs`). Otherwise an
    `image` is an `input_image`, `audio` is dropped for a model without the
    audio modality, and a `resource` or `resource_link` is its JSON as text.
muse 1.4.20-R8619.1  An `image` is given as it is (live session records:
    `tool_result_model_visible_content` of kind `image`).
omp 18.20.0  An `image` passes through as it is; `audio` is dropped; a
    `resource` is its text or its URI (`mcp/tool-bridge.ts`).
opencode 2.x  An `image`, or a resource blob, of PNG, JPEG, GIF or WebP is
    given as media (`tool/mcp.ts`); `structuredContent` drops nothing; a
    result with `isError` is its text alone.
agy 1.5.8  An `image`, or an embedded `resource` with a blob and no text, is
    written to `steps/<n>/media_<i>` and given to the model only when its
    decoded bytes are at most `tool_config.mcp.max_output_bytes` (10,240 by
    default); otherwise the model is told `[Resource offloaded to file://...]`
    (`(*mcpToolCall).callTool`). `audio` and `resource_link` are dropped.
spiron e05781b9e  A mounted tool's result is serialized to text and cut to
    32,000 characters, head and tail: an image is base64 text, never vision
    (`loop_turn.rs` `tool_result_message`).
"""
import base64
import hashlib
import json
import sys

IMAGE_TYPES = ("image/jpeg", "image/png", "image/gif", "image/webp")
AGY_MAX_OUTPUT_BYTES = 10240


def image(data, mime):
    raw = base64.b64decode(data)
    return {"mime": mime, "sha256": hashlib.sha256(raw).hexdigest()}


def mime_of(block):
    mime = (block.get("mimeType") or "").split(";")[0].strip().lower()
    return "image/jpeg" if mime == "image/jpg" else mime


def claude_code(result, seen):
    for block in result.get("content") or []:
        kind = block.get("type")
        if kind == "text":
            seen["text"].append(block["text"])
        elif kind == "image" and mime_of(block) in IMAGE_TYPES:
            seen["images"].append(image(block["data"], mime_of(block)))
        elif kind == "resource":
            resource = block.get("resource") or {}
            if resource.get("text"):
                seen["text"].append(resource["text"])
            elif mime_of(resource) in IMAGE_TYPES:
                seen["images"].append(image(resource["blob"], mime_of(resource)))
            else:
                seen["text"].append("[Binary content saved to disk]")
        else:
            seen["text"].append("[Binary content saved to disk]")
    if result.get("structuredContent") is not None:
        seen["text"].append(json.dumps(result["structuredContent"]))


def codex(result, seen):
    if result.get("structuredContent") is not None:
        seen["text"].append(json.dumps(result["structuredContent"]))
        return
    for block in result.get("content") or []:
        kind = block.get("type")
        if kind == "text":
            seen["text"].append(block["text"])
        elif kind == "image":
            seen["images"].append(image(block["data"], mime_of(block)))
        elif kind in ("resource", "resource_link"):
            seen["text"].append(json.dumps(block))


def muse(result, seen):
    for block in result.get("content") or []:
        if block.get("type") == "text":
            seen["text"].append(block["text"])
        elif block.get("type") == "image":
            seen["images"].append(image(block["data"], mime_of(block)))


def omp(result, seen):
    for block in result.get("content") or []:
        kind = block.get("type")
        if kind == "text":
            seen["text"].append(block["text"])
        elif kind == "image":
            seen["images"].append(image(block["data"], mime_of(block)))
        elif kind == "resource":
            resource = block.get("resource") or {}
            seen["text"].append(resource.get("text") or resource.get("uri") or "")
    if result.get("structuredContent") is not None:
        seen["text"].append(json.dumps(result["structuredContent"]))


def opencode(result, seen):
    for block in result.get("content") or []:
        kind = block.get("type")
        if kind == "text":
            seen["text"].append(block["text"])
        elif result.get("isError"):
            continue
        elif kind == "image" and mime_of(block) in IMAGE_TYPES:
            seen["images"].append(image(block["data"], mime_of(block)))
        elif kind == "resource":
            resource = block.get("resource") or {}
            if resource.get("blob") and mime_of(resource) in IMAGE_TYPES:
                seen["images"].append(image(resource["blob"], mime_of(resource)))
            elif resource.get("text"):
                seen["text"].append(resource["text"])


def agy(result, seen):
    for index, block in enumerate(result.get("content") or []):
        kind = block.get("type")
        if kind == "text":
            seen["text"].append(block["text"])
            continue
        if kind == "image":
            data, mime = block["data"], mime_of(block)
        elif kind == "resource" and not (block.get("resource") or {}).get("text") \
                and (block.get("resource") or {}).get("blob"):
            data, mime = block["resource"]["blob"], mime_of(block["resource"])
        else:
            continue
        path = "steps/0/media_%d" % index
        if len(base64.b64decode(data)) <= AGY_MAX_OUTPUT_BYTES:
            seen["images"].append(image(data, mime))
        else:
            seen["offloaded"].append(path)
            seen["text"].append("[Resource offloaded to file://%s]" % path)


def spiron(result, seen):
    text = json.dumps(result)
    seen["text"].append(text if len(text) <= 32000 else text[:16000] + text[-16000:])


CLIENTS = {"claude-code": claude_code, "codex": codex, "muse": muse, "omp": omp,
           "opencode": opencode, "agy": agy, "spiron": spiron}


def main():
    seen = {"images": [], "text": [], "offloaded": []}
    CLIENTS[sys.argv[1]](json.load(sys.stdin), seen)
    json.dump(seen, sys.stdout)


if __name__ == "__main__":
    main()

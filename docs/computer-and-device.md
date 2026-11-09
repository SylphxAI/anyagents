# Desktop and mobile control

`tools.computer` supports `platform` (or `os`) overrides: `linux`, `macos`,
`windows`, and `chromeos`/`chromebook`. Otherwise it uses the host OS, with
`SOMMELIER_VERSION` or `CHROMEOS_RELEASE_NAME` identifying Crostini. An override
selects a local backend; it does not connect to a remote desktop. Cloud
`session_id`/`provider: "cloud"` requests are forwarded before local execution.

| Desktop | Dependencies and scope |
| --- | --- |
| Linux | X11: `xdotool`, `wmctrl`, ImageMagick `import`; capture fallbacks: Wayland `grim`, then `scrot`. Clipboard: `xclip`, `xsel`, or `wl-copy`/`wl-paste`. |
| macOS | `screencapture`, JXA (`osascript` + System Events), CoreGraphics, and `cliclick` for cursor/click operations. Grant Screen Recording, Accessibility, and Automation permissions to the invoking application. Clipboard uses `pbcopy`/`pbpaste`. |
| Windows | Windows PowerShell, .NET Windows Forms/Drawing, UIAutomation, and user32 input APIs. Requires an interactive user session; cannot control UAC's secure desktop or a locked session. Scripts and text travel over stdin, not argv. |
| ChromeOS | Crostini's Sommelier/XWayland environment uses the Linux tools above. Optional `display` and `wayland_display` select its displays. `open_url` uses `garcon-url-handler`. Android control uses the authorized ARC ADB bridge. Crostini does **not** grant access to all ChromeOS host windows. |

Desktop backend support is separate from coordinator packaging: the coordinator
still has Unix-specific runtime dependencies and does not yet ship a native
Windows distribution. Native backend scripts require validation on their target
OS; Linux Rust tests do not prove macOS/Windows GUI permissions or runtime APIs.

## Semantic snapshots

```javascript
const snapshot = await tools.computer.snapshot({ interactive_only: true });
await tools.computer.click({ ref: snapshot.elements[0].ref_id });
```

Snapshots prune unnamed, noninteractive layout wrappers and return
`@<snapshot_id>:e<N>` refs. Desktop and mobile refs require the qualified form.
Each new snapshot replaces the active ref map. Old refs fail with `STALE_REF`;
take a fresh snapshot instead of retrying them. Errors include a disposition:
`settled_absence`, `retryable_failure`, `stale_ref`, or `terminal_failure`.

Browser snapshots additionally expose `snapshot_id` and each element's
`qualified_ref`. Existing short `@e<N>` refs resolve against the active browser
snapshot for compatibility. Prefer qualified refs to detect stale snapshots.
Browser errors use the same four dispositions and redact secret input details.

## Android and iOS

`tools.device` accepts `platform: "android" | "ios"`. Without an override,
`udid` or `wda_url` selects iOS. On macOS, exactly one booted simulator also
selects iOS; otherwise Android is the default. For multiple ADB devices or
simulators, pass `serial` or `udid` explicitly.

```javascript
const state = await tools.device.snapshot({ platform: "android", serial: "emulator-5554" });
await tools.device.tap({ platform: "android", serial: "emulator-5554", ref: state.elements[0].ref_id });
await tools.device.type({ platform: "android", serial: "emulator-5554", ref: state.elements[0].ref_id, text: "hello" });

const ios = { platform: "ios", wda_url: "http://127.0.0.1:8100", wda_session_id: "YOUR-SESSION-ID" };
const screen = await tools.device.snapshot(ios);
await tools.device.tap({ ...ios, ref: screen.elements[0].ref_id });
```

Refs are scoped to the workdir and device identity (including supplied WDA session
identity), not shared with desktop snapshots or other devices. `type` with a ref
first taps its bounds' center. `rotate` invalidates the device's active refs.

Both platforms support `snapshot`, `screenshot`, `tap`, `swipe`, `type`, `press`,
`install_app`, `launch_app`, `terminate_app`, `open_url`, and `rotate`:

- **Android:** requires authorized `adb`. UI hierarchy comes from `uiautomator`;
  screenshot from `screencap`. `install_app` takes `apk_path`/`path`;
  launch/terminate take `package`. On Chromebooks, enable and authorize ARC ADB
  debugging first and select its serial. ADB's `input text` retains its platform
  limitations for non-ASCII input.
- **iOS simulators:** Xcode's `xcrun simctl` handles screenshots and app/URL
  lifecycle. Install `idb` plus its companion for semantic snapshots and touch,
  keyboard, button, and rotation input. `simctl` alone has no general-purpose
  touch or accessibility-tree API.
- **iOS physical devices:** use an existing WebDriverAgent endpoint (`wda_url`,
  optionally ending in `/session/<id>`) or `backend: "idb"` with `udid` and a
  connected idb companion. WDA discovers the active session or creates one when
  none exists. JSON `/source?format=json` supplies the semantic tree.
- **iOS app installation:** use `app_path`/`path` under the workdir. `simctl`
  installs `.app` bundles; idb supports `.app`/`.ipa`. WDA has no install API:
  installing while using WDA also requires `udid` and idb.
- **iOS app lifecycle:** use `bundle_id` (`package` is also accepted).
  `press` supports `home`, `power`/`lock`, `volume_up`, and `volume_down`; idb
  additionally supports `siri`. Android-only `back`/`recents` are not invented
  for iOS. Orientation is `portrait` or `landscape`; idb also accepts
  `landscape_left`, `landscape_right`, and `portrait_upside_down`.

WDA sends typed text in HTTP JSON bodies. Desktop scripts and clipboard setters
use stdin. idb's `ui text` and Android's `input text` CLI APIs accept text in argv;
do not use those paths for secrets on a machine with untrusted local users.

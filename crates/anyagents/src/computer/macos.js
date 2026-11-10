// Executed by osascript -l JavaScript -; `request` is JSON supplied over stdin.
ObjC.import('AppKit');
ObjC.import('CoreGraphics');
const se = Application('System Events');
function read(fn, fallback) { try { return fn(); } catch (_) { return fallback; } }
function bounds(el) {
    const p = read(() => el.position(), [0, 0]), s = read(() => el.size(), [0, 0]);
    return {x: Math.round(p[0]), y: Math.round(p[1]), width: Math.max(0, Math.round(s[0])), height: Math.max(0, Math.round(s[1]))};
}
function windows() {
    const out = [];
    for (const p of se.applicationProcesses()) {
        if (read(() => p.backgroundOnly(), true)) continue;
        const pid = p.unixId(), app = p.name();
        const ws = read(() => p.windows(), []);
        for (let i = 0; i < ws.length; i++) out.push({window_id: pid + ':' + i, title: read(() => ws[i].name(), ''), app: app, pid: pid, bounds: bounds(ws[i]), el: ws[i]});
    }
    return out;
}
function focus(wid) {
    const w = windows().find(w => w.window_id === wid);
    if (!w) throw Error('window not found');
    se.applicationProcesses.whose({unixId: w.pid})[0].frontmost = true;
    read(() => w.el.actions.byName('AXRaise').perform(), null);
}
function cliclick(commands) {
    // NSTask pipes input instead of using a shell or putting commands/text in argv.
    const task = $.NSTask.alloc.init, pipe = $.NSPipe.pipe;
    task.launchPath = '/usr/bin/env'; task.arguments = ['cliclick', '-f', '-'];
    task.standardInput = pipe;
    task.launch;
    pipe.fileHandleForWriting.writeData($(commands.join('\n') + '\n').dataUsingEncoding($.NSUTF8StringEncoding));
    pipe.fileHandleForWriting.closeFile;
    task.waitUntilExit;
    if (task.terminationStatus !== 0) throw Error('cliclick failed');
}
function point(p) { return Math.round(p.x) + ',' + Math.round(p.y); }
function keyName(k) { return ({ctrl:'ctrl',control:'ctrl',alt:'alt',option:'alt',super:'cmd',meta:'cmd',command:'cmd',cmd:'cmd',shift:'shift'})[k.toLowerCase()]; }
function key(k, mods, hold) {
    const parts = k.split('+'), last = parts.pop();
    const modifiers = (mods || []).concat(parts).map(keyName);
    if (modifiers.some(m => !m)) throw Error('unsupported modifier');
    const codes = {return:36,enter:36,tab:48,space:49,backspace:51,escape:53,esc:53,left:123,arrowleft:123,right:124,arrowright:124,down:125,arrowdown:125,up:126,arrowup:126,delete:117,home:115,end:119,pageup:116,pagedown:121};
    const code = codes[last.toLowerCase()];
    const using = modifiers.map(m => ({ctrl:'control down',alt:'option down',cmd:'command down',shift:'shift down'})[m]);
    if (hold) {
        // CoreGraphics key-down/up uses virtual keycodes, not repeated keystrokes.
        if (code === undefined || modifiers.length) throw Error('hold_key requires an unmodified named key');
        $.CGEventPost(0, $.CGEventCreateKeyboardEvent(null, code, true));
        delay(Math.min(hold, 30000) / 1000);
        $.CGEventPost(0, $.CGEventCreateKeyboardEvent(null, code, false));
    } else if (code !== undefined) se.keyCode(code, {using: using});
    else if (last.length === 1) se.keystroke(last, {using: using});
    else throw Error('unsupported key');
}
function main(r) {
    if (r.action === 'list_windows') return {windows: windows().map(w => ({window_id:w.window_id,title:w.title,app:w.app,pid:w.pid,bounds:w.bounds}))};
    if (r.action === 'snapshot') {
        let count = 0;
        function node(el, w, depth) {
            if (depth > 32 || ++count > 2000) return null;
            const axRole = read(() => el.role(), 'AXUnknown');
            const role = ({AXGroup:'group',AXScrollArea:'scroll_pane',AXTextField:'textbox',AXTextArea:'textbox',AXButton:'button',AXWindow:'window',AXStaticText:'text'})[axRole] || axRole;
            const name = String(read(() => el.name(), '') || read(() => el.description(), '') || read(() => el.value(), '') || '');
            const actions = read(() => el.actions.name(), []);
            const interactive = actions.indexOf('AXPress') >= 0 || /TextField|TextArea|Button|CheckBox|MenuItem|Link/.test(axRole);
            return {role:role,name:name,window_id:w.window_id,app:w.app,bounds:bounds(el),interactive:interactive,
                children:read(() => el.uiElements(), []).map(c => node(c, w, depth + 1)).filter(Boolean)};
        }
        return {nodes: windows().filter(w => (!r.app || w.app.toLowerCase().includes(r.app.toLowerCase())) && (!r.window_id || w.window_id === r.window_id)).map(w => node(w.el, w, 0)).filter(Boolean)};
    }
    if (r.action === 'focus_window') { focus(r.window_id || r.id); return {action:r.action,status:'focused'}; }
    if (r.target) {
        if (r.target.window_id) focus(r.target.window_id);
        if (r.action === 'type') cliclick(['c:' + point({x:r.target.bounds.x + r.target.bounds.width / 2,y:r.target.bounds.y + r.target.bounds.height / 2})]);
    }
    const c = r.coordinate, button = r.button || 'left';
    const btn = {left:0,right:1,middle:2,back:3,forward:4}[button];
    const eventType = down => button === 'left' ? (down ? 1 : 2) : button === 'right' ? (down ? 3 : 4) : (down ? 25 : 26);
    function mouse(p, down) { $.CGEventPost(0, $.CGEventCreateMouseEvent(null, eventType(down), $.CGPointMake(p.x,p.y), btn)); }
    const commands = [];
    switch (r.action) {
    case 'cursor_position': {
        const p = $.CGEventGetLocation($.CGEventCreate(null)); return {action:r.action,x:Math.round(p.x),y:Math.round(p.y)};
    }
    case 'mouse_move': commands.push('m:' + point(c)); break;
    case 'click':
        for (const m of r.modifiers || []) { const k = keyName(m); if (!k) throw Error('invalid modifier'); commands.push('kd:' + k); }
        if (button === 'left') commands.push((r.count === 2 ? 'dc:' : r.count === 3 ? 'tc:' : 'c:') + point(c));
        else {
            if (commands.length) { cliclick(commands); commands.length = 0; }
            for (let i = 0; i < (r.count || 1); i++) { mouse(c,true); mouse(c,false); }
        }
        for (const m of (r.modifiers || []).slice().reverse()) commands.push('ku:' + keyName(m));
        break;
    case 'mouse_down': case 'mouse_up': {
        const p = c || $.CGEventGetLocation($.CGEventCreate(null)); mouse(p,r.action === 'mouse_down'); break;
    }
    case 'drag': {
        mouse(r.start,true);
        const dragType = button === 'left' ? 6 : button === 'right' ? 7 : 27;
        for (let i = 1; i <= 20; i++) {
            const p = $.CGPointMake(r.start.x + (r.end.x-r.start.x)*i/20,r.start.y + (r.end.y-r.start.y)*i/20);
            $.CGEventPost(0,$.CGEventCreateMouseEvent(null,dragType,p,btn)); delay(0.01);
        }
        mouse(r.end,false); break;
    }
    case 'scroll': {
        cliclick(['m:' + point(c)].concat((r.modifiers || []).map(m => { const k = keyName(m); if (!k) throw Error('invalid modifier'); return 'kd:' + k; })));
        const n = Math.min(r.amount || 3,100), vertical = r.direction === 'up' ? n : r.direction === 'down' ? -n : 0;
        const horizontal = r.direction === 'left' ? n : r.direction === 'right' ? -n : 0;
        $.CGEventPost(0,$.CGEventCreateScrollWheelEvent(null,1,2,vertical,horizontal));
        commands.push(...(r.modifiers || []).slice().reverse().map(m => 'ku:' + keyName(m))); break;
    }
    case 'type': se.keystroke(r.text); break;
    case 'key': key(r.key,r.modifiers,0); break;
    case 'hold_key': key(r.key,[],r.duration_ms); break;
    default: throw Error('unsupported desktop action');
    }
    if (commands.length) cliclick(commands);
    return {action:r.action,backend:'osascript/cliclick',stdin_piped:true,ref:r.target ? r.target.ref_id : null};
}
JSON.stringify(main(request));

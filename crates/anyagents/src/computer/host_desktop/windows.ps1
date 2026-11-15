# This script and its base64 JSON request arrive only over PowerShell stdin.
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class NativeDesktop {
 [DllImport("user32.dll")] public static extern bool SetCursorPos(int x, int y);
 [DllImport("user32.dll")] public static extern void mouse_event(uint flags, uint dx, uint dy, uint data, UIntPtr extra);
 [DllImport("user32.dll")] public static extern void keybd_event(byte key, byte scan, uint flags, UIntPtr extra);
 [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hwnd);
 [StructLayout(LayoutKind.Sequential)] public struct INPUT { public uint type; public UNION data; }
 [StructLayout(LayoutKind.Explicit)] public struct UNION { [FieldOffset(0)] public MOUSEINPUT mouse; [FieldOffset(0)] public KEYBDINPUT key; }
 [StructLayout(LayoutKind.Sequential)] public struct MOUSEINPUT { public int x,y; public uint data,flags,time; public UIntPtr extra; }
 [StructLayout(LayoutKind.Sequential)] public struct KEYBDINPUT { public ushort vk,scan; public uint flags,time; public UIntPtr extra; }
 [DllImport("user32.dll",SetLastError=true)] static extern uint SendInput(uint count, INPUT[] inputs, int size);
 public static void TypeText(string text) {
  foreach(char c in text) {
   INPUT down = new INPUT { type=1, data=new UNION { key=new KEYBDINPUT { scan=c,flags=4 } } };
   INPUT up=down; up.data.key.flags=6;
   if(SendInput(2,new INPUT[]{down,up},Marshal.SizeOf(typeof(INPUT)))!=2) throw new Exception("SendInput failed");
  }
 }
}
'@
function Region($el) {
 $b=$el.Current.BoundingRectangle
 return @{x=[int]$b.X;y=[int]$b.Y;width=[uint32][Math]::Max(0,$b.Width);height=[uint32][Math]::Max(0,$b.Height)}
}
function KeyCode($key) {
 $aliases=@{ctrl='ControlKey';control='ControlKey';alt='Menu';super='LWin';meta='LWin';cmd='LWin';shift='ShiftKey';return='Enter';escape='Escape';arrowleft='Left';arrowright='Right';arrowup='Up';arrowdown='Down';backspace='Back'}
 $name=$aliases[$key.ToLower()]; if (!$name) { $name=$key }
 return [byte][System.Enum]::Parse([System.Windows.Forms.Keys],$name,$true)
}
function KeyEvent($key,$up) { [NativeDesktop]::keybd_event((KeyCode $key),0,$(if($up){2}else{0}),[UIntPtr]::Zero) }
function MouseButton($button,$up) {
 $flags=switch($button){ 'right' {if($up){16}else{8}} 'middle' {if($up){64}else{32}} 'back' {if($up){256}else{128}} 'forward' {if($up){256}else{128}} default {if($up){4}else{2}} }
 $data=if($button -eq 'back'){1}elseif($button -eq 'forward'){2}else{0}
 [NativeDesktop]::mouse_event($flags,0,0,$data,[UIntPtr]::Zero)
}
function Move($p) { if (![NativeDesktop]::SetCursorPos([int]$p.x,[int]$p.y)) { throw 'cursor move failed' } }
$root=[System.Windows.Automation.AutomationElement]::RootElement
$windows=$root.FindAll([System.Windows.Automation.TreeScope]::Children,[System.Windows.Automation.Condition]::TrueCondition)
$result=@{action=$r.action;backend='powershell';stdin_piped=$true}
switch ($r.action) {
 'list_windows' {
  $items=@(foreach($w in $windows){if(!$w.Current.IsOffscreen){@{window_id=[string]$w.Current.NativeWindowHandle;title=$w.Current.Name;pid=$w.Current.ProcessId;bounds=(Region $w)}}})
  $result=@{windows=$items}
 }
 'snapshot' {
  $script:nodeCount=0
  function Node($el,$wid,$app,$depth) {
   if($depth -gt 32 -or $script:nodeCount -ge 2000){return $null}; $script:nodeCount++
   $c=$el.Current; if($c.IsOffscreen){return $null}
   $role=$c.ControlType.ProgrammaticName.Replace('ControlType.','').ToLower()
   if($role -eq 'pane'){$role='panel'}; if($role -eq 'edit'){$role='textbox'}
   $children=@(); $walker=[System.Windows.Automation.TreeWalker]::ControlViewWalker
   $child=$walker.GetFirstChild($el)
   while($null -ne $child -and $script:nodeCount -lt 2000){$n=Node $child $wid $app ($depth+1);if($null -ne $n){$children+=,$n};$child=$walker.GetNextSibling($child)}
   return @{role=$role;name=$c.Name;window_id=$wid;app=$app;bounds=(Region $el);interactive=($c.IsEnabled -and ($c.IsKeyboardFocusable -or $role -in @('button','checkbox','hyperlink','menuitem')));children=@($children)}
  }
  $nodes=@(foreach($w in $windows){
   $wid=[string]$w.Current.NativeWindowHandle
   $app=(Get-Process -Id $w.Current.ProcessId -ErrorAction SilentlyContinue).ProcessName
   if((!$r.window_id -or $r.window_id -eq $wid) -and (!$r.app -or $app -like "*$($r.app)*")) { $n=Node $w $wid ([string]$app) 0;if($null -ne $n){$n} }
  })
  $result=@{nodes=@($nodes)}
 }
 'focus_window' { $wid=if($r.window_id){$r.window_id}else{$r.id}; if(![NativeDesktop]::SetForegroundWindow([IntPtr][long]$wid)){throw 'window not found'};$result.status='focused' }
 'clipboard_get' { $result.text=[System.Windows.Forms.Clipboard]::GetText() }
 'clipboard_set' { if($null -eq $r.text){throw 'missing text'};if($r.text.Length){[System.Windows.Forms.Clipboard]::SetText($r.text)}else{[System.Windows.Forms.Clipboard]::Clear()} }
 {$_ -in 'screenshot','zoom'} {
  $b=if($r.action -eq 'zoom'){$r.region}else{[System.Windows.Forms.SystemInformation]::VirtualScreen}
  $bitmap=New-Object System.Drawing.Bitmap([int]$b.width,[int]$b.height)
  $graphics=[System.Drawing.Graphics]::FromImage($bitmap);$stream=New-Object IO.MemoryStream
  try{$graphics.CopyFromScreen([int]$b.x,[int]$b.y,0,0,$bitmap.Size);$bitmap.Save($stream,[System.Drawing.Imaging.ImageFormat]::Png);$result.image_base64=[Convert]::ToBase64String($stream.ToArray());$result.mime_type='image/png';$result.width=$bitmap.Width;$result.height=$bitmap.Height;$result.bytes=$stream.Length}finally{$graphics.Dispose();$bitmap.Dispose();$stream.Dispose()}
 }
 default {
  if($r.target.window_id){if(![NativeDesktop]::SetForegroundWindow([IntPtr][long]$r.target.window_id)){throw 'target window unavailable'}}
  if($r.target -and $r.action -eq 'type'){$b=$r.target.bounds;Move @{x=($b.x+$b.width/2);y=($b.y+$b.height/2)};MouseButton 'left' $false;MouseButton 'left' $true}
  switch($r.action){
   'cursor_position' {$p=[System.Windows.Forms.Cursor]::Position;$result.x=$p.X;$result.y=$p.Y}
   'mouse_move' {Move $r.coordinate}
   'click' {Move $r.coordinate;try{foreach($m in $r.modifiers){KeyEvent $m $false};for($i=0;$i -lt [Math]::Max(1,$r.count);$i++){MouseButton $r.button $false;MouseButton $r.button $true;Start-Sleep -Milliseconds 60}}finally{foreach($m in $r.modifiers){KeyEvent $m $true}}}
   'mouse_down' {if($r.coordinate){Move $r.coordinate};MouseButton $r.button $false}
   'mouse_up' {if($r.coordinate){Move $r.coordinate};MouseButton $r.button $true}
   'drag' {Move $r.start;MouseButton $r.button $false;try{for($i=1;$i -le 20;$i++){Move @{x=($r.start.x+($r.end.x-$r.start.x)*$i/20);y=($r.start.y+($r.end.y-$r.start.y)*$i/20)};Start-Sleep -Milliseconds 10}}finally{MouseButton $r.button $true}}
   'scroll' {Move $r.coordinate;try{foreach($m in $r.modifiers){KeyEvent $m $false};$delta=[int](120*[Math]::Min(100,$r.amount));if($r.direction -in 'down','left'){$delta=-$delta};$flags=if($r.direction -in 'left','right'){4096}else{2048};[NativeDesktop]::mouse_event($flags,0,0,[BitConverter]::ToUInt32([BitConverter]::GetBytes($delta),0),[UIntPtr]::Zero)}finally{foreach($m in $r.modifiers){KeyEvent $m $true}}}
   'type' {[NativeDesktop]::TypeText([string]$r.text);$result.chars=$r.text.Length}
   'key' {$parts=@($r.modifiers)+@($r.key.Split('+'));try{foreach($k in $parts){KeyEvent $k $false}}finally{[array]::Reverse($parts);foreach($k in $parts){KeyEvent $k $true}}}
   'hold_key' {$parts=$r.key.Split('+');try{foreach($k in $parts){KeyEvent $k $false};Start-Sleep -Milliseconds ([Math]::Min(30000,$r.duration_ms))}finally{[array]::Reverse($parts);foreach($k in $parts){KeyEvent $k $true}}}
   default {throw 'unsupported desktop action'}
  }
  if($r.target){$result.ref=$r.target.ref_id}
 }
}
ConvertTo-Json -InputObject $result -Depth 100 -Compress

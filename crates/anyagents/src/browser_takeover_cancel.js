(() => {
 const key=Symbol.for('anyagents.takeover.cancelMouse');
 if(window[key]) return true;
 const state={modifiedDown:false,modifiedUp:null};
 const sentinel=e=>e.altKey&&e.ctrlKey&&e.metaKey&&e.shiftKey;
 // A genuine four-modifier press is not the protocol's unmodified down plus
 // marked release. Missing/hidden down or up must default to cancellation.
 window.addEventListener('pointerdown',e=>{
  if(!e.isTrusted||e.pointerType!=='mouse')return;
  state.modifiedDown=sentinel(e);state.modifiedUp=null;
 },true);
 window.addEventListener('pointerup',e=>{
  if(!e.isTrusted||e.pointerType!=='mouse')return;
  state.modifiedUp=state.modifiedDown&&sentinel(e)?e.timeStamp:null;
  state.modifiedDown=false;
 },true);
 for(const type of ['click','auxclick','dblclick','contextmenu'])window.addEventListener(type,e=>{
  if(e.isTrusted&&sentinel(e)&&state.modifiedUp!==e.timeStamp){
   e.preventDefault();e.stopImmediatePropagation();
  }
 },true);
 window[key]=state;
 return true;
})()

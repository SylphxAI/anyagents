(() => {
 const key=Symbol.for('anyagents.takeover.cancelMouse');
 if(window[key]) return true;
 const state={held:false,cancelled:false};
 const sentinel=e=>e.altKey&&e.ctrlKey&&e.metaKey&&e.shiftKey;
 window.addEventListener('pointerdown',e=>{
  if(!e.isTrusted||e.pointerType!=='mouse')return;
  state.held=!e.altKey&&!e.ctrlKey&&!e.metaKey&&!e.shiftKey;state.cancelled=false;
 },true);
 window.addEventListener('pointerup',e=>{
  if(!e.isTrusted||e.pointerType!=='mouse')return;
  state.cancelled=state.held&&sentinel(e);state.held=false;
 },true);
 for(const type of ['click','auxclick','dblclick','contextmenu'])window.addEventListener(type,e=>{
  if(e.isTrusted&&state.cancelled&&sentinel(e)){
   e.preventDefault();e.stopImmediatePropagation();
  }
 },true);
 window[key]=state;
 return true;
})()

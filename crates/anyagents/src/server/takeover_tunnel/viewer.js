(() => {
  const signal = document.querySelector('#signal');
  const take = document.querySelector('#take');
  const handback = document.querySelector('#handback');
  if (!signal || !take || !handback) return;
  const base = new URL('./', location.href);
  history.replaceState(null, '', location.pathname);

  // A guest's noVNC connection (not just document load) is the signal that
  // a desktop is available. Do not loop trying to reclaim an idle-released seat.
  const observer = new MutationObserver(acquire);
  observer.observe(signal, {attributes: true, attributeFilter: ['data-state']});
  function acquire() {
    if (signal.dataset.state === 'waiting') return;
    if (signal.dataset.state === 'view' && !take.disabled && !take.hidden) take.click();
    if (['view', 'control'].includes(signal.dataset.state)) observer.disconnect();
  }
  acquire();

  const status = document.createElement('p');
  status.setAttribute('role', 'status');
  status.hidden = true;
  handback.insertAdjacentElement('afterend', status);
  let pending = false;
  handback.addEventListener('click', async (event) => {
    event.preventDefault();
    event.stopImmediatePropagation();
    if (pending) return;
    observer.disconnect();
    pending = true;
    handback.disabled = true;
    status.hidden = false;
    status.textContent = 'Handing control back…';
    try {
      const response = await fetch(new URL('_handback', base), {
        method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error',
        headers: {'x-anyagents-takeover-action': 'handback'},
        signal: AbortSignal.timeout(45000)
      });
      if (!response.ok || !(await response.json()).handed_back) throw new Error();
      location.replace(new URL('_returned', base));
    } catch {
      // The workstation can already be in view-only mode. Keep a visible
      // retry action for an ambiguous/failed wake rather than false success.
      status.textContent = 'Couldn’t confirm handback. Try again.';
      const retry = document.createElement('button');
      retry.textContent = 'Retry hand back';
      retry.onclick = () => { retry.remove(); handback.click(); };
      status.append(' ', retry);
      pending = false;
      handback.disabled = false;
    }
  }, {capture: true});
})();

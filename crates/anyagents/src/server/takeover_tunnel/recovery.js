(() => {
  if (document.querySelector('#reopen, #reopen-sign-in') && location.search) {
    history.replaceState(null, '', location.pathname);
  }
  const button = document.querySelector('#reopen');
  if (!button) return;
  const status = document.querySelector('#progress');
  const signIn = () => '/admin/auth/login?' + new URLSearchParams({next: location.pathname.replace(/\/?$/, '/')});
  const check = async method => {
    const response = await fetch(button.dataset.url, {
      method, cache: 'no-store', credentials: 'same-origin', redirect: 'error',
      headers: {'Accept': 'application/json', 'x-anyagents-admin-csrf': '1'},
      signal: AbortSignal.timeout(15000)
    });
    if (response.status === 401 && button.dataset.login === 'true') {
      location.assign(signIn());
      return true;
    }
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Couldn’t reopen the takeover. Try again.');
    if (!data.url) return false;
    const url = new URL(data.url, location.origin);
    if (url.origin !== location.origin || !/^\/takeover\/[A-Za-z0-9][A-Za-z0-9_-]{0,63}\/$/.test(url.pathname)) {
      throw new Error('The new desktop link was invalid. Try again.');
    }
    status.textContent = 'Desktop ready. Opening…';
    location.replace(url.href);
    return true;
  };
  button.addEventListener('click', async () => {
    button.disabled = true;
    status.textContent = 'Requesting takeover…';
    try {
      if (await check('POST')) return;
      status.textContent = 'Waiting for the agent to connect the desktop…';
      const deadline = performance.now() + 120000;
      while (performance.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 1500));
        if (await check('GET')) return;
      }
      status.textContent = 'The desktop hasn’t connected yet. Try again to check for a new link.';
    } catch (error) {
      status.textContent = error instanceof TypeError || error.name === 'TimeoutError'
        ? 'Couldn’t reach the coordinator. Check your connection and try again.' : error.message;
    }
    button.disabled = false;
    button.focus();
  });
})();

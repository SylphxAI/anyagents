import { render } from 'solid-js/web';
import { App } from './App';
import { CopyButton, ThemeToggle } from './ui/primitives';
import './style.css';

const consoleRoot = document.getElementById('console-root');
const authTheme = document.getElementById('auth-theme');
const copyRoot = document.getElementById('auth-copy');
const command = document.getElementById('auth-command');
if (consoleRoot) { consoleRoot.textContent = ''; render(() => <App />, consoleRoot); }
if (authTheme) render(() => <ThemeToggle />, authTheme);
if (copyRoot && command) {
  const value = command.textContent?.trimEnd().endsWith('--url')
    ? 'anyagents auth login --url ' + window.location.origin : command.textContent ?? '';
  command.textContent = value;
  render(() => <CopyButton value={value} label="Copy" text />, copyRoot);
}

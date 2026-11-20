import { createRoot } from 'react-dom/client';
import { App } from './App';
import { CopyButton, ThemeButton, useTheme } from './ui';
import './style.css';

function AuthTheme() { return <ThemeButton {...useTheme()} />; }
const consoleRoot = document.getElementById('console-root');
const authTheme = document.getElementById('auth-theme');
const copyRoot = document.getElementById('auth-copy');
const command = document.getElementById('auth-command');
if (consoleRoot) createRoot(consoleRoot).render(<App />);
if (authTheme) createRoot(authTheme).render(<AuthTheme />);
if (copyRoot && command) {
  const value = command.textContent?.trimEnd().endsWith('--url')
    ? 'anyagents auth login --url ' + window.location.origin : command.textContent ?? '';
  command.textContent = value;
  createRoot(copyRoot).render(<CopyButton value={value} label="Copy command" text />);
}

import { BookOpen, Bot, LayoutDashboard, PackageCheck, Settings, WalletCards } from 'lucide-solid';
import type { Page } from './types';

export const NAV: { page: Page; label: string; icon: typeof Bot }[] = [
  { page: 'overview', label: 'Overview', icon: LayoutDashboard },
  { page: 'agents', label: 'Agents', icon: Bot },
  { page: 'accounts', label: 'Accounts & Quotas', icon: WalletCards },
  { page: 'knowledge', label: 'Knowledge', icon: BookOpen },
  { page: 'versions', label: 'Versions & Updates', icon: PackageCheck },
  { page: 'settings', label: 'Settings', icon: Settings },
];

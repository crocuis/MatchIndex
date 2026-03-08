import {
  LayoutDashboard,
  Trophy,
  Shield,
  Users,
  Globe,
  Calendar,
  Search,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

export interface NavItem {
  label: string;
  href: string;
  icon: LucideIcon;
}

export interface NavGroup {
  title: string;
  items: NavItem[];
}

export const NAV_GROUPS: NavGroup[] = [
  {
    title: 'Overview',
    items: [
      { label: 'Dashboard', href: '/', icon: LayoutDashboard },
      { label: 'Results', href: '/results', icon: Calendar },
      { label: 'Search', href: '/search', icon: Search },
    ],
  },
  {
    title: 'Data',
    items: [
      { label: 'Leagues', href: '/leagues', icon: Trophy },
      { label: 'Clubs', href: '/clubs', icon: Shield },
      { label: 'Players', href: '/players', icon: Users },
      { label: 'Nations', href: '/nations', icon: Globe },
    ],
  },
];

// Flat list for quick access
export const NAV_ITEMS: NavItem[] = NAV_GROUPS.flatMap((group) => group.items);

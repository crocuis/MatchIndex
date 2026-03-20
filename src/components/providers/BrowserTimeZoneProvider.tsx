'use client';

import { createContext, useContext, useEffect, useSyncExternalStore, type ReactNode } from 'react';
import { DEFAULT_TIME_ZONE, TIME_ZONE_COOKIE, normalizeTimeZone } from '@/lib/timeZone';

const ONE_YEAR_IN_SECONDS = 31536000;

const BrowserTimeZoneContext = createContext<string>(DEFAULT_TIME_ZONE);

interface BrowserTimeZoneProviderProps {
  initialTimeZone: string;
  children: ReactNode;
}

export function BrowserTimeZoneProvider({ initialTimeZone, children }: BrowserTimeZoneProviderProps) {
  return (
    <BrowserTimeZoneContext.Provider value={initialTimeZone}>
      <TimeZoneCookieSync />
      {children}
    </BrowserTimeZoneContext.Provider>
  );
}

export function useBrowserTimeZone() {
  const initialTimeZone = useContext(BrowserTimeZoneContext);

  return useSyncExternalStore(
    () => () => {},
    () => normalizeTimeZone(Intl.DateTimeFormat().resolvedOptions().timeZone),
    () => initialTimeZone
  );
}

function TimeZoneCookieSync() {
  useEffect(() => {
    const timeZone = normalizeTimeZone(Intl.DateTimeFormat().resolvedOptions().timeZone);
    const currentCookie = document.cookie
      .split('; ')
      .find((entry) => entry.startsWith(`${TIME_ZONE_COOKIE}=`))
      ?.split('=')[1];

    if (currentCookie === encodeURIComponent(timeZone)) {
      return;
    }

    document.cookie = `${TIME_ZONE_COOKIE}=${encodeURIComponent(timeZone)}; path=/; max-age=${ONE_YEAR_IN_SECONDS}; SameSite=Lax`;
  }, []);

  return null;
}

'use client';

import { useLocale } from 'next-intl';
import { useBrowserTimeZone } from '@/components/providers/BrowserTimeZoneProvider';
import {
  formatMatchDateForTimeZone,
  formatMatchDateTimeForTimeZone,
  formatMatchDateLabelForTimeZone,
  formatMatchDateShortForTimeZone,
  formatMatchTimeForTimeZone,
  getMatchSourceOffsetMinutes,
} from '@/lib/utils';

interface LocalizedMatchTextProps {
  matchId?: string;
  venue?: string;
  date: string;
  time?: string | null;
  variant: 'date' | 'dateShort' | 'time' | 'dateLabel' | 'dateTime';
  displayMode?: 'browser' | 'source';
  className?: string;
}

export function LocalizedMatchText({ matchId, venue = '', date, time, variant, displayMode = 'browser', className }: LocalizedMatchTextProps) {
  const locale = useLocale();
  const timeZone = useBrowserTimeZone();
  const sourceOffsetMinutes = displayMode === 'browser' ? getMatchSourceOffsetMinutes({ id: matchId ?? '', venue }) : 0;
  const targetTimeZone = displayMode === 'browser' ? timeZone : 'UTC';

  const formatted = variant === 'date'
    ? formatMatchDateForTimeZone(date, time, locale, targetTimeZone, sourceOffsetMinutes)
    : variant === 'dateShort'
      ? formatMatchDateShortForTimeZone(date, time, locale, targetTimeZone, sourceOffsetMinutes)
      : variant === 'dateLabel'
        ? formatMatchDateLabelForTimeZone(date, time, locale, targetTimeZone, sourceOffsetMinutes)
        : variant === 'dateTime'
          ? formatMatchDateTimeForTimeZone(date, time, locale, targetTimeZone, sourceOffsetMinutes)
          : formatMatchTimeForTimeZone(date, time, locale, targetTimeZone, sourceOffsetMinutes);

  return <span className={className} suppressHydrationWarning>{formatted}</span>;
}

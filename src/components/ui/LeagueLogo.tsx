import { cn } from '@/lib/utils';
import { isTournamentCompetition } from '@/data/competitionTypes';
import type { League } from '@/data/types';

interface LeagueLogoProps {
  leagueId: string;
  name: string;
  competitionType?: League['competitionType'];
  logo?: string;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

const leagueColorMap: Record<string, { primary: string; accent: string }> = {
  pl: { primary: '#3d195b', accent: '#00ff85' },
  laliga: { primary: '#ee8707', accent: '#1a2b4a' },
};

const defaultColors = { primary: '#374151', accent: '#9ca3af' };

const sizeClasses = { sm: 'h-6 w-6', md: 'h-8 w-8', lg: 'h-12 w-12' };
const tournamentFramePadding = { sm: 'p-1', md: 'p-1.5', lg: 'p-2' };
const svgSizes = { sm: 24, md: 32, lg: 48 };
const fontSizes = { sm: 6, md: 8, lg: 11 };

export function LeagueLogo({ leagueId, name, competitionType, logo, size = 'md', className }: LeagueLogoProps) {
  const colors = leagueColorMap[leagueId] ?? defaultColors;
  const isTournament = isTournamentCompetition({ competitionType: competitionType ?? 'league' });
  const sz = svgSizes[size];
  const fs = fontSizes[size];
  const cx = sz / 2;

  // Get abbreviation (e.g., "Premier League" → "PL", "La Liga" → "LL")
  const abbr = name.split(' ').map((word) => word[0]).join('').toUpperCase().slice(0, 2);

  if (logo) {
    return (
      <div
        className={cn(
          sizeClasses[size],
          'shrink-0 overflow-hidden',
          isTournament && [
            'rounded-md border border-border bg-surface-0 shadow-sm',
            'ring-1 ring-white/5',
            'bg-[radial-gradient(circle_at_top,_rgba(255,255,255,0.14),_transparent_42%),linear-gradient(145deg,_rgba(15,23,42,0.98),_rgba(17,24,39,0.96)_55%,_rgba(3,7,18,0.98))]',
            tournamentFramePadding[size],
          ],
          className,
        )}
      >
        <img
          src={logo}
          alt={`${name} logo`}
          width={sz}
          height={sz}
          referrerPolicy="no-referrer-when-downgrade"
          className="h-full w-full object-contain"
        />
      </div>
    );
  }

  return (
    <div className={cn(sizeClasses[size], 'shrink-0', className)}>
      <svg viewBox={`0 0 ${sz} ${sz}`} width={sz} height={sz} xmlns="http://www.w3.org/2000/svg">
        {/* Circle background */}
        <circle cx={cx} cy={cx} r={cx * 0.92} fill={colors.primary} />
        {/* Inner ring */}
        <circle cx={cx} cy={cx} r={cx * 0.72} fill="none" stroke={colors.accent} strokeWidth={sz * 0.04} opacity="0.6" />
        {/* Star decoration */}
        <circle cx={cx} cy={sz * 0.22} r={sz * 0.04} fill={colors.accent} opacity="0.8" />
        {/* Abbreviation */}
        <text
          x="50%"
          y="52%"
          textAnchor="middle"
          dominantBaseline="central"
          fill={colors.accent}
          fontSize={fs}
          fontWeight="900"
          fontFamily="system-ui, sans-serif"
          letterSpacing="1"
        >
          {abbr}
        </text>
      </svg>
    </div>
  );
}

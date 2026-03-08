import { cn } from '@/lib/utils';

interface LeagueLogoProps {
  leagueId: string;
  name: string;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

const leagueColorMap: Record<string, { primary: string; accent: string }> = {
  pl: { primary: '#3d195b', accent: '#00ff85' },
  laliga: { primary: '#ee8707', accent: '#1a2b4a' },
};

const defaultColors = { primary: '#374151', accent: '#9ca3af' };

const sizeClasses = { sm: 'h-6 w-6', md: 'h-8 w-8', lg: 'h-12 w-12' };
const svgSizes = { sm: 24, md: 32, lg: 48 };
const fontSizes = { sm: 6, md: 8, lg: 11 };

export function LeagueLogo({ leagueId, name, size = 'md', className }: LeagueLogoProps) {
  const colors = leagueColorMap[leagueId] ?? defaultColors;
  const sz = svgSizes[size];
  const fs = fontSizes[size];
  const cx = sz / 2;

  // Get abbreviation (e.g., "Premier League" → "PL", "La Liga" → "LL")
  const abbr = name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);

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

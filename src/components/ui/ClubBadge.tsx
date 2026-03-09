import { cn } from '@/lib/utils';

interface ClubBadgeProps {
  shortName: string;
  clubId: string;
  logo?: string;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

// Unique color per club derived from ID hash
const clubColorMap: Record<string, { primary: string; secondary: string }> = {
  arsenal: { primary: '#e63946', secondary: '#f1faee' },
  chelsea: { primary: '#034694', secondary: '#d4af37' },
  liverpool: { primary: '#c8102e', secondary: '#00b2a9' },
  mancity: { primary: '#6cabdd', secondary: '#1c2c5b' },
  barcelona: { primary: '#a50044', secondary: '#004d98' },
  realmadrid: { primary: '#febe10', secondary: '#00529f' },
  atletico: { primary: '#cb3524', secondary: '#272e61' },
  sevilla: { primary: '#d72028', secondary: '#f5f5f5' },
};

const defaultColors = { primary: '#4b5563', secondary: '#9ca3af' };

const sizeClasses = {
  sm: 'h-6 w-6',
  md: 'h-8 w-8',
  lg: 'h-12 w-12',
};

const svgSizes = { sm: 24, md: 32, lg: 48 };
const fontSizes = { sm: 7, md: 9, lg: 13 };

export function ClubBadge({ shortName, clubId, logo, size = 'md', className }: ClubBadgeProps) {
  const colors = clubColorMap[clubId] ?? defaultColors;
  const sz = svgSizes[size];
  const fs = fontSizes[size];
  const cx = sz / 2;

  if (logo) {
    return (
      <div className={cn(sizeClasses[size], 'shrink-0 overflow-hidden', className)}>
        <img
          src={logo}
          alt={`${shortName} badge`}
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
        {/* Shield shape */}
        <path
          d={`M ${cx} ${sz * 0.06}
              L ${sz * 0.9} ${sz * 0.2}
              L ${sz * 0.9} ${sz * 0.55}
              Q ${sz * 0.9} ${sz * 0.85} ${cx} ${sz * 0.96}
              Q ${sz * 0.1} ${sz * 0.85} ${sz * 0.1} ${sz * 0.55}
              L ${sz * 0.1} ${sz * 0.2} Z`}
          fill={colors.primary}
          stroke={colors.secondary}
          strokeWidth={sz * 0.03}
        />
        {/* Horizontal stripe */}
        <rect
          x={sz * 0.15}
          y={sz * 0.38}
          width={sz * 0.7}
          height={sz * 0.08}
          fill={colors.secondary}
          opacity="0.4"
          rx={sz * 0.02}
        />
        {/* Club short name */}
        <text
          x="50%"
          y="50%"
          textAnchor="middle"
          dominantBaseline="central"
          fill={colors.secondary}
          fontSize={fs}
          fontWeight="800"
          fontFamily="system-ui, sans-serif"
          letterSpacing="0.5"
        >
          {shortName}
        </text>
      </svg>
    </div>
  );
}

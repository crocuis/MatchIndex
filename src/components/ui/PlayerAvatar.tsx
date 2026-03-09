import { cn } from '@/lib/utils';

interface PlayerAvatarProps {
  name: string;
  position: 'GK' | 'DEF' | 'MID' | 'FWD';
  imageUrl?: string;
  size?: 'sm' | 'md' | 'lg' | 'xl';
  className?: string;
}

const positionColors: Record<string, { bg: string; fg: string; accent: string }> = {
  GK: { bg: '#92400e', fg: '#fbbf24', accent: '#f59e0b' },
  DEF: { bg: '#1e3a5f', fg: '#60a5fa', accent: '#3b82f6' },
  MID: { bg: '#064e3b', fg: '#34d399', accent: '#10b981' },
  FWD: { bg: '#7f1d1d', fg: '#f87171', accent: '#ef4444' },
};

const sizeClasses = {
  sm: 'h-7 w-7',
  md: 'h-9 w-9',
  lg: 'h-14 w-14',
  xl: 'h-32 w-32',
};

const fontSizes = {
  sm: 9,
  md: 11,
  lg: 16,
  xl: 34,
};

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash |= 0;
  }
  return Math.abs(hash);
}

function getInitials(name: string): string {
  const parts = name.split(' ');
  if (parts.length >= 2) {
    return `${parts[0][0]}${parts[parts.length - 1][0]}`.toUpperCase();
  }
  return name.slice(0, 2).toUpperCase();
}

export function PlayerAvatar({ name, position, imageUrl, size = 'md', className }: PlayerAvatarProps) {
  const colors = positionColors[position] ?? positionColors.MID;
  const initials = getInitials(name);
  const hash = hashString(name);
  const rotation = (hash % 360);
  const sz = size === 'sm' ? 28 : size === 'md' ? 36 : size === 'lg' ? 56 : 128;
  const fontSize = fontSizes[size];

  if (imageUrl) {
    return (
      <div className={cn(sizeClasses[size], 'rounded-full overflow-hidden shrink-0 border border-border-subtle bg-surface-2', className)}>
        <img
          src={imageUrl}
          alt={`${name} portrait`}
          width={sz}
          height={sz}
          className="h-full w-full object-cover"
        />
      </div>
    );
  }

  return (
    <div className={cn(sizeClasses[size], 'rounded-full overflow-hidden shrink-0 border border-border-subtle', className)}>
      <svg viewBox={`0 0 ${sz} ${sz}`} width={sz} height={sz} xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id={`pg-${hash}`} x1="0%" y1="0%" x2="100%" y2="100%"
            gradientTransform={`rotate(${rotation})`}>
            <stop offset="0%" stopColor={colors.bg} />
            <stop offset="100%" stopColor={colors.accent} />
          </linearGradient>
        </defs>
        <rect width={sz} height={sz} fill={`url(#pg-${hash})`} />
        {/* Subtle pattern overlay */}
        <circle cx={sz * 0.7} cy={sz * 0.3} r={sz * 0.25} fill={colors.fg} opacity="0.07" />
        <circle cx={sz * 0.3} cy={sz * 0.8} r={sz * 0.15} fill={colors.fg} opacity="0.05" />
        {/* Initials */}
        <text
          x="50%"
          y="50%"
          textAnchor="middle"
          dominantBaseline="central"
          fill={colors.fg}
          fontSize={fontSize}
          fontWeight="700"
          fontFamily="system-ui, sans-serif"
        >
          {initials}
        </text>
      </svg>
    </div>
  );
}

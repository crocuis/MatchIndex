import { cn } from '@/lib/utils';

interface NationBadgeProps {
  nationId: string;
  code: string;
  crest?: string;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

const sizeClasses = {
  sm: 'h-6 w-6',
  md: 'h-8 w-8',
  lg: 'h-12 w-12',
};

const svgSizes = { sm: 24, md: 32, lg: 48 };
const fontSizes = { sm: 8, md: 10, lg: 14 };

function isPlaceholderNationBadge(crest?: string) {
  return Boolean(crest?.startsWith('https://api.fifa.com/api/v3/picture/flags-sq-3/'));
}

export function NationBadge({ nationId, code, crest, size = 'md', className }: NationBadgeProps) {
  const resolvedCrest = crest && !isPlaceholderNationBadge(crest) ? crest : undefined;
  const svgSize = svgSizes[size];
  const fontSize = fontSizes[size];

  if (resolvedCrest) {
    return (
      <div className={cn(sizeClasses[size], 'shrink-0 overflow-hidden rounded-full border border-border-subtle bg-surface-2', className)}>
        <img
          src={resolvedCrest}
          alt={`${code} badge`}
          width={svgSize}
          height={svgSize}
          referrerPolicy="no-referrer-when-downgrade"
          className="h-full w-full object-contain"
        />
      </div>
    );
  }

  return (
    <div className={cn(sizeClasses[size], 'shrink-0 overflow-hidden rounded-full border border-border-subtle bg-surface-2', className)}>
      <svg viewBox={`0 0 ${svgSize} ${svgSize}`} width={svgSize} height={svgSize} xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id={`nation-badge-${nationId}`} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#12343b" />
            <stop offset="100%" stopColor="#1f6f78" />
          </linearGradient>
        </defs>
        <circle cx={svgSize / 2} cy={svgSize / 2} r={svgSize * 0.47} fill={`url(#nation-badge-${nationId})`} />
        <text
          x="50%"
          y="50%"
          textAnchor="middle"
          dominantBaseline="central"
          fill="#d9f3f0"
          fontSize={fontSize}
          fontWeight="800"
          fontFamily="system-ui, sans-serif"
          letterSpacing="0.4"
        >
          {code}
        </text>
      </svg>
    </div>
  );
}

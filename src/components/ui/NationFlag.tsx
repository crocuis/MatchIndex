import { cn } from '@/lib/utils';

interface NationFlagProps {
  nationId: string;
  code: string;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

// Simplified flag-inspired color bands per nation
const flagColors: Record<string, { bands: string[]; textColor: string }> = {
  eng: { bands: ['#ffffff', '#ce1124'], textColor: '#ce1124' },       // England: white + red cross
  esp: { bands: ['#c60b1e', '#ffc400', '#c60b1e'], textColor: '#fff' }, // Spain: red-yellow-red
  fra: { bands: ['#002395', '#ffffff', '#ed2939'], textColor: '#002395' }, // France: tricolor
  bra: { bands: ['#009c3b', '#ffdf00', '#002776'], textColor: '#002776' }, // Brazil: green-yellow-blue
};

const defaultFlag = { bands: ['#4b5563', '#6b7280', '#4b5563'], textColor: '#fff' };

const sizeClasses = { sm: 'h-5 w-7', md: 'h-6 w-9', lg: 'h-8 w-12' };
const svgW = { sm: 28, md: 36, lg: 48 };
const svgH = { sm: 20, md: 24, lg: 32 };
const fontSizes = { sm: 7, md: 8, lg: 11 };

export function NationFlag({ nationId, code, size = 'md', className }: NationFlagProps) {
  const flag = flagColors[nationId] ?? defaultFlag;
  const w = svgW[size];
  const h = svgH[size];
  const fs = fontSizes[size];
  const bandH = h / flag.bands.length;

  return (
    <div className={cn(sizeClasses[size], 'rounded-sm overflow-hidden shrink-0 border border-border-subtle', className)}>
      <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h} xmlns="http://www.w3.org/2000/svg">
        {/* Flag bands */}
        {flag.bands.map((color, i) => (
          <rect
            key={i}
            x={0}
            y={i * bandH}
            width={w}
            height={bandH + 0.5} // overlap to prevent gaps
            fill={color}
          />
        ))}
        {/* Country code overlay */}
        <text
          x="50%"
          y="50%"
          textAnchor="middle"
          dominantBaseline="central"
          fill={flag.textColor}
          fontSize={fs}
          fontWeight="800"
          fontFamily="system-ui, sans-serif"
          opacity="0.6"
          letterSpacing="0.5"
        >
          {code}
        </text>
      </svg>
    </div>
  );
}

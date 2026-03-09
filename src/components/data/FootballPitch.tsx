import { cn } from '@/lib/utils';

interface FootballPitchProps {
  children?: React.ReactNode;
  className?: string;
}

export function FootballPitch({ children, className }: FootballPitchProps) {
  return (
    <svg
      viewBox="0 0 120 80"
      className={cn('w-full rounded border border-border bg-[#09141d]', className)}
      role="img"
      aria-label="football pitch"
    >
      <rect x="0" y="0" width="120" height="80" fill="#09141d" />
      <rect x="1" y="1" width="118" height="78" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <line x1="60" y1="1" x2="60" y2="79" stroke="#d4dde6" strokeWidth="0.6" />
      <circle cx="60" cy="40" r="10" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <circle cx="60" cy="40" r="0.8" fill="#d4dde6" />
      <rect x="1" y="18" width="18" height="44" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <rect x="1" y="30" width="6" height="20" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <circle cx="12" cy="40" r="0.8" fill="#d4dde6" />
      <path d="M 18 30 A 10 10 0 0 1 18 50" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <rect x="101" y="18" width="18" height="44" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <rect x="113" y="30" width="6" height="20" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      <circle cx="108" cy="40" r="0.8" fill="#d4dde6" />
      <path d="M 102 30 A 10 10 0 0 0 102 50" fill="none" stroke="#d4dde6" strokeWidth="0.6" />
      {children}
    </svg>
  );
}

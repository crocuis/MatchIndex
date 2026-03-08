import Link from 'next/link';
import { cn } from '@/lib/utils';
import type { EntityType } from '@/data/types';

interface EntityLinkProps {
  type: EntityType;
  id: string;
  children: React.ReactNode;
  className?: string;
}

const typeToPath: Record<EntityType, string> = {
  player: '/players',
  club: '/clubs',
  league: '/leagues',
  nation: '/nations',
};

export function EntityLink({ type, id, children, className }: EntityLinkProps) {
  return (
    <Link
      href={`${typeToPath[type]}/${id}`}
      className={cn(
        'text-text-primary hover:text-accent-emerald transition-colors',
        className
      )}
    >
      {children}
    </Link>
  );
}

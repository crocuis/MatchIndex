'use client';

import Link from 'next/link';
import { useState } from 'react';
import type { ReactNode } from 'react';

interface HoverPrefetchLinkProps {
  href: string;
  children: ReactNode;
  className?: string;
}

export function HoverPrefetchLink({ href, children, className }: HoverPrefetchLinkProps) {
  const [active, setActive] = useState(false);

  return (
    <Link
      href={href}
      prefetch={active ? null : false}
      onMouseEnter={() => setActive(true)}
      className={className}
    >
      {children}
    </Link>
  );
}

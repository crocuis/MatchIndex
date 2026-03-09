'use client';

import type { MatchAnalysisEvent } from '@/data/types';
import { FootballPitch } from '@/components/data/FootballPitch';

interface PassMapProps {
  events: MatchAnalysisEvent[];
  playerId: string;
  playerLabel: string;
}

function isSuccessfulPass(event: MatchAnalysisEvent) {
  return !event.outcome || event.outcome === 'Complete';
}

export function PassMap({ events, playerId, playerLabel }: PassMapProps) {
  const passes = events.filter(
    (event) => event.type === 'pass'
      && event.playerId === playerId
      && event.locationX !== undefined
      && event.locationY !== undefined
      && event.endLocationX !== undefined
      && event.endLocationY !== undefined
  );

  return (
    <div className="space-y-2">
      <div className="text-[12px] font-medium text-text-secondary">{playerLabel}</div>
      <FootballPitch>
        <defs>
          <marker id={`pass-arrow-${playerId}`} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="4" markerHeight="4" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#34d399" />
          </marker>
          <marker id={`pass-arrow-fail-${playerId}`} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="4" markerHeight="4" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#f87171" />
          </marker>
        </defs>
        {passes.map((event) => {
          const successful = isSuccessfulPass(event);

          return (
            <line
              key={event.id}
              x1={event.locationX}
              y1={event.locationY}
              x2={event.endLocationX}
              y2={event.endLocationY}
              stroke={successful ? '#34d399' : '#f87171'}
              strokeOpacity={successful ? 0.75 : 0.5}
              strokeWidth="0.7"
              markerEnd={`url(#${successful ? `pass-arrow-${playerId}` : `pass-arrow-fail-${playerId}`})`}
            />
          );
        })}
      </FootballPitch>
    </div>
  );
}

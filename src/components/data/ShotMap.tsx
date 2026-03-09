'use client';

import type { MatchAnalysisEvent } from '@/data/types';
import { FootballPitch } from '@/components/data/FootballPitch';

interface ShotMapProps {
  events: MatchAnalysisEvent[];
  homeTeamId: string;
  awayTeamId: string;
}

function isShotEvent(event: MatchAnalysisEvent) {
  return event.type === 'shot'
    || event.type === 'goal'
    || event.type === 'penalty_scored'
    || event.type === 'penalty_missed';
}

function getShotColor(outcome?: string) {
  if (outcome === 'Goal') return '#34d399';
  if (outcome === 'Saved' || outcome === 'Saved To Post') return '#fbbf24';
  if (outcome === 'Blocked') return '#94a3b8';
  return '#f87171';
}

export function ShotMap({ events, homeTeamId, awayTeamId }: ShotMapProps) {
  const shots = events.filter(
    (event) => isShotEvent(event) && event.locationX !== undefined && event.locationY !== undefined
  );

  return (
    <FootballPitch>
      {shots.map((event) => {
        const x = event.teamId === awayTeamId ? 120 - event.locationX! : event.locationX!;
        const y = event.locationY!;
        const radius = 1.8 + ((event.statsbombXg ?? 0.05) * 8);

        return (
          <circle
            key={event.id}
            cx={x}
            cy={y}
            r={radius}
            fill={getShotColor(event.outcome)}
            fillOpacity={event.teamId === homeTeamId ? 0.85 : 0.5}
            stroke="#e5e7eb"
            strokeWidth="0.35"
          />
        );
      })}
    </FootballPitch>
  );
}

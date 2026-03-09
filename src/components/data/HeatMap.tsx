'use client';

import type { MatchAnalysisEvent } from '@/data/types';
import { FootballPitch } from '@/components/data/FootballPitch';

interface HeatMapProps {
  events: MatchAnalysisEvent[];
  playerId: string;
  playerLabel: string;
}

const GRID_X = 12;
const GRID_Y = 8;
const CELL_WIDTH = 120 / GRID_X;
const CELL_HEIGHT = 80 / GRID_Y;

export function HeatMap({ events, playerId, playerLabel }: HeatMapProps) {
  const bins = Array.from({ length: GRID_X * GRID_Y }, () => 0);

  for (const event of events) {
    if (event.playerId !== playerId || event.locationX === undefined || event.locationY === undefined) {
      continue;
    }

    const xIndex = Math.min(GRID_X - 1, Math.max(0, Math.floor(event.locationX / CELL_WIDTH)));
    const yIndex = Math.min(GRID_Y - 1, Math.max(0, Math.floor(event.locationY / CELL_HEIGHT)));
    bins[yIndex * GRID_X + xIndex] += 1;
  }

  const maxValue = Math.max(...bins, 0);

  return (
    <div className="space-y-2">
      <div className="text-[12px] font-medium text-text-secondary">{playerLabel}</div>
      <FootballPitch>
        {bins.map((value, index) => {
          if (value === 0 || maxValue === 0) {
            return null;
          }

          const x = (index % GRID_X) * CELL_WIDTH;
          const y = Math.floor(index / GRID_X) * CELL_HEIGHT;
          const opacity = 0.12 + ((value / maxValue) * 0.58);

          return (
            <rect
              key={`${playerId}-${index}`}
              x={x}
              y={y}
              width={CELL_WIDTH}
              height={CELL_HEIGHT}
              fill="#34d399"
              fillOpacity={opacity}
            />
          );
        })}
      </FootballPitch>
    </div>
  );
}

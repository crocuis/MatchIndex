'use client';

import { useMemo, useState } from 'react';
import { useTranslations } from 'next-intl';
import type { MatchAnalysisEvent, MatchEventVisibleAreaEntry } from '@/data/types';
import { AnalysisEventPicker, type AnalysisEventPickerOption } from '@/components/data/AnalysisEventPicker';
import { FootballPitch } from '@/components/data/FootballPitch';

interface VisibleAreaMapProps {
  events: MatchAnalysisEvent[];
  visibleAreas: MatchEventVisibleAreaEntry[];
}

function buildPickerOption(entry: MatchEventVisibleAreaEntry, event?: MatchAnalysisEvent): AnalysisEventPickerOption | null {
  if (!entry.sourceEventId) {
    return null;
  }

  return {
    eventId: entry.sourceEventId,
    minute: event?.minute ?? 0,
    playerName: event?.playerName ?? 'Unknown',
    eventType: event?.type ?? 'event',
    detail: event?.detail,
    metaLabel: `${Math.floor(entry.visibleArea.length / 2)} polygon points`,
  };
}

function buildPolygonPoints(visibleArea: number[]) {
  const points: string[] = [];

  for (let index = 0; index < visibleArea.length; index += 2) {
    const x = visibleArea[index];
    const y = visibleArea[index + 1];

    if (typeof x === 'number' && typeof y === 'number') {
      points.push(`${x},${y}`);
    }
  }

  return points.join(' ');
}

export function VisibleAreaMap({ events, visibleAreas }: VisibleAreaMapProps) {
  const tMatch = useTranslations('match');
  const eventById = useMemo(() => new Map(events.map((event) => [event.id, event])), [events]);
  const options = useMemo<AnalysisEventPickerOption[]>(() => {
    return visibleAreas
      .map((entry) => buildPickerOption(entry, entry.sourceEventId ? eventById.get(entry.sourceEventId) : undefined))
      .filter((entry): entry is AnalysisEventPickerOption => entry !== null)
      .sort((left, right) => left.minute - right.minute);
  }, [eventById, visibleAreas]);
  const [selectedEventId, setSelectedEventId] = useState(options[0]?.eventId ?? '');
  const selectedArea = visibleAreas.find((entry) => entry.sourceEventId === selectedEventId) ?? visibleAreas[0];
  const selectedEvent = selectedArea?.sourceEventId ? eventById.get(selectedArea.sourceEventId) : undefined;

  if (!selectedArea) {
    return null;
  }

  return (
    <div className="space-y-3">
      <AnalysisEventPicker
        label={tMatch('visibleArea')}
        options={options}
        selectedEventId={selectedArea.sourceEventId ?? options[0]?.eventId ?? ''}
        onSelect={setSelectedEventId}
      />
      <FootballPitch>
        <polygon
          points={buildPolygonPoints(selectedArea.visibleArea)}
          fill="#34d399"
          fillOpacity={0.16}
          stroke="#34d399"
          strokeOpacity={0.75}
          strokeWidth="0.5"
        />
        {selectedEvent?.locationX !== undefined && selectedEvent.locationY !== undefined ? (
          <g>
            <circle
              cx={selectedEvent.locationX}
              cy={selectedEvent.locationY}
              r={3.5}
              fill="rgba(52, 211, 153, 0.12)"
              stroke="#34d399"
              strokeWidth="0.5"
              strokeOpacity={0.95}
            />
            <circle
              cx={selectedEvent.locationX}
              cy={selectedEvent.locationY}
              r={0.9}
              fill="#34d399"
              stroke="#e5e7eb"
              strokeWidth="0.2"
            />
          </g>
        ) : null}
      </FootballPitch>
    </div>
  );
}

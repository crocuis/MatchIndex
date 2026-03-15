'use client';

import { useMemo, useState } from 'react';
import { useTranslations } from 'next-intl';
import type { MatchAnalysisEvent, MatchEventFreezeFrameEntry } from '@/data/types';
import { AnalysisEventPicker, type AnalysisEventPickerOption } from '@/components/data/AnalysisEventPicker';
import { FootballPitch } from '@/components/data/FootballPitch';

interface FreezeFrameViewProps {
  events: MatchAnalysisEvent[];
  freezeFrames: MatchEventFreezeFrameEntry[];
}

function buildPickerOption(entry: MatchEventFreezeFrameEntry, event?: MatchAnalysisEvent): AnalysisEventPickerOption | null {
  if (!entry.sourceEventId) {
    return null;
  }

  return {
    eventId: entry.sourceEventId,
    minute: event?.minute ?? 0,
    playerName: event?.playerName ?? 'Unknown',
    eventType: event?.type ?? 'event',
    detail: event?.detail,
    metaLabel: `${entry.freezeFrames.length} frame points`,
  };
}

function getMarkerFill(isTeammate: boolean | null, isGoalkeeper: boolean | null, isActor: boolean | null) {
  if (isActor) {
    return '#34d399';
  }

  if (isGoalkeeper) {
    return '#f59e0b';
  }

  return isTeammate ? '#60a5fa' : '#f87171';
}

export function FreezeFrameView({ events, freezeFrames }: FreezeFrameViewProps) {
  const tMatch = useTranslations('match');
  const eventById = useMemo(() => new Map(events.map((event) => [event.id, event])), [events]);
  const options = useMemo<AnalysisEventPickerOption[]>(() => {
    return freezeFrames
      .map((entry) => buildPickerOption(entry, entry.sourceEventId ? eventById.get(entry.sourceEventId) : undefined))
      .filter((entry): entry is AnalysisEventPickerOption => entry !== null)
      .sort((left, right) => left.minute - right.minute);
  }, [eventById, freezeFrames]);
  const [selectedEventId, setSelectedEventId] = useState(options[0]?.eventId ?? '');
  const selectedFrame = freezeFrames.find((entry) => entry.sourceEventId === selectedEventId) ?? freezeFrames[0];
  const selectedEvent = selectedFrame?.sourceEventId ? eventById.get(selectedFrame.sourceEventId) : undefined;

  if (!selectedFrame) {
    return null;
  }

  return (
    <div className="space-y-3">
      <AnalysisEventPicker
        label={tMatch('freezeFrame')}
        options={options}
        selectedEventId={selectedFrame.sourceEventId ?? options[0]?.eventId ?? ''}
        onSelect={setSelectedEventId}
      />
      <FootballPitch>
        {selectedEvent?.locationX !== undefined && selectedEvent.locationY !== undefined ? (
          <g>
            <circle
              cx={selectedEvent.locationX}
              cy={selectedEvent.locationY}
              r={3.25}
              fill="none"
              stroke="#34d399"
              strokeWidth="0.45"
              strokeOpacity={0.95}
              strokeDasharray="1.2 1.2"
            />
            <circle
              cx={selectedEvent.locationX}
              cy={selectedEvent.locationY}
              r={0.8}
              fill="#34d399"
              fillOpacity={0.95}
              stroke="#e5e7eb"
              strokeWidth="0.2"
            />
          </g>
        ) : null}
        {selectedFrame.freezeFrames.map((point, index) => (
          <g key={`${point.sourceEventId ?? 'frame'}-${index}`}>
            <circle
              cx={point.locationX}
              cy={point.locationY}
              r={point.isActor ? 1.9 : 1.45}
              fill={getMarkerFill(point.isTeammate, point.isGoalkeeper, point.isActor)}
              stroke="#e5e7eb"
              strokeWidth="0.3"
              fillOpacity={0.9}
            />
            {point.isGoalkeeper ? (
              <circle
                cx={point.locationX}
                cy={point.locationY}
                r={2.35}
                fill="none"
                stroke="#f59e0b"
                strokeWidth="0.25"
                strokeOpacity={0.9}
              />
            ) : null}
          </g>
        ))}
      </FootballPitch>
      <div className="flex flex-wrap gap-3 text-[11px] text-text-muted">
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-accent-emerald" />{tMatch('actor')}</span>
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-accent-blue" />{tMatch('teammates')}</span>
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-red-400" />{tMatch('opponents')}</span>
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-amber-400" />{tMatch('goalkeeper')}</span>
      </div>
    </div>
  );
}

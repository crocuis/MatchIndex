'use client';

import { useMemo, useState } from 'react';
import { useTranslations } from 'next-intl';
import type {
  MatchAnalysisData,
  MatchEventFreezeFramesArtifactPayload,
  MatchEventVisibleAreasArtifactPayload,
} from '@/data/types';
import { FreezeFrameView } from '@/components/data/FreezeFrameView';
import { HeatMap } from '@/components/data/HeatMap';
import { PassMap } from '@/components/data/PassMap';
import { ShotMap } from '@/components/data/ShotMap';
import { VisibleAreaMap } from '@/components/data/VisibleAreaMap';
import { Badge } from '@/components/ui/Badge';
import { SectionCard } from '@/components/ui/SectionCard';
import { TabGroup } from '@/components/ui/TabGroup';

interface MatchAnalysisArtifactSources {
  analysis: string | null;
  freezeFrames: string | null;
  visibleAreas: string | null;
}

interface AnalysisCapabilityItem {
  key: string;
  label: string;
  value: number;
}

interface MatchAnalysisTabsProps {
  analysis: MatchAnalysisData;
  freezeFrames: MatchEventFreezeFramesArtifactPayload | null;
  visibleAreas: MatchEventVisibleAreasArtifactPayload | null;
  homeTeamId: string;
  awayTeamId: string;
  artifactSources: MatchAnalysisArtifactSources;
}

export function MatchAnalysisTabs({
  analysis,
  freezeFrames,
  visibleAreas,
  homeTeamId,
  awayTeamId,
  artifactSources,
}: MatchAnalysisTabsProps) {
  const tMatch = useTranslations('match');
  const linkedEventIds = useMemo(() => new Set(analysis.events.map((event) => event.id)), [analysis.events]);
  const capabilityItems = useMemo<AnalysisCapabilityItem[]>(() => [
    { key: 'total', label: tMatch('analysisCapabilityTotalEvents'), value: analysis.summary.totalEvents },
    { key: 'timeline', label: tMatch('analysisCapabilityTimeline'), value: analysis.summary.timelineEvents },
    { key: 'coords', label: tMatch('analysisCapabilityCoordinates'), value: analysis.summary.coordinateEvents },
    { key: 'pass', label: tMatch('analysisCapabilityPassMap'), value: analysis.summary.passMapEligibleEvents },
    { key: 'heat', label: tMatch('analysisCapabilityHeatMap'), value: analysis.summary.heatMapEligibleEvents },
    { key: 'shot', label: tMatch('analysisCapabilityShotMap'), value: analysis.summary.shotMapEligibleEvents },
  ], [analysis.summary, tMatch]);
  const selectablePlayers = useMemo(() => {
    const seen = new Set<string>();

    return analysis.events
      .filter((event) => event.playerId && event.playerName)
      .map((event) => ({
        id: event.playerId!,
        name: event.playerName!,
        teamId: event.teamId,
      }))
      .filter((player) => {
        if (seen.has(player.id)) {
          return false;
        }

        seen.add(player.id);
        return true;
      })
      .sort((left, right) => left.name.localeCompare(right.name));
  }, [analysis.events]);
  const hasPassMapData = analysis.summary.passMapEligibleEvents > 0;
  const hasHeatMapData = analysis.summary.heatMapEligibleEvents > 0;
  const hasShotMapData = analysis.summary.shotMapEligibleEvents > 0;
  const hasLinkedFreezeFrames = useMemo(() => Boolean(
    freezeFrames?.freezeFrames.some((entry) => entry.sourceEventId && linkedEventIds.has(entry.sourceEventId)),
  ), [freezeFrames, linkedEventIds]);
  const hasLinkedVisibleAreas = useMemo(() => Boolean(
    visibleAreas?.visibleAreas.some((entry) => entry.sourceEventId && linkedEventIds.has(entry.sourceEventId)),
  ), [linkedEventIds, visibleAreas]);
  const showPlayerSelector = (hasPassMapData || hasHeatMapData) && selectablePlayers.length > 0;
  const [selectedPlayerId, setSelectedPlayerId] = useState(selectablePlayers[0]?.id ?? '');
  const selectedPlayer = selectablePlayers.find((player) => player.id === selectedPlayerId) ?? selectablePlayers[0];

  if (analysis.events.length === 0) {
    return (
      <SectionCard title={tMatch('analysis')}>
        <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisEmpty')}</div>
      </SectionCard>
    );
  }

  const tabs = [
  ];

  if (hasPassMapData) {
    tabs.push({
      key: 'pass-map',
      label: tMatch('passMap'),
      content: (
        selectedPlayer
          ? <PassMap events={analysis.events} playerId={selectedPlayer.id} playerLabel={selectedPlayer.name} />
          : <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisPlayerDataUnavailable')}</div>
      ),
    });
  }

  if (hasHeatMapData) {
    tabs.push({
      key: 'heat-map',
      label: tMatch('heatMap'),
      content: (
        selectedPlayer
          ? <HeatMap events={analysis.events} playerId={selectedPlayer.id} playerLabel={selectedPlayer.name} />
          : <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisPlayerDataUnavailable')}</div>
      ),
    });
  }

  if (hasShotMapData) {
    tabs.push({
      key: 'shot-map',
      label: tMatch('shotMap'),
      content: <ShotMap events={analysis.events} homeTeamId={homeTeamId} awayTeamId={awayTeamId} />,
    });
  }

  if (freezeFrames && freezeFrames.freezeFrames.length > 0) {
    tabs.push({
      key: 'freeze-frame',
      label: tMatch('freezeFrame'),
      content: (
        <FreezeFrameView
          events={analysis.events}
          freezeFrames={freezeFrames.freezeFrames}
          hasLinkedEvents={hasLinkedFreezeFrames}
        />
      ),
    });
  }

  if (visibleAreas && visibleAreas.visibleAreas.length > 0) {
    tabs.push({
      key: 'visible-area',
      label: tMatch('visibleArea'),
      content: (
        <VisibleAreaMap
          events={analysis.events}
          visibleAreas={visibleAreas.visibleAreas}
          hasLinkedEvents={hasLinkedVisibleAreas}
        />
      ),
    });
  }

  if (tabs.length === 0) {
    return (
      <SectionCard title={tMatch('analysis')}>
        <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisRenderableEmpty')}</div>
      </SectionCard>
    );
  }

  return (
    <SectionCard title={tMatch('analysis')}>
      <div className="mb-4 flex flex-wrap items-center gap-2">
        <Badge variant="info">{tMatch('analysisSourceLabel')}: {artifactSources.analysis ?? '-'}</Badge>
        <Badge variant={artifactSources.freezeFrames ? 'default' : 'warning'}>{tMatch('freezeFrame')}: {artifactSources.freezeFrames ?? '-'}</Badge>
        <Badge variant={artifactSources.visibleAreas ? 'default' : 'warning'}>{tMatch('visibleArea')}: {artifactSources.visibleAreas ?? '-'}</Badge>
      </div>
      <div className="mb-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
        {capabilityItems.map((item) => (
          <div key={item.key} className="rounded border border-border bg-surface-2/50 px-3 py-2">
            <div className="text-[10px] font-bold uppercase tracking-[0.16em] text-text-muted">{item.label}</div>
            <div className="mt-1 text-[16px] font-semibold text-text-primary">{item.value}</div>
          </div>
        ))}
      </div>
      {showPlayerSelector ? (
        <div className="mb-4 flex items-center gap-2">
          <label htmlFor="analysis-player" className="text-[12px] text-text-muted">{tMatch('player')}</label>
          <select
            id="analysis-player"
            value={selectedPlayer?.id ?? ''}
            onChange={(event) => setSelectedPlayerId(event.target.value)}
            className="rounded border border-border bg-surface-2 px-2 py-1.5 text-[12px] text-text-primary"
          >
            {selectablePlayers.map((player) => (
              <option key={player.id} value={player.id}>{player.name}</option>
            ))}
          </select>
        </div>
      ) : null}
      <TabGroup tabs={tabs} />
    </SectionCard>
  );
}

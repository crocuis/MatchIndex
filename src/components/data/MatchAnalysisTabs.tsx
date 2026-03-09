'use client';

import { useMemo, useState } from 'react';
import { useTranslations } from 'next-intl';
import type { MatchAnalysisData } from '@/data/types';
import { HeatMap } from '@/components/data/HeatMap';
import { PassMap } from '@/components/data/PassMap';
import { ShotMap } from '@/components/data/ShotMap';
import { SectionCard } from '@/components/ui/SectionCard';
import { TabGroup } from '@/components/ui/TabGroup';

interface MatchAnalysisTabsProps {
  analysis: MatchAnalysisData;
  homeTeamId: string;
  awayTeamId: string;
}

export function MatchAnalysisTabs({ analysis, homeTeamId, awayTeamId }: MatchAnalysisTabsProps) {
  const tMatch = useTranslations('match');
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
  const [selectedPlayerId, setSelectedPlayerId] = useState(selectablePlayers[0]?.id ?? '');
  const selectedPlayer = selectablePlayers.find((player) => player.id === selectedPlayerId) ?? selectablePlayers[0];

  if (analysis.events.length === 0) {
    return (
      <SectionCard title={tMatch('analysis')}>
        <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisEmpty')}</div>
      </SectionCard>
    );
  }

  return (
    <SectionCard title={tMatch('analysis')}>
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
      <TabGroup
        tabs={[
          {
            key: 'pass-map',
            label: tMatch('passMap'),
            content: (
              selectedPlayer
                ? <PassMap events={analysis.events} playerId={selectedPlayer.id} playerLabel={selectedPlayer.name} />
                : <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisEmpty')}</div>
            ),
          },
          {
            key: 'heat-map',
            label: tMatch('heatMap'),
            content: (
              selectedPlayer
                ? <HeatMap events={analysis.events} playerId={selectedPlayer.id} playerLabel={selectedPlayer.name} />
                : <div className="py-8 text-center text-[13px] text-text-muted">{tMatch('analysisEmpty')}</div>
            ),
          },
          {
            key: 'shot-map',
            label: tMatch('shotMap'),
            content: <ShotMap events={analysis.events} homeTeamId={homeTeamId} awayTeamId={awayTeamId} />,
          },
        ]}
      />
    </SectionCard>
  );
}

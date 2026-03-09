'use client';

import { useEffect, useMemo, useRef } from 'react';
import {
  generateLineup,
  LayoutType,
  Position,
  Team,
  type LineupData,
  type PlayerPositioning,
} from '@talabes/football-lineup-generator';
import type { MatchLineup as MatchLineupRow } from '@/data/types';

interface MatchLineupProps {
  lineups: MatchLineupRow[];
  homeTeamId: string;
  awayTeamId: string;
  homeTeamName: string;
  awayTeamName: string;
  placeholder: string;
}

const DEFAULT_TEAM_LABEL_X = 70;
const ADJUSTED_TEAM_LABEL_X = 130;

function redrawTeamLabels(
  canvas: HTMLCanvasElement,
  homeTeamName: string,
  awayTeamName: string,
  fieldColor: string,
  homeTeamColor: string,
  awayTeamColor: string,
  fontSize: number,
) {
  const context = canvas.getContext('2d');
  if (!context) {
    return;
  }

  const labelBandTop = 8;
  const labelBandHeight = 32;
  const clearWidth = 180;

  context.fillStyle = fieldColor;
  context.fillRect(DEFAULT_TEAM_LABEL_X - clearWidth / 2, labelBandTop, clearWidth, labelBandHeight);
  context.fillRect(canvas.width - DEFAULT_TEAM_LABEL_X - clearWidth / 2, labelBandTop, clearWidth, labelBandHeight);

  context.font = `bold ${fontSize + 4}px Arial`;
  context.textAlign = 'center';
  context.textBaseline = 'middle';

  context.fillStyle = homeTeamColor;
  context.fillText(homeTeamName, ADJUSTED_TEAM_LABEL_X, 30);

  context.fillStyle = awayTeamColor;
  context.fillText(awayTeamName, canvas.width - ADJUSTED_TEAM_LABEL_X, 30);
}

function toLineupPosition(rawPosition?: string) {
  const normalized = rawPosition?.toLowerCase() ?? '';

  if (normalized.includes('goalkeeper')) return Position.GOALKEEPER;
  if (normalized.includes('left center forward')) return Position.LEFT_FORWARD;
  if (normalized.includes('right center forward')) return Position.RIGHT_FORWARD;
  if (normalized.includes('center forward')) return Position.CENTER_FORWARD;
  if (normalized.includes('left wing')) return Position.LEFT_WINGER;
  if (normalized.includes('right wing')) return Position.RIGHT_WINGER;
  if (normalized.includes('attacking midfield')) return Position.ATTACKING_MIDFIELDER;
  if (normalized.includes('defensive midfield')) return Position.DEFENSIVE_MIDFIELDER;
  if (normalized.includes('center midfield')) return Position.CENTER_MIDFIELDER;
  if (normalized.includes('left midfield')) return Position.LEFT_MIDFIELDER;
  if (normalized.includes('right midfield')) return Position.RIGHT_MIDFIELDER;
  if (normalized.includes('left back')) return Position.LEFT_BACK;
  if (normalized.includes('right back')) return Position.RIGHT_BACK;
  if (normalized.includes('center back')) return Position.CENTER_BACK;

  return Position.CENTER_MIDFIELDER;
}

function abbreviatePlayerName(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);

  if (parts.length <= 1) {
    return name;
  }

  const firstName = parts[0];
  const lastName = parts[parts.length - 1];
  return `${firstName.charAt(0)}. ${lastName}`;
}

function createTeamPlayers(lineups: MatchLineupRow[], team: Team): PlayerPositioning[] {
  return lineups
    .filter((lineup) => lineup.isStarter)
    .map((lineup, index) => ({
      player: {
        id: team === Team.RED ? index + 1 : index + 101,
        name: abbreviatePlayerName(lineup.playerName),
        jerseyNumber: lineup.shirtNumber,
      },
      team,
      position: toLineupPosition(lineup.position),
    }));
}

export function MatchLineup({ lineups, homeTeamId, awayTeamId, homeTeamName, awayTeamName, placeholder }: MatchLineupProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  const lineupData = useMemo<LineupData | null>(() => {
    const homePlayers = createTeamPlayers(
      lineups.filter((lineup) => lineup.teamId === homeTeamId),
      Team.RED,
    );
    const awayPlayers = createTeamPlayers(
      lineups.filter((lineup) => lineup.teamId === awayTeamId),
      Team.YELLOW,
    );

    if (homePlayers.length === 0 || awayPlayers.length === 0) {
      return null;
    }

    return {
      homeTeam: { name: homeTeamName, players: homePlayers },
      awayTeam: { name: awayTeamName, players: awayPlayers },
    };
  }, [awayTeamId, awayTeamName, homeTeamId, homeTeamName, lineups]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    container.replaceChildren();

    if (!lineupData) {
      return;
    }

    const canvas = generateLineup(lineupData, {
      layoutType: LayoutType.FULL_PITCH,
      width: 960,
      height: 600,
      showPlayerNames: true,
      showJerseyNumbers: true,
      fieldColor: '#193223',
      lineColor: '#6f8f78',
      homeTeamColor: '#10b981',
      awayTeamColor: '#3b82f6',
      fontSize: 15,
      playerCircleSize: 24,
    });

    redrawTeamLabels(canvas, homeTeamName, awayTeamName, '#193223', '#10b981', '#3b82f6', 15);

    canvas.className = 'h-auto w-full';
    container.appendChild(canvas);

    return () => {
      container.replaceChildren();
    };
  }, [awayTeamName, homeTeamName, lineupData]);

  if (!lineupData) {
    return <div className="text-[13px] text-text-muted">{placeholder}</div>;
  }

  return <div ref={containerRef} className="mx-auto w-[calc(100%-32px)] max-w-full" />;
}

'use client';

import { useState } from 'react';
import type { MatchLineup as MatchLineupRow } from '@/data/types';
import { EntityLink } from '@/components/ui/EntityLink';
import { Badge } from '@/components/ui/Badge';
import { cn } from '@/lib/utils';

interface MatchLineupProps {
  lineups: MatchLineupRow[];
  homeTeamId: string;
  awayTeamId: string;
  homeTeamName: string;
  awayTeamName: string;
  placeholder: string;
}

interface GridPosition {
  row: number;
  column: number;
}

interface PositionedPlayer extends MatchLineupRow {
  displayName: string;
  x: number;
  y: number;
  rowSize: number;
  columnIndex: number;
}

interface TeamLineupModel {
  formationLabel: string | null;
  positionedStarters: PositionedPlayer[];
  starters: MatchLineupRow[];
  substitutes: MatchLineupRow[];
}

interface FormationTemplate {
  rowDepths: number[];
  rowWidths: number[][];
}

interface GridRowGroup {
  row: number;
  players: Array<{ grid: GridPosition; lineup: MatchLineupRow }>;
}

type LineupViewMode = 'pitch' | 'list';
type TeamSide = 'away' | 'home';
type RoleBucket = 'goalkeeper' | 'defender' | 'midfielder' | 'forward';

const FIELD_VIEWBOX_WIDTH = 136;
const FIELD_VIEWBOX_HEIGHT = 100;
const FIELD_LEFT = 10;
const FIELD_TOP = 8;
const FIELD_WIDTH = 116;
const FIELD_HEIGHT = 84;
const MIDLINE_X = FIELD_LEFT + FIELD_WIDTH / 2;
const HOME_COLOR = '#10b981';
const AWAY_COLOR = '#3b82f6';
const FIELD_COLOR = '#193223';
const LINE_COLOR = '#6f8f78';
const PLAYER_MARKER_RADIUS = 3.45;
const PLAYER_NUMBER_FONT_SIZE = 2.2;
const PLAYER_NAME_FONT_SIZE = 1.9;
const FIELD_CENTER_Y = FIELD_TOP + FIELD_HEIGHT / 2;
const PLAYER_LABEL_OUTER_GAP = 6.8;
const GOALKEEPER_EDGE_INSET = 3;
const DEFENDER_DEPTH_PULL = 4;
const MIDFIELDER_DEPTH_PULL = 2;
const FORWARD_DEPTH_PUSH = 0;
const MIDLINE_PLAYER_GAP = 4;

const FORMATION_TEMPLATES: Record<string, FormationTemplate> = {
  '4-3-3': {
    rowDepths: [0.18, 0.46, 0.78],
    rowWidths: [
      [0.1, 0.36, 0.64, 0.9],
      [0.28, 0.5, 0.72],
      [0.14, 0.5, 0.86],
    ],
  },
  '4-2-3-1': {
    rowDepths: [0.18, 0.4, 0.6, 0.9],
    rowWidths: [
      [0.08, 0.34, 0.66, 0.92],
      [0.32, 0.68],
      [0.1, 0.5, 0.9],
      [0.5],
    ],
  },
  '4-4-2': {
    rowDepths: [0.2, 0.52, 0.8],
    rowWidths: [
      [0.1, 0.36, 0.64, 0.9],
      [0.1, 0.36, 0.64, 0.9],
      [0.4, 0.6],
    ],
  },
  '4-4-1-1': {
    rowDepths: [0.18, 0.5, 0.73, 0.88],
    rowWidths: [
      [0.1, 0.36, 0.64, 0.9],
      [0.1, 0.36, 0.64, 0.9],
      [0.5],
      [0.5],
    ],
  },
  '4-5-1': {
    rowDepths: [0.18, 0.54, 0.84],
    rowWidths: [
      [0.1, 0.36, 0.64, 0.9],
      [0.12, 0.32, 0.5, 0.68, 0.88],
      [0.5],
    ],
  },
  '4-1-4-1': {
    rowDepths: [0.18, 0.34, 0.58, 0.84],
    rowWidths: [
      [0.1, 0.36, 0.64, 0.9],
      [0.5],
      [0.12, 0.36, 0.64, 0.88],
      [0.5],
    ],
  },
  '4-1-3-2': {
    rowDepths: [0.18, 0.36, 0.62, 0.86],
    rowWidths: [
      [0.08, 0.34, 0.66, 0.92],
      [0.5],
      [0.16, 0.5, 0.84],
      [0.38, 0.62],
    ],
  },
  '4-3-2-1': {
    rowDepths: [0.18, 0.46, 0.68, 0.86],
    rowWidths: [
      [0.1, 0.36, 0.64, 0.9],
      [0.28, 0.5, 0.72],
      [0.38, 0.62],
      [0.5],
    ],
  },
  '3-4-3': {
    rowDepths: [0.22, 0.52, 0.8],
    rowWidths: [
      [0.24, 0.5, 0.76],
      [0.08, 0.36, 0.64, 0.92],
      [0.14, 0.5, 0.86],
    ],
  },
  '3-5-2': {
    rowDepths: [0.22, 0.56, 0.84],
    rowWidths: [
      [0.24, 0.5, 0.76],
      [0.08, 0.3, 0.5, 0.7, 0.92],
      [0.4, 0.6],
    ],
  },
  '3-4-2-1': {
    rowDepths: [0.22, 0.5, 0.72, 0.88],
    rowWidths: [
      [0.24, 0.5, 0.76],
      [0.08, 0.36, 0.64, 0.92],
      [0.38, 0.62],
      [0.5],
    ],
  },
  '5-3-2': {
    rowDepths: [0.16, 0.5, 0.82],
    rowWidths: [
      [0.06, 0.24, 0.5, 0.76, 0.94],
      [0.28, 0.5, 0.72],
      [0.4, 0.6],
    ],
  },
  '5-4-1': {
    rowDepths: [0.16, 0.52, 0.84],
    rowWidths: [
      [0.06, 0.24, 0.5, 0.76, 0.94],
      [0.1, 0.36, 0.64, 0.9],
      [0.5],
    ],
  },
};

const HORIZONTAL_LANE_TEMPLATES: Record<RoleBucket, Partial<Record<number, number[]>>> = {
  goalkeeper: {
    1: [0.5],
  },
  defender: {
    1: [0.5],
    2: [0.42, 0.58],
    3: [0.28, 0.5, 0.72],
    4: [0.16, 0.39, 0.61, 0.84],
    5: [0.1, 0.3, 0.5, 0.7, 0.9],
  },
  midfielder: {
    1: [0.5],
    2: [0.4, 0.6],
    3: [0.32, 0.5, 0.68],
    4: [0.24, 0.42, 0.58, 0.76],
    5: [0.18, 0.34, 0.5, 0.66, 0.82],
  },
  forward: {
    1: [0.5],
    2: [0.45, 0.55],
    3: [0.38, 0.5, 0.62],
    4: [0.3, 0.44, 0.56, 0.7],
  },
};

const VERTICAL_LANE_TEMPLATES: Partial<Record<number, number[]>> = {
  1: [0.005],
  2: [0.14, 0.86],
  3: [0.06, 0.5, 0.94],
  4: [0.03, 0.31, 0.69, 0.97],
  5: [0.02, 0.22, 0.5, 0.78, 0.98],
};

function parseGridPosition(gridPosition?: string) {
  if (!gridPosition) {
    return null;
  }

  const match = gridPosition.match(/^(\d+):(\d+)$/);
  if (!match) {
    return null;
  }

  return {
    row: Number(match[1]),
    column: Number(match[2]),
  } satisfies GridPosition;
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

function getPitchDisplayName(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    return name;
  }

  const lastName = parts[parts.length - 1];
  if (lastName.length <= 10) {
    return lastName;
  }

  const abbreviated = abbreviatePlayerName(name);
  return abbreviated.length <= 12 ? abbreviated : `${abbreviated.slice(0, 11)}.`;
}

function getCompactPitchDisplayName(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    return name;
  }

  const lastName = parts[parts.length - 1];
  return lastName.length <= 10 ? lastName : `${lastName.slice(0, 9)}.`;
}

function getPlayerDisplayName(name: string, rowSize: number) {
  return rowSize >= 4 ? getCompactPitchDisplayName(name) : getPitchDisplayName(name);
}

function getRoleBucket(position?: string): RoleBucket {
  const normalized = position?.trim().toLowerCase() ?? '';

  if (normalized.includes('goalkeeper') || normalized === 'g') return 'goalkeeper';
  if (normalized.includes('def') || normalized === 'd') return 'defender';
  if (normalized.includes('mid') || normalized === 'm') return 'midfielder';
  if (normalized.includes('for') || normalized.includes('wing') || normalized === 'f') return 'forward';

  return 'midfielder';
}

function getPositionBadgeLabel(position?: string) {
  const role = getRoleBucket(position);

  switch (role) {
    case 'goalkeeper':
      return 'GK';
    case 'defender':
      return 'DEF';
    case 'midfielder':
      return 'MID';
    case 'forward':
      return 'FWD';
    default:
      return null;
  }
}

function formatFormationFromGrid(starters: MatchLineupRow[]) {
  const rowCounts = new Map<number, number>();

  for (const starter of starters) {
    const grid = parseGridPosition(starter.gridPosition);
    if (!grid) {
      return null;
    }

    rowCounts.set(grid.row, (rowCounts.get(grid.row) ?? 0) + 1);
  }

  const sortedRows = [...rowCounts.entries()].sort((left, right) => left[0] - right[0]);
  if (sortedRows.length === 0) {
    return null;
  }

  const outfieldRows = sortedRows.filter(([row]) => row !== 1).map(([, count]) => count);
  return outfieldRows.length > 0 ? outfieldRows.join('-') : null;
}

function formatFormationFromRoles(starters: MatchLineupRow[]) {
  const counts = {
    defender: 0,
    midfielder: 0,
    forward: 0,
  };

  for (const starter of starters) {
    const role = getRoleBucket(starter.position);
    if (role === 'goalkeeper') {
      continue;
    }

    counts[role] += 1;
  }

  const parts = [counts.defender, counts.midfielder, counts.forward].filter((count) => count > 0);
  return parts.length > 0 ? parts.join('-') : null;
}

function resolveFormationLabel(starters: MatchLineupRow[]) {
  return formatFormationFromGrid(starters) ?? formatFormationFromRoles(starters);
}

function groupStartersByGridRow(starters: MatchLineupRow[]) {
  const byRow = new Map<number, Array<{ grid: GridPosition; lineup: MatchLineupRow }>>();

  for (const starter of starters) {
    const grid = parseGridPosition(starter.gridPosition);
    if (!grid) {
      return null;
    }

    const rowPlayers = byRow.get(grid.row) ?? [];
    rowPlayers.push({ grid, lineup: starter });
    byRow.set(grid.row, rowPlayers);
  }

  return [...byRow.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([row, players]) => ({
      row,
      players: players.sort((left, right) => left.grid.column - right.grid.column),
    })) satisfies GridRowGroup[];
}

function splitGoalkeeperRow(rows: GridRowGroup[]) {
  const goalkeeperIndex = rows.findIndex(
    (row) => row.players.length === 1 && getRoleBucket(row.players[0]?.lineup.position) === 'goalkeeper'
  );

  if (goalkeeperIndex < 0) {
    return null;
  }

  return {
    goalkeeperRow: rows[goalkeeperIndex],
    outfieldRows: rows.filter((_, index) => index !== goalkeeperIndex),
  };
}

function resolveTemplateDepthX(side: TeamSide, depth: number) {
  const homeStart = FIELD_LEFT + 14;
  const homeEnd = MIDLINE_X - MIDLINE_PLAYER_GAP;
  const awayStart = FIELD_LEFT + FIELD_WIDTH - 14;
  const awayEnd = MIDLINE_X + MIDLINE_PLAYER_GAP;

  return side === 'home'
    ? homeStart + depth * (homeEnd - homeStart)
    : awayStart - depth * (awayStart - awayEnd);
}

function resolveTemplateWidthY(side: TeamSide, width: number) {
  const lane = side === 'away' ? 1 - width : width;
  return FIELD_TOP + lane * FIELD_HEIGHT;
}

function buildTemplatePositionedPlayers(starters: MatchLineupRow[], side: TeamSide) {
  const rows = groupStartersByGridRow(starters);
  if (!rows) {
    return null;
  }

  const splitRows = splitGoalkeeperRow(rows);
  if (!splitRows) {
    return null;
  }

  const shapeKey = splitRows.outfieldRows.map((row) => row.players.length).join('-');
  const template = FORMATION_TEMPLATES[shapeKey];
  if (!template || template.rowDepths.length !== splitRows.outfieldRows.length) {
    return null;
  }

  const positioned: PositionedPlayer[] = [];
  const goalkeeperEntry = splitRows.goalkeeperRow.players[0];

  positioned.push({
    ...goalkeeperEntry.lineup,
    displayName: getPlayerDisplayName(goalkeeperEntry.lineup.playerName, 1),
    x: side === 'home' ? FIELD_LEFT + GOALKEEPER_EDGE_INSET : FIELD_LEFT + FIELD_WIDTH - GOALKEEPER_EDGE_INSET,
    y: FIELD_TOP + FIELD_HEIGHT / 2,
    rowSize: 1,
    columnIndex: 0,
  });

  for (const [rowIndex, row] of splitRows.outfieldRows.entries()) {
    const widths = template.rowWidths[rowIndex];
    if (!widths || widths.length !== row.players.length) {
      return null;
    }

    for (const [columnIndex, entry] of row.players.entries()) {
      positioned.push({
        ...entry.lineup,
        displayName: getPlayerDisplayName(entry.lineup.playerName, row.players.length),
        x: resolveTemplateDepthX(side, template.rowDepths[rowIndex]),
        y: resolveTemplateWidthY(side, widths[columnIndex]),
        rowSize: row.players.length,
        columnIndex,
      });
    }
  }

  return positioned;
}

function resolveRowLane(rowIndex: number, totalRows: number) {
  const template = VERTICAL_LANE_TEMPLATES[totalRows];
  if (template?.[rowIndex] !== undefined) {
    return template[rowIndex];
  }

  if (totalRows <= 1) {
    return 0.5;
  }

  return 0.12 + (rowIndex / (totalRows - 1)) * 0.76;
}

function resolveRowX(side: TeamSide, rowIndex: number, totalRows: number) {
  const leftHalfStart = FIELD_LEFT + 16;
  const leftHalfEnd = MIDLINE_X - 4;
  const rightHalfStart = FIELD_LEFT + FIELD_WIDTH - 16;
  const rightHalfEnd = MIDLINE_X + 4;
  const lane = resolveRowLane(rowIndex, totalRows);

  return side === 'home'
    ? leftHalfStart + lane * (leftHalfEnd - leftHalfStart)
    : rightHalfStart - lane * (rightHalfStart - rightHalfEnd);
}

function resolvePlayerX(role: RoleBucket, side: TeamSide, rowIndex: number, totalRows: number) {
  if (role === 'goalkeeper') {
    return side === 'home'
      ? FIELD_LEFT + GOALKEEPER_EDGE_INSET
      : FIELD_LEFT + FIELD_WIDTH - GOALKEEPER_EDGE_INSET;
  }

  const baseX = resolveRowX(side, rowIndex, totalRows);
  const direction = side === 'home' ? 1 : -1;

  const adjustedX = role === 'defender'
    ? baseX - direction * DEFENDER_DEPTH_PULL
    : role === 'midfielder'
      ? baseX - direction * MIDFIELDER_DEPTH_PULL
      : role === 'forward'
        ? baseX + direction * FORWARD_DEPTH_PUSH
      : baseX;

  return side === 'home'
    ? Math.min(adjustedX, MIDLINE_X - MIDLINE_PLAYER_GAP)
    : Math.max(adjustedX, MIDLINE_X + MIDLINE_PLAYER_GAP);
}

function resolveHorizontalLanes(role: RoleBucket, totalColumns: number) {
  const roleTemplate = HORIZONTAL_LANE_TEMPLATES[role][totalColumns];
  if (roleTemplate) {
    if (totalColumns >= 4) {
      return roleTemplate.map((lane) => 0.02 + lane * 0.96);
    }

    if (totalColumns === 3) {
      return roleTemplate.map((lane) => 0.04 + lane * 0.92);
    }

    return roleTemplate;
  }

  if (totalColumns <= 1) {
    return [0.5];
  }

  const start = totalColumns >= 4 ? 0.03 : totalColumns === 3 ? 0.06 : 0.1;
  const span = totalColumns >= 4 ? 0.94 : totalColumns === 3 ? 0.88 : 0.8;
  return Array.from({ length: totalColumns }, (_, index) => start + (index / (totalColumns - 1)) * span);
}

function resolveColumnY(role: RoleBucket, columnIndex: number, totalColumns: number, side: TeamSide) {
  const lanes = resolveHorizontalLanes(role, totalColumns);
  const baseLane = lanes[columnIndex] ?? lanes.at(-1) ?? 0.5;
  const lane = side === 'away' ? 1 - baseLane : baseLane;

  return FIELD_TOP + lane * FIELD_HEIGHT;
}

function buildGridPositionedPlayers(starters: MatchLineupRow[], side: TeamSide) {
  const rows = groupStartersByGridRow(starters);
  if (!rows) {
    return null;
  }

  const positioned: PositionedPlayer[] = [];

  for (const [rowIndex, row] of rows.entries()) {
    const rowPlayers = row.players;
    const rowRole = getRoleBucket(rowPlayers[0]?.lineup.position);

    for (const [index, entry] of rowPlayers.entries()) {
      positioned.push({
        ...entry.lineup,
        displayName: getPlayerDisplayName(entry.lineup.playerName, rowPlayers.length),
        x: resolvePlayerX(rowRole, side, rowIndex, rows.length),
        y: resolveColumnY(rowRole, index, rowPlayers.length, side),
        rowSize: rowPlayers.length,
        columnIndex: index,
      });
    }
  }

  return positioned;
}

function buildFallbackPositionedPlayers(starters: MatchLineupRow[], side: TeamSide) {
  const buckets = {
    goalkeeper: starters.filter((starter) => getRoleBucket(starter.position) === 'goalkeeper'),
    defender: starters.filter((starter) => getRoleBucket(starter.position) === 'defender'),
    midfielder: starters.filter((starter) => getRoleBucket(starter.position) === 'midfielder'),
    forward: starters.filter((starter) => getRoleBucket(starter.position) === 'forward'),
  };

  const rows = [buckets.goalkeeper, buckets.defender, buckets.midfielder, buckets.forward].filter((bucket) => bucket.length > 0);
  const positioned: PositionedPlayer[] = [];

  rows.forEach((rowPlayers, rowIndex) => {
    const rowRole = getRoleBucket(rowPlayers[0]?.position);
    rowPlayers.forEach((player, columnIndex) => {
      positioned.push({
        ...player,
        displayName: getPlayerDisplayName(player.playerName, rowPlayers.length),
        x: resolvePlayerX(rowRole, side, rowIndex, rows.length),
        y: resolveColumnY(rowRole, columnIndex, rowPlayers.length, side),
        rowSize: rowPlayers.length,
        columnIndex,
      });
    });
  });

  return positioned;
}

function buildTeamLineupModel(lineups: MatchLineupRow[], side: TeamSide): TeamLineupModel {
  const starters = lineups.filter((lineup) => lineup.isStarter);
  const substitutes = lineups.filter((lineup) => !lineup.isStarter);
  const positionedStarters = buildTemplatePositionedPlayers(starters, side)
    ?? buildGridPositionedPlayers(starters, side)
    ?? buildFallbackPositionedPlayers(starters, side);

  return {
    formationLabel: resolveFormationLabel(starters),
    positionedStarters,
    starters,
    substitutes,
  };
}

function renderTeamSummary(teamName: string, teamColor: string, model: TeamLineupModel, side: TeamSide) {
  return (
    <div className="rounded-lg border border-border-subtle bg-surface-2/60 px-3 py-2.5">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: teamColor }} />
          <div className="min-w-0">
            <div className="truncate text-[10px] font-semibold uppercase tracking-[0.18em] text-text-secondary">
              {side === 'home' ? 'Home XI' : 'Away XI'}
            </div>
            <div className="truncate text-[13px] font-semibold text-text-primary">{teamName}</div>
          </div>
        </div>
        {model.formationLabel ? <Badge className="shrink-0 bg-surface-1 text-text-primary">{model.formationLabel}</Badge> : null}
      </div>
      <div className="mt-2 flex items-center gap-2 text-[10px] uppercase tracking-[0.14em] text-text-muted">
        <span>{model.starters.length} starters</span>
        <span className="h-1 w-1 rounded-full bg-border" />
        <span>{model.substitutes.length} bench</span>
      </div>
    </div>
  );
}

function renderBenchList(teamName: string, teamColor: string, players: MatchLineupRow[]) {
  if (players.length === 0) {
    return null;
  }

  return (
    <div className="overflow-hidden rounded-lg border border-border-subtle bg-surface-2/60">
      <div className="flex items-center justify-between border-b border-border-subtle bg-surface-3/40 px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: teamColor }} />
          <span className="truncate text-[10px] font-semibold uppercase tracking-[0.18em] text-text-secondary">{teamName}</span>
        </div>
        <Badge className="shrink-0 bg-surface-1 text-text-muted">Bench {players.length}</Badge>
      </div>
      <div className="grid gap-px bg-border-subtle sm:grid-cols-2">
        {players.map((player) => (
          <div key={`${teamName}-${player.playerId}`} className="flex items-center gap-2 bg-surface-1/80 px-2.5 py-2 text-[11px] text-text-secondary">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-border bg-surface-3 text-[10px] font-bold tabular-nums text-text-primary">
              {player.shirtNumber ?? '-'}
            </span>
            <div className="min-w-0 flex-1">
              <EntityLink type="player" id={player.playerId} className="truncate font-medium text-text-primary transition-colors hover:text-accent-emerald">
                <span>{player.playerName}</span>
              </EntityLink>
            </div>
            {getPositionBadgeLabel(player.position) ? <Badge className="shrink-0">{getPositionBadgeLabel(player.position)}</Badge> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

function renderPlayerListRows(players: MatchLineupRow[], teamName: string) {
  return players.map((player) => (
    <div key={`${teamName}-${player.playerId}`} className="flex items-center gap-2 bg-surface-1/80 px-2.5 py-2 text-[11px] text-text-secondary">
      <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-border bg-surface-3 text-[10px] font-bold tabular-nums text-text-primary">
        {player.shirtNumber ?? '-'}
      </span>
      <div className="min-w-0 flex-1">
        <EntityLink type="player" id={player.playerId} className="truncate font-medium text-text-primary transition-colors hover:text-accent-emerald">
          <span>{player.playerName}</span>
        </EntityLink>
      </div>
      {getPositionBadgeLabel(player.position) ? <Badge className="shrink-0">{getPositionBadgeLabel(player.position)}</Badge> : null}
    </div>
  ));
}

function renderListSection(title: string, countLabel: string, teamColor: string, teamName: string, players: MatchLineupRow[]) {
  if (players.length === 0) {
    return null;
  }

  return (
    <div className="overflow-hidden rounded-lg border border-border-subtle bg-surface-2/60">
      <div className="flex items-center justify-between border-b border-border-subtle bg-surface-3/40 px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: teamColor }} />
          <span className="truncate text-[10px] font-semibold uppercase tracking-[0.18em] text-text-secondary">{title}</span>
        </div>
        <Badge className="shrink-0 bg-surface-1 text-text-muted">{countLabel}</Badge>
      </div>
      <div className="grid gap-px bg-border-subtle sm:grid-cols-2">{renderPlayerListRows(players, `${teamName}-${title}`)}</div>
    </div>
  );
}

function renderTeamListView(teamName: string, teamColor: string, model: TeamLineupModel, side: TeamSide) {
  return (
    <div className="space-y-3 rounded-xl border border-border-subtle bg-surface-2/60 p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="mt-0.5 h-2.5 w-2.5 shrink-0 rounded-full" style={{ backgroundColor: teamColor }} />
          <div className="min-w-0">
            <div className="truncate text-[10px] font-semibold uppercase tracking-[0.18em] text-text-secondary">
              {side === 'home' ? 'Home Team' : 'Away Team'}
            </div>
            <div className="truncate text-[13px] font-semibold text-text-primary">{teamName}</div>
          </div>
        </div>
        {model.formationLabel ? <Badge className="shrink-0 bg-surface-1 text-text-primary">{model.formationLabel}</Badge> : null}
      </div>

      <div className="grid gap-3">
        {renderListSection('Starting XI', `${model.starters.length} players`, teamColor, teamName, model.starters)}
        {renderListSection('Bench', `${model.substitutes.length} players`, teamColor, teamName, model.substitutes)}
      </div>
    </div>
  );
}

function renderPitchView(homeTeamName: string, awayTeamName: string, homeModel: TeamLineupModel, awayModel: TeamLineupModel) {
  return (
    <>
      <div className="grid gap-2 sm:grid-cols-2">
        {renderTeamSummary(homeTeamName, HOME_COLOR, homeModel, 'home')}
        {renderTeamSummary(awayTeamName, AWAY_COLOR, awayModel, 'away')}
      </div>

      <div className="overflow-hidden rounded-xl border border-border-subtle bg-surface-2/60 p-2.5">
        <svg viewBox={`0 0 ${FIELD_VIEWBOX_WIDTH} ${FIELD_VIEWBOX_HEIGHT}`} className="h-auto w-full">
          <rect x={FIELD_LEFT} y={FIELD_TOP} width={FIELD_WIDTH} height={FIELD_HEIGHT} rx={3} fill={FIELD_COLOR} stroke={LINE_COLOR} strokeWidth={0.6} />
          <line x1={MIDLINE_X} y1={FIELD_TOP} x2={MIDLINE_X} y2={FIELD_TOP + FIELD_HEIGHT} stroke={LINE_COLOR} strokeWidth={0.45} />
          <circle cx={MIDLINE_X} cy={FIELD_TOP + FIELD_HEIGHT / 2} r={7} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />
          <circle cx={MIDLINE_X} cy={FIELD_TOP + FIELD_HEIGHT / 2} r={0.6} fill={LINE_COLOR} />

          <rect x={FIELD_LEFT} y={FIELD_TOP + 24} width={18} height={36} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />
          <rect x={FIELD_LEFT} y={FIELD_TOP + 33} width={7} height={18} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />
          <circle cx={FIELD_LEFT + 10} cy={FIELD_TOP + FIELD_HEIGHT / 2} r={0.6} fill={LINE_COLOR} />
          <path d={`M ${FIELD_LEFT + 18} ${FIELD_TOP + 33} A 7 7 0 0 1 ${FIELD_LEFT + 18} ${FIELD_TOP + 51}`} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />

          <rect x={FIELD_LEFT + FIELD_WIDTH - 18} y={FIELD_TOP + 24} width={18} height={36} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />
          <rect x={FIELD_LEFT + FIELD_WIDTH - 7} y={FIELD_TOP + 33} width={7} height={18} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />
          <circle cx={FIELD_LEFT + FIELD_WIDTH - 10} cy={FIELD_TOP + FIELD_HEIGHT / 2} r={0.6} fill={LINE_COLOR} />
          <path d={`M ${FIELD_LEFT + FIELD_WIDTH - 18} ${FIELD_TOP + 33} A 7 7 0 0 0 ${FIELD_LEFT + FIELD_WIDTH - 18} ${FIELD_TOP + 51}`} fill="none" stroke={LINE_COLOR} strokeWidth={0.45} />

          {awayModel.positionedStarters.map((player) => renderPlayer(player, 'away'))}
          {homeModel.positionedStarters.map((player) => renderPlayer(player, 'home'))}
        </svg>
      </div>

      <div className="grid gap-3 xl:grid-cols-2">
        {renderBenchList(homeTeamName, HOME_COLOR, homeModel.substitutes)}
        {renderBenchList(awayTeamName, AWAY_COLOR, awayModel.substitutes)}
      </div>
    </>
  );
}

function renderPlayer(player: PositionedPlayer, side: TeamSide) {
  const numberY = player.y + 1;
  const circleFill = side === 'away' ? AWAY_COLOR : HOME_COLOR;
  const href = `/players/${player.playerId}`;
  const crowdedRow = player.rowSize >= 4;
  const nameX = player.x;
  const textAnchor = 'middle';
  const distanceFromCenterY = player.y - FIELD_CENTER_Y;
  const verticalNudge = crowdedRow ? (player.columnIndex % 2 === 0 ? -0.7 : 0.7) : 0;
  const edgeNudge = Math.abs(distanceFromCenterY) > 20 ? (distanceFromCenterY < 0 ? -0.3 : 0.3) : 0;
  const nameY = player.y + PLAYER_LABEL_OUTER_GAP + verticalNudge + edgeNudge;

  return (
    <a
      key={`${side}-${player.playerId}`}
      href={href}
      aria-label={`${player.playerName} player page`}
      className="cursor-pointer"
    >
      <g>
        <circle cx={player.x} cy={player.y} r={PLAYER_MARKER_RADIUS} fill={circleFill} stroke="#08130d" strokeWidth={0.9} />
        <text x={player.x} y={numberY} textAnchor="middle" fontSize={PLAYER_NUMBER_FONT_SIZE} fontWeight="700" fill="#08130d" paintOrder="stroke" stroke="#f3f8f5" strokeWidth="0.12" strokeLinejoin="round">
          {player.shirtNumber ?? ''}
        </text>
        <text x={nameX} y={nameY} textAnchor={textAnchor} fontSize={PLAYER_NAME_FONT_SIZE} fontWeight="700" fill="#f5f7f6" paintOrder="stroke" stroke="#112019" strokeWidth="0.8" strokeLinejoin="round">
          {player.displayName}
        </text>
      </g>
    </a>
  );
}

export function MatchLineup({
  lineups,
  homeTeamId,
  awayTeamId,
  homeTeamName,
  awayTeamName,
  placeholder,
}: MatchLineupProps) {
  const [viewMode, setViewMode] = useState<LineupViewMode>('pitch');
  const homeLineups = lineups.filter((lineup) => lineup.teamId === homeTeamId);
  const awayLineups = lineups.filter((lineup) => lineup.teamId === awayTeamId);
  const homeModel = buildTeamLineupModel(homeLineups, 'home');
  const awayModel = buildTeamLineupModel(awayLineups, 'away');

  if (homeModel.starters.length === 0 || awayModel.starters.length === 0) {
    return <div className="text-[13px] text-text-muted">{placeholder}</div>;
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3 rounded-xl border border-border-subtle bg-surface-2/60 px-3 py-2">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-text-secondary">Lineup View</div>
          <div className="text-[12px] text-text-muted">Switch between pitch layout and team list view.</div>
        </div>
        <div className="inline-flex rounded-lg border border-border-subtle bg-surface-1 p-1">
          {([
            ['pitch', 'Pitch'],
            ['list', 'List'],
          ] as const).map(([mode, label]) => (
            <button
              key={mode}
              type="button"
              onClick={() => setViewMode(mode)}
              className={cn(
                'rounded-md px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.14em] transition-colors',
                viewMode === mode
                  ? 'bg-surface-3 text-text-primary shadow-[inset_0_0_0_1px_var(--color-border-subtle)]'
                  : 'text-text-muted hover:bg-surface-2 hover:text-text-secondary'
              )}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {viewMode === 'pitch'
        ? renderPitchView(homeTeamName, awayTeamName, homeModel, awayModel)
        : (
          <div className="grid gap-3 xl:grid-cols-2">
            {renderTeamListView(homeTeamName, HOME_COLOR, homeModel, 'home')}
            {renderTeamListView(awayTeamName, AWAY_COLOR, awayModel, 'away')}
          </div>
        )}
    </div>
  );
}

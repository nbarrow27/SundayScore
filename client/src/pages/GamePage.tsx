import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useParams, useLocation } from "wouter";
import { apiRequest } from "@/lib/queryClient";
import { ArrowLeft, Info } from "lucide-react";

// ─── Score Color Helper ─────────────────────────────────────────────────────
function getScoreClass(score: number): string {
  if (score >= 9.0) return "score-elite";
  if (score >= 7.5) return "score-great";
  if (score >= 6.5) return "score-good";
  if (score >= 5.5) return "score-average";
  if (score >= 4.5) return "score-below";
  return "score-poor";
}

function getScoreBg(score: number): string {
  if (score >= 9.0) return "hsl(142 71% 45%)";
  if (score >= 7.5) return "hsl(142 60% 50%)";
  if (score >= 6.5) return "hsl(82 60% 45%)";
  if (score >= 5.5) return "hsl(43 96% 56%)";
  if (score >= 4.5) return "hsl(25 95% 53%)";
  return "hsl(0 72% 51%)";
}

function getScoreTextColor(score: number): string {
  if (score >= 5.5) return "white";
  return "#1a1a1a";
}

// ─── Position layout definitions for field visualization ────────────────────
// Positions mapped to rows on the field
// offense: from line of scrimmage back
// defense: from line of scrimmage forward

type PositionGroup = {
  label: string;
  positions: string[];
  row: number; // 0 = closest to LOS, higher = deeper
  side: "offense" | "defense";
};

const POSITION_GROUPS: PositionGroup[] = [
  { label: "QB", positions: ["QB"], row: 2, side: "offense" },
  { label: "RB/FB", positions: ["RB", "FB"], row: 1, side: "offense" },
  { label: "WR", positions: ["WR"], row: 0, side: "offense" },
  { label: "TE", positions: ["TE"], row: 0, side: "offense" },
  { label: "OL", positions: ["OT", "OG", "C", "G", "T", "LS", "OL"], row: 3, side: "offense" },
  { label: "DL", positions: ["DE", "DT", "NT", "DL", "IDL", "ED"], row: 0, side: "defense" },
  { label: "LB", positions: ["LB", "ILB", "OLB", "MLB"], row: 1, side: "defense" },
  { label: "DB", positions: ["CB", "S", "SS", "FS", "SAF"], row: 2, side: "defense" },
];

// Map player position abbreviation -> group label
function getGroupLabel(pos: string): string {
  for (const g of POSITION_GROUPS) {
    if (g.positions.includes(pos?.toUpperCase())) return g.label;
  }
  if (["K", "P", "LS"].includes(pos?.toUpperCase())) return "ST";
  return "Other";
}

type Player = {
  id: string;
  name: string;
  position: string;
  headshot: string | null;
  jersey: string;
  stats: Record<string, Record<string, string>>;
  sundayScore: number;
  snapsPlayed: number | null;
  offenseSnapsPlayed: number | null;
  defenseSnapsPlayed: number | null;
};

const OL_POSITIONS = new Set(["OT","G","C","OG","OL","LS","FB"]);
const DEF_POSITIONS = new Set(["CB","DB","S","SS","FS","SAF","LB","ILB","OLB","MLB","DE","DT","NT","DL"]);

function didNotPlay(player: Player): boolean {
  const pos = player.position?.toUpperCase();

  // For OL/FB: grade is based on offensive snaps only — ST-only doesn't count
  if (OL_POSITIONS.has(pos)) {
    if (player.offenseSnapsPlayed === null) return false; // no data, don’t assume DNP
    return player.offenseSnapsPlayed === 0;
  }

  // For defensive players: grade is based on defensive snaps
  if (DEF_POSITIONS.has(pos)) {
    if (player.defenseSnapsPlayed === null) return player.snapsPlayed === 0;
    return player.defenseSnapsPlayed === 0 && (player.snapsPlayed ?? 1) < 10;
  }

  // For everyone else: any snaps = played
  if (player.snapsPlayed === null) return false;
  return player.snapsPlayed === 0;
}

type Team = {
  id: string;
  name: string;
  abbreviation: string;
  logo: string;
  score: string;
  color: string;
  alternateColor: string;
};

// ─── Main Page ───────────────────────────────────────────────────────────────
export default function GamePage() {
  const { gameId } = useParams<{ gameId: string }>();
  const [, navigate] = useLocation();
  const [selectedTeamId, setSelectedTeamId] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<"field" | "bench">("field");
  const [selectedPlayer, setSelectedPlayer] = useState<Player | null>(null);

  const { data: gameData, isLoading, error } = useQuery({
    queryKey: ["/api/game", gameId],
    queryFn: () => apiRequest("GET", `/api/game/${gameId}`).then(r => r.json()),
    enabled: !!gameId,
  });

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="w-16 h-16 rounded-full border-4 border-primary border-t-transparent animate-spin mx-auto mb-4" />
          <p className="text-muted-foreground">Loading game data...</p>
        </div>
      </div>
    );
  }

  if (error || !gameData) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <p className="text-destructive mb-4">Failed to load game data.</p>
          <button onClick={() => navigate("/")} className="text-primary underline">Go back</button>
        </div>
      </div>
    );
  }

  const homeTeam: Team = gameData.homeTeam;
  const awayTeam: Team = gameData.awayTeam;
  const allPlayers: Record<string, Player[]> = gameData.players;

  const activeTeamId = selectedTeamId || homeTeam?.id;
  const activeTeam = activeTeamId === homeTeam?.id ? homeTeam : awayTeam;
  const activePlayers: Player[] = allPlayers[activeTeamId || homeTeam?.id] || [];

  // Separate offense and defense
  const offensePositions = new Set(["QB", "RB", "FB", "WR", "TE", "OT", "OG", "C", "G", "T", "LS", "OL"]);
  const defensePlayers = activePlayers.filter(p => {
    const pos = p.position?.toUpperCase();
    return !offensePositions.has(pos) && !["K", "P", "K/P"].includes(pos);
  });
  const offensePlayers = activePlayers.filter(p => offensePositions.has(p.position?.toUpperCase()));
  const specialTeams = activePlayers.filter(p => ["K", "P", "K/P"].includes(p.position?.toUpperCase()));

  return (
    <div className="min-h-screen" style={{ background: "hsl(218 28% 8%)" }}>
      {/* Top Nav */}
      <header className="border-b border-border/50 px-4 py-3 sticky top-0 z-20" style={{ background: "hsl(218 28% 8%)" }}>
        <div className="max-w-6xl mx-auto flex items-center gap-4">
          <button onClick={() => navigate("/")} className="flex items-center gap-1.5 text-muted-foreground hover:text-foreground transition-colors text-sm">
            <ArrowLeft className="w-4 h-4" />
            Back
          </button>

          {/* Logo */}
          <div className="flex items-center gap-2">
            <svg width="28" height="28" viewBox="0 0 40 40" fill="none" aria-label="SundayScore">
              <rect width="40" height="40" rx="8" fill="hsl(43 96% 56%)" />
              <ellipse cx="20" cy="20" rx="10" ry="14" stroke="hsl(218 28% 8%)" strokeWidth="2" fill="none" />
              <line x1="10" y1="20" x2="30" y2="20" stroke="hsl(218 28% 8%)" strokeWidth="1.5" />
              <line x1="12" y1="14" x2="28" y2="14" stroke="hsl(218 28% 8%)" strokeWidth="1" strokeDasharray="2 2" />
              <line x1="12" y1="26" x2="28" y2="26" stroke="hsl(218 28% 8%)" strokeWidth="1" strokeDasharray="2 2" />
            </svg>
            <span className="bebas text-xl text-primary tracking-widest">SundayScore</span>
          </div>

          {/* Scoreboard */}
          <div className="ml-auto flex items-center gap-2">
            <TeamScorePill team={awayTeam} isWinner={parseInt(awayTeam?.score) > parseInt(homeTeam?.score)} />
            <span className="text-xs text-muted-foreground font-bold">@</span>
            <TeamScorePill team={homeTeam} isWinner={parseInt(homeTeam?.score) > parseInt(awayTeam?.score)} />
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6">
        {/* Team selector */}
        <div className="flex gap-3 mb-6">
          {[awayTeam, homeTeam].map(team => (
            <button
              key={team?.id}
              data-testid={`team-tab-${team?.abbreviation}`}
              onClick={() => { setSelectedTeamId(team?.id); setSelectedPlayer(null); }}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm font-semibold transition-all ${
                activeTeamId === team?.id
                  ? "bg-primary text-primary-foreground shadow-lg"
                  : "glass-card text-muted-foreground hover:text-foreground"
              }`}
            >
              {team?.logo && <img src={team.logo} alt={team.abbreviation} className="w-5 h-5 object-contain" />}
              {team?.abbreviation}
            </button>
          ))}

          <div className="ml-auto flex gap-2">
            <button
              onClick={() => setViewMode("field")}
              className={`px-3 py-2 rounded-lg text-xs font-semibold transition-all ${
                viewMode === "field" ? "bg-secondary text-foreground" : "text-muted-foreground"
              }`}
            >
              Field View
            </button>
            <button
              onClick={() => setViewMode("bench")}
              className={`px-3 py-2 rounded-lg text-xs font-semibold transition-all ${
                viewMode === "bench" ? "bg-secondary text-foreground" : "text-muted-foreground"
              }`}
            >
              All Players
            </button>
          </div>
        </div>

        {viewMode === "field" ? (
          <FieldView
            offensePlayers={offensePlayers}
            defensePlayers={defensePlayers}
            specialTeams={specialTeams}
            teamColor={activeTeam?.color}
            onPlayerClick={setSelectedPlayer}
            selectedPlayer={selectedPlayer}
          />
        ) : (
          <AllPlayersView
            players={activePlayers}
            onPlayerClick={setSelectedPlayer}
            selectedPlayer={selectedPlayer}
          />
        )}

        {/* Player detail panel */}
        {selectedPlayer && (
          <PlayerDetailPanel
            player={selectedPlayer}
            teamColor={activeTeam?.color}
            onClose={() => setSelectedPlayer(null)}
          />
        )}

        {/* Score legend */}
        <ScoreLegend />

        {/* Grading methodology note */}
        <MethodologyNote />
      </main>
    </div>
  );
}

// ─── Team Score Pill ─────────────────────────────────────────────────────────
function TeamScorePill({ team, isWinner }: { team: Team; isWinner: boolean }) {
  if (!team) return null;
  return (
    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg glass-card ${isWinner ? "border-primary/50" : ""}`}>
      {team.logo && <img src={team.logo} alt={team.abbreviation} className="w-6 h-6 object-contain" />}
      <span className={`bebas text-xl leading-none ${isWinner ? "text-foreground" : "text-muted-foreground"}`}>
        {team.score}
      </span>
    </div>
  );
}

// ─── Field View ───────────────────────────────────────────────────────────────
function FieldView({
  offensePlayers,
  defensePlayers,
  specialTeams,
  teamColor,
  onPlayerClick,
  selectedPlayer,
}: {
  offensePlayers: Player[];
  defensePlayers: Player[];
  specialTeams: Player[];
  teamColor?: string;
  onPlayerClick: (p: Player) => void;
  selectedPlayer: Player | null;
}) {
  // Get top players per group — exclude DNP players entirely from field view
  function getTopPlayers(players: Player[], positions: string[], maxCount: number) {
    return players
      .filter(p => positions.includes(p.position?.toUpperCase()) && !didNotPlay(p))
      .sort((a, b) => b.sundayScore - a.sundayScore)
      .slice(0, maxCount);
  }

  const qbs = getTopPlayers(offensePlayers, ["QB"], 2);
  const rbs = getTopPlayers(offensePlayers, ["RB", "FB"], 3);
  const wrs = getTopPlayers(offensePlayers, ["WR"], 4);
  const tes = getTopPlayers(offensePlayers, ["TE"], 2);
  const ols = getTopPlayers(offensePlayers, ["OT", "OG", "C", "G", "T", "OL", "LS"], 5);
  const dls = getTopPlayers(defensePlayers, ["DE", "DT", "NT", "DL", "IDL", "ED"], 4);
  const lbs = getTopPlayers(defensePlayers, ["LB", "ILB", "OLB", "MLB"], 3);
  const dbs = getTopPlayers(defensePlayers, ["CB", "S", "SS", "FS", "SAF"], 4);

  return (
    <div className="space-y-3">
      {/* OFFENSE */}
      <div className="glass-card rounded-xl overflow-hidden">
        <div className="px-4 py-2.5 border-b border-border/50 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="bebas text-base text-primary tracking-widest">SundayScore</span>
            <span className="text-xs text-muted-foreground">·</span>
            <span className="text-xs font-bold text-green-400 uppercase tracking-widest">Offense</span>
          </div>
          <span className="text-xs text-muted-foreground">Top starters shown · click any player</span>
        </div>

        {/* Football field */}
        <div
          className="relative p-4"
          style={{
            background: "linear-gradient(180deg, hsl(142 38% 22%) 0%, hsl(142 40% 25%) 50%, hsl(142 38% 22%) 100%)",
            borderBottom: "2px solid hsl(142 30% 40% / 0.5)",
          }}
        >
          {/* Watermark */}
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none select-none z-0">
            <span className="bebas text-5xl opacity-[0.04] text-white tracking-widest">SUNDAY SCORE</span>
          </div>
          {/* @nbarrow27 watermark */}
          <div className="absolute bottom-2 right-4 pointer-events-none select-none z-0">
            <span className="text-[10px] opacity-[0.18] text-white font-medium">@nbarrow27</span>
          </div>

          {/* Yard line dividers */}
          <div className="absolute inset-0 opacity-20 pointer-events-none">
            {[...Array(9)].map((_, i) => (
              <div
                key={i}
                className="absolute top-0 bottom-0 border-l border-white/30"
                style={{ left: `${(i + 1) * 10}%` }}
              />
            ))}
          </div>

          <div className="relative z-10 space-y-3">
            {/* WR / TE row */}
            <FieldRow label="WR / TE" players={[...wrs, ...tes]} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
            {/* RB row */}
            <FieldRow label="RB" players={rbs} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
            {/* QB row */}
            <FieldRow label="QB" players={qbs} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
            {/* OL row */}
            <div className="relative">
              <div className="absolute inset-0 flex items-center pointer-events-none">
                <div className="w-full border-t-2 border-white/50" />
              </div>
              <div className="relative flex justify-center">
                <span className="px-2 text-[9px] text-white/60 font-bold uppercase tracking-widest" style={{ background: "hsl(142 38% 22%)" }}>
                  Line of Scrimmage
                </span>
              </div>
            </div>
            <FieldRow label="O-LINE" players={ols} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
          </div>
        </div>
      </div>

      {/* DEFENSE */}
      <div className="glass-card rounded-xl overflow-hidden">
        <div
          className="relative p-4"
          style={{
            background: "linear-gradient(180deg, hsl(218 30% 18%) 0%, hsl(218 28% 16%) 50%, hsl(218 30% 18%) 100%)",
          }}
        >
          {/* Watermark */}
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none select-none z-0">
            <span className="bebas text-5xl opacity-[0.04] text-white tracking-widest">SUNDAY SCORE</span>
          </div>

          <div className="relative z-10 space-y-3">
            <FieldRow label="D-LINE" players={dls} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
            <FieldRow label="LB" players={lbs} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
            <FieldRow label="DB / S" players={dbs} onPlayerClick={onPlayerClick} selectedPlayer={selectedPlayer} />
          </div>
        </div>
        <div className="px-4 py-2.5 border-t border-border/50 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="bebas text-base text-primary tracking-widest">SundayScore</span>
            <span className="text-xs text-muted-foreground">·</span>
            <span className="text-xs font-bold text-blue-400 uppercase tracking-widest">Defense</span>
          </div>
        </div>
      </div>

      {/* Special teams */}
      {specialTeams.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs font-bold text-purple-400 uppercase tracking-widest">Special Teams</span>
          </div>
          <div className="flex flex-wrap gap-3">
            {specialTeams.map(p => (
              <PlayerChip key={p.id} player={p} onClick={() => onPlayerClick(p)} isSelected={selectedPlayer?.id === p.id} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function FieldRow({
  label,
  players,
  onPlayerClick,
  selectedPlayer,
}: {
  label: string;
  players: Player[];
  onPlayerClick: (p: Player) => void;
  selectedPlayer: Player | null;
}) {
  if (players.length === 0) return null;
  return (
    <div className="flex items-center gap-2">
      <span className="text-[9px] text-white/40 font-bold uppercase tracking-widest w-10 flex-shrink-0 text-right">{label}</span>
      <div className="flex flex-1 justify-center gap-2 flex-wrap">
        {players.map(p => (
          <PlayerChip key={p.id} player={p} onClick={() => onPlayerClick(p)} isSelected={selectedPlayer?.id === p.id} />
        ))}
      </div>
    </div>
  );
}

// ─── Player Chip (field card) ────────────────────────────────────────────────
function PlayerChip({
  player,
  onClick,
  isSelected,
}: {
  player: Player;
  onClick: () => void;
  isSelected: boolean;
}) {
  const dnp = didNotPlay(player);
  const scoreBg = dnp ? "hsl(220 15% 35%)" : getScoreBg(player.sundayScore);
  const textColor = dnp ? "hsl(220 10% 65%)" : (player.sundayScore >= 5.5 ? "white" : "#1a1a1a");

  const nameParts = player.name?.split(" ") || [];
  const lastName = nameParts.length > 1 ? nameParts.slice(1).join(" ") : nameParts[0] || "";

  return (
    <button
      data-testid={`player-chip-${player.id}`}
      onClick={onClick}
      className={`relative flex flex-col items-center gap-1 player-card-hover cursor-pointer p-1 rounded-xl ${
        isSelected ? "ring-2 ring-primary bg-black/20" : "hover:bg-black/10"
      } ${dnp ? "opacity-40" : ""}`}
      style={{ minWidth: 60 }}
    >
      {/* Headshot or avatar */}
      <div
        className="w-11 h-11 rounded-full overflow-hidden flex-shrink-0 flex items-center justify-center text-sm font-bold text-white"
        style={{
          border: `2.5px solid ${scoreBg}`,
          boxShadow: dnp ? "none" : `0 0 8px ${scoreBg}40`,
          background: player.headshot ? "transparent" : "hsl(218 25% 20%)",
        }}
      >
        {player.headshot ? (
          <img src={player.headshot} alt={player.name} className="w-full h-full object-cover" />
        ) : (
          <span style={{ color: scoreBg }}>{player.jersey || player.name?.[0]}</span>
        )}
      </div>

      {/* Name */}
      <span className="text-[9px] text-white/85 font-semibold leading-none max-w-[56px] text-center truncate">
        {lastName}
      </span>

      {/* Score badge — grey N/A for DNP */}
      <div
        className="px-2 py-0.5 rounded-md text-[11px] font-black leading-none shadow-sm"
        style={{ background: scoreBg, color: textColor }}
      >
        {dnp ? "N/A" : player.sundayScore.toFixed(1)}
      </div>

      {/* Snap count */}
      {!dnp && player.snapsPlayed != null && player.snapsPlayed > 0 && (
        <span className="text-[8px] text-white/40 leading-none">
          {player.snapsPlayed} snaps
        </span>
      )}
    </button>
  );
}

// ─── All Players View ─────────────────────────────────────────────────────────
function AllPlayersView({
  players,
  onPlayerClick,
  selectedPlayer,
}: {
  players: Player[];
  onPlayerClick: (p: Player) => void;
  selectedPlayer: Player | null;
}) {
  const groups: Record<string, Player[]> = {};
  for (const p of players) {
    const g = getGroupLabel(p.position);
    if (!groups[g]) groups[g] = [];
    groups[g].push(p);
  }

  // Sort each group: DNP players at the bottom, rest by score descending
  for (const g of Object.keys(groups)) {
    groups[g].sort((a, b) => {
      const aDnp = didNotPlay(a), bDnp = didNotPlay(b);
      if (aDnp && !bDnp) return 1;
      if (!aDnp && bDnp) return -1;
      return b.sundayScore - a.sundayScore;
    });
  }

  const groupOrder = ["QB", "RB/FB", "WR", "TE", "OL", "DL", "LB", "DB", "ST", "Other"];

  return (
    <div className="space-y-4">
      {groupOrder.filter(g => groups[g]?.length > 0).map(g => (
        <div key={g} className="glass-card rounded-xl overflow-hidden">
          <div className="px-4 py-2 border-b border-border/50">
            <span className="text-xs font-bold text-muted-foreground uppercase tracking-widest">{g}</span>
          </div>
          <div className="p-3 flex flex-col gap-2">
            {groups[g].map(player => (
              <PlayerRow
                key={player.id}
                player={player}
                onClick={() => onPlayerClick(player)}
                isSelected={selectedPlayer?.id === player.id}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function PlayerRow({
  player,
  onClick,
  isSelected,
}: {
  player: Player;
  onClick: () => void;
  isSelected: boolean;
}) {
  const dnp = didNotPlay(player);
  const scoreBg = dnp ? "hsl(220 15% 30%)" : getScoreBg(player.sundayScore);
  const textColor = dnp ? "hsl(220 10% 55%)" : (player.sundayScore >= 5.5 ? "white" : "#1a1a1a");
  const ringColor = dnp ? "hsl(220 15% 35%)" : scoreBg;

  return (
    <button
      data-testid={`player-row-${player.id}`}
      onClick={onClick}
      className={`flex items-center gap-3 px-3 py-2 rounded-lg text-left transition-all cursor-pointer ${
        isSelected
          ? "bg-primary/10 ring-1 ring-primary"
          : "hover:bg-secondary/50"
      } ${dnp ? "opacity-50" : ""}`}
    >
      {/* Headshot */}
      <div
        className="w-8 h-8 rounded-full overflow-hidden border-2 flex-shrink-0 flex items-center justify-center text-xs font-bold"
        style={{
          borderColor: ringColor,
          background: player.headshot ? "transparent" : "hsl(218 25% 20%)",
        }}
      >
        {player.headshot ? (
          <img src={player.headshot} alt={player.name} className="w-full h-full object-cover" />
        ) : (
          <span style={{ color: ringColor }}>{player.jersey || player.name?.[0]}</span>
        )}
      </div>

      {/* Name + position */}
      <div className="flex-1 min-w-0">
        <p className={`text-sm font-semibold truncate ${dnp ? "text-muted-foreground" : "text-foreground"}`}>{player.name}</p>
        <p className="text-xs text-muted-foreground">
          {player.position} #{player.jersey}
          {dnp ? " · Did not play" : (
            player.snapsPlayed != null && player.snapsPlayed > 0
              ? ` · ${player.snapsPlayed} snaps`
              : ""
          )}
        </p>
      </div>

      {/* Quick stats — hidden for DNP */}
      {!dnp && (
        <div className="hidden sm:block text-xs text-muted-foreground">
          <QuickStats player={player} />
        </div>
      )}

      {/* Score — grey N/A badge for DNP */}
      <div
        className="w-12 h-8 rounded-lg flex items-center justify-center font-black text-sm flex-shrink-0"
        style={{ background: scoreBg, color: textColor }}
      >
        {dnp ? "N/A" : player.sundayScore.toFixed(1)}
      </div>
    </button>
  );
}

function QuickStats({ player }: { player: Player }) {
  const s = player.stats;
  const pos = player.position?.toUpperCase();

  if (pos === "QB" && s.passing) {
    return <span>{s.passing["C/ATT"]} · {s.passing["YDS"]}yds · {s.passing["TD"]}TD</span>;
  }
  if ((pos === "RB" || pos === "FB") && s.rushing) {
    return <span>{s.rushing["CAR"]} car · {s.rushing["YDS"]}yds</span>;
  }
  if ((pos === "WR" || pos === "TE") && s.receiving) {
    return <span>{s.receiving["REC"]} rec · {s.receiving["YDS"]}yds</span>;
  }
  if (s.defensive) {
    const def = s.defensive;
    return <span>{def["TOT"]} tkl · {def["SACKS"]} sk</span>;
  }
  return null;
}

// ─── Player Detail Panel ──────────────────────────────────────────────────────
function PlayerDetailPanel({
  player,
  teamColor,
  onClose,
}: {
  player: Player;
  teamColor?: string;
  onClose: () => void;
}) {
  const scoreBg = getScoreBg(player.sundayScore);
  const textColor = player.sundayScore >= 5.5 ? "white" : "#1a1a1a";
  const gradeLabel = getGradeLabel(player.sundayScore);

  return (
    <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center p-4" onClick={onClose}>
      <div
        className="w-full max-w-md rounded-2xl overflow-hidden shadow-2xl"
        style={{ background: "hsl(218 25% 12%)", border: "1px solid hsl(218 22% 22%)" }}
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div
          className="p-5 flex items-center gap-4"
          style={{ background: `hsl(218 28% 16%)` }}
        >
          <div
            className="w-14 h-14 rounded-full overflow-hidden border-3 flex-shrink-0 flex items-center justify-center font-bold text-xl"
            style={{
              borderWidth: 3,
              borderColor: scoreBg,
              background: player.headshot ? "transparent" : "hsl(218 25% 22%)",
            }}
          >
            {player.headshot ? (
              <img src={player.headshot} alt={player.name} className="w-full h-full object-cover" />
            ) : (
              <span style={{ color: scoreBg }}>{player.jersey}</span>
            )}
          </div>
          <div className="flex-1 min-w-0">
            <p className="font-bold text-lg text-foreground leading-tight truncate">{player.name}</p>
            <p className="text-sm text-muted-foreground">{player.position} · #{player.jersey}</p>
          </div>
          <div className="flex flex-col items-center gap-1">
            <div
              className="w-14 h-14 rounded-xl flex items-center justify-center font-black text-2xl"
              style={{
                background: didNotPlay(player) ? "hsl(220 15% 28%)" : scoreBg,
                color: didNotPlay(player) ? "hsl(220 10% 55%)" : textColor
              }}
            >
              {didNotPlay(player) ? "N/A" : player.sundayScore.toFixed(1)}
            </div>
            <span className="text-[10px] font-bold uppercase tracking-wider" style={{ color: didNotPlay(player) ? "hsl(220 10% 45%)" : scoreBg }}>
              {didNotPlay(player) ? "Inactive" : gradeLabel}
            </span>
          </div>
        </div>

        {/* Stats */}
        <div className="p-5">
          {didNotPlay(player) ? (
            <div className="flex items-center gap-2 px-4 py-3 rounded-xl" style={{ background: "hsl(220 15% 18%)" }}>
              <span className="text-sm text-muted-foreground">This player did not take a snap in this game.</span>
            </div>
          ) : (
            <>
              <h3 className="text-xs font-bold text-muted-foreground uppercase tracking-widest mb-3">Game Stats</h3>
              <StatsTable stats={player.stats} position={player.position} />
            </>
          )}
        </div>

        {/* Grading explanation */}
        <div className="px-5 pb-4">
          <GradingExplanation player={player} />
        </div>

        <button
          onClick={onClose}
          className="w-full py-3 text-sm text-muted-foreground hover:text-foreground transition-colors border-t border-border/50"
        >
          Close
        </button>
      </div>
    </div>
  );
}

function StatsTable({ stats, position }: { stats: Record<string, Record<string, string>>; position: string }) {
  const pos = position?.toUpperCase();
  const entries: { label: string; value: string }[] = [];

  if (stats.passing) {
    const p = stats.passing;
    if (p["C/ATT"]) entries.push({ label: "Completions/Att", value: p["C/ATT"] });
    if (p["YDS"]) entries.push({ label: "Pass Yards", value: `${p["YDS"]} yds` });
    if (p["TD"]) entries.push({ label: "Pass TDs", value: p["TD"] });
    if (p["INT"]) entries.push({ label: "Interceptions", value: p["INT"] });
    if (p["QBR"]) entries.push({ label: "ESPN QBR", value: p["QBR"] });
    if (p["RTG"]) entries.push({ label: "Passer Rating", value: p["RTG"] });
    if (p["SACKS"]) entries.push({ label: "Sacks Taken", value: p["SACKS"] });
  }

  if (stats.rushing) {
    const r = stats.rushing;
    if (r["CAR"]) entries.push({ label: "Carries", value: r["CAR"] });
    if (r["YDS"]) entries.push({ label: "Rush Yards", value: `${r["YDS"]} yds` });
    if (r["AVG"]) entries.push({ label: "Yards/Carry", value: r["AVG"] });
    if (r["TD"]) entries.push({ label: "Rush TDs", value: r["TD"] });
  }

  if (stats.receiving) {
    const rec = stats.receiving;
    if (rec["TGTS"]) entries.push({ label: "Targets", value: rec["TGTS"] });
    if (rec["REC"]) entries.push({ label: "Receptions", value: rec["REC"] });
    if (rec["YDS"]) entries.push({ label: "Rec Yards", value: `${rec["YDS"]} yds` });
    if (rec["AVG"]) entries.push({ label: "Yards/Rec", value: rec["AVG"] });
    if (rec["TD"]) entries.push({ label: "Rec TDs", value: rec["TD"] });
  }

  if (stats.defensive) {
    const d = stats.defensive;
    if (d["TOT"]) entries.push({ label: "Total Tackles", value: d["TOT"] });
    if (d["SOLO"]) entries.push({ label: "Solo Tackles", value: d["SOLO"] });
    if (d["SACKS"]) entries.push({ label: "Sacks", value: d["SACKS"] });
    if (d["TFL"]) entries.push({ label: "Tackles For Loss", value: d["TFL"] });
    if (d["PD"]) entries.push({ label: "Pass Defensed", value: d["PD"] });
    if (d["QB HTS"]) entries.push({ label: "QB Hits", value: d["QB HTS"] });
    if (d["TD"]) entries.push({ label: "Defensive TDs", value: d["TD"] });
  }

  if (stats.interceptions) {
    const i = stats.interceptions;
    if (i["INT"]) entries.push({ label: "Interceptions", value: i["INT"] });
    if (i["YDS"]) entries.push({ label: "INT Return Yds", value: i["YDS"] });
  }

  if (stats.kicking) {
    const k = stats.kicking;
    if (k["FG"]) entries.push({ label: "FGs (Made/Att)", value: k["FG"] });
    if (k["LONG"]) entries.push({ label: "Long FG", value: `${k["LONG"]} yds` });
    if (k["XP"]) entries.push({ label: "Extra Points", value: k["XP"] });
    if (k["PTS"]) entries.push({ label: "Points Scored", value: k["PTS"] });
  }

  if (stats.punting) {
    const pu = stats.punting;
    if (pu["NO"]) entries.push({ label: "Punts", value: pu["NO"] });
    if (pu["YDS"]) entries.push({ label: "Total Punt Yards", value: pu["YDS"] });
    if (pu["AVG"]) entries.push({ label: "Punt Average", value: `${pu["AVG"]} yds` });
    if (pu["In 20"]) entries.push({ label: "Inside 20", value: pu["In 20"] });
  }

  if (entries.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        {pos === "OT" || pos === "OG" || pos === "C" || pos === "G" || pos === "T"
          ? "Offensive linemen don't accumulate traditional box score stats. Grade is based on team context and snap performance."
          : "No detailed stats available for this player in this game."}
      </p>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-2">
      {entries.map(e => (
        <div key={e.label} className="bg-secondary/50 rounded-lg px-3 py-2">
          <p className="text-[10px] text-muted-foreground uppercase tracking-wide">{e.label}</p>
          <p className="text-sm font-bold text-foreground">{e.value}</p>
        </div>
      ))}
    </div>
  );
}

function GradingExplanation({ player }: { player: Player }) {
  const pos = player.position?.toUpperCase();
  let text = "";

  if (pos === "QB") {
    text = "Graded on: completion %, passing yards, TD:INT ratio, ESPN QBR, passer rating, yards per attempt, and rushing contribution.";
  } else if (pos === "RB" || pos === "FB") {
    text = "Graded on: rushing yards, yards per carry, TDs, receiving contribution, and ball security (fumbles penalized heavily).";
  } else if (pos === "WR" || pos === "TE") {
    text = "Graded on: yards, catch rate vs targets, TDs, yards per reception, and blocking contribution.";
  } else if (["OT", "OG", "C", "G", "T", "OL", "LS"].includes(pos)) {
    text = "Individually graded from play-by-play: penalties committed (holding −1.0, false start −0.6), gap-specific rush yards through this player's zone (vs. league avg 4.2 YPC), run blocking volume, rushing TDs through the gap, and shared unit grade for sacks allowed (0 sacks = +1.0, 5+ sacks = −1.6).";
  } else if (["DE", "DT", "NT", "DL", "IDL", "ED"].includes(pos)) {
    text = "Graded on: sacks, QB hits (pressure proxy), tackles for loss, general stops, and pass defensed.";
  } else if (["LB", "ILB", "OLB", "MLB"].includes(pos)) {
    text = "Graded on: tackle volume & quality, sacks, QB hits, TFLs, coverage plays, and interceptions.";
  } else if (pos === "CB") {
    text = "Coverage-first grading: LOW targets = HIGH score (QBs avoid elite CBs). Key signals: target rate vs. snaps played, catch rate allowed, yards per target, TDs allowed, contested catch battles. A lockdown CB who forces 0 targets on 50+ snaps floors at 7.0. PDs and INTs from box score further boost the grade. Tackles alone are NOT rewarded — many tackles often means many catches allowed.";
  } else if (["S", "SS", "FS", "SAF"].includes(pos)) {
    text = "Graded on: tackles, pass defensed, interceptions, TFLs, and blitz/pass rush contribution.";
  }

  if (!text) return null;

  return (
    <div className="flex gap-2 bg-secondary/30 rounded-lg p-3">
      <Info className="w-4 h-4 text-muted-foreground flex-shrink-0 mt-0.5" />
      <p className="text-xs text-muted-foreground">{text}</p>
    </div>
  );
}

// ─── Score Legend ─────────────────────────────────────────────────────────────
function ScoreLegend() {
  const levels = [
    { range: "9.0–10", label: "Elite", bg: "hsl(142 71% 45%)", text: "white" },
    { range: "7.5–8.9", label: "Great", bg: "hsl(142 60% 50%)", text: "white" },
    { range: "6.5–7.4", label: "Good", bg: "hsl(82 60% 45%)", text: "white" },
    { range: "5.5–6.4", label: "Average", bg: "hsl(43 96% 56%)", text: "#1a1a1a" },
    { range: "4.5–5.4", label: "Below", bg: "hsl(25 95% 53%)", text: "white" },
    { range: "1.0–4.4", label: "Poor", bg: "hsl(0 72% 51%)", text: "white" },
  ];

  return (
    <div className="mt-6 glass-card rounded-xl p-4">
      <p className="text-xs font-bold text-muted-foreground uppercase tracking-widest mb-3">Score Scale</p>
      <div className="flex flex-wrap gap-2">
        {levels.map(l => (
          <div key={l.range} className="flex items-center gap-2">
            <div className="w-8 h-5 rounded text-[9px] font-black flex items-center justify-center" style={{ background: l.bg, color: l.text }}>
              {l.range.split("–")[0]}
            </div>
            <span className="text-xs text-muted-foreground">{l.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Methodology Note ─────────────────────────────────────────────────────────
function MethodologyNote() {
  return (
    <div className="mt-4 px-4 py-3 rounded-xl border border-border/30 text-xs text-muted-foreground/60 leading-relaxed">
      <strong className="text-muted-foreground">SundayScore Methodology:</strong> Ratings start at 5.0 and are position-weighted using ESPN box-score stats.
      QBs: completion %, EPA/QBR, TD:INT, Y/A. RBs: Y/C, YAC, TDs, fumbles. WRs/TEs: catch rate, yards, separation proxy.
      Defense: tackles, sacks/QBH, TFLs, coverage plays, INTs. O-Line: graded on sacks allowed + rushing efficiency (unit-level grade).
      Inspired by SofaScore. Stats via ESPN API. <strong className="text-muted-foreground/80">@nbarrow27</strong>
    </div>
  );
}

function getGradeLabel(score: number): string {
  if (score >= 9.0) return "Elite";
  if (score >= 7.5) return "Great";
  if (score >= 6.5) return "Good";
  if (score >= 5.5) return "Average";
  if (score >= 4.5) return "Below Avg";
  return "Poor";
}

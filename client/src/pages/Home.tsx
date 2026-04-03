import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { apiRequest } from "@/lib/queryClient";
import { ChevronDown, Star, TrendingUp, Shield } from "lucide-react";

export default function Home() {
  const [selectedSeason, setSelectedSeason] = useState<number>(2025);
  const [selectedWeek, setSelectedWeek] = useState<string>("2-1");
  const [, navigate] = useLocation();

  const { data: seasonsData } = useQuery({
    queryKey: ["/api/seasons"],
    queryFn: () => apiRequest("GET", "/api/seasons").then(r => r.json()),
  });

  const { data: weeksData } = useQuery({
    queryKey: ["/api/seasons", selectedSeason, "weeks"],
    queryFn: () => apiRequest("GET", `/api/weeks`).then(r => r.json()),
  });

  const { data: gamesData, isLoading: gamesLoading } = useQuery({
    queryKey: ["/api/games", selectedSeason, selectedWeek],
    queryFn: () => apiRequest("GET", `/api/games?year=${selectedSeason}&week=${selectedWeek}`).then(r => r.json()),
    enabled: !!selectedWeek,
  });

  const seasons = seasonsData?.seasons || [];
  const weeks = weeksData?.weeks || [];
  const games = gamesData?.games || [];

  return (
    <div className="min-h-screen" style={{ background: "hsl(218 28% 8%)" }}>
      {/* Header */}
      <header className="border-b border-border/50 px-4 py-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          {/* Logo */}
          <div className="flex items-center gap-3">
            <svg width="40" height="40" viewBox="0 0 40 40" fill="none" aria-label="SundayScore" className="flex-shrink-0">
              <rect width="40" height="40" rx="8" fill="hsl(43 96% 56%)" />
              <ellipse cx="20" cy="20" rx="10" ry="14" stroke="hsl(218 28% 8%)" strokeWidth="2" fill="none" />
              <line x1="10" y1="20" x2="30" y2="20" stroke="hsl(218 28% 8%)" strokeWidth="1.5" />
              <line x1="12" y1="14" x2="28" y2="14" stroke="hsl(218 28% 8%)" strokeWidth="1" strokeDasharray="2 2" />
              <line x1="12" y1="26" x2="28" y2="26" stroke="hsl(218 28% 8%)" strokeWidth="1" strokeDasharray="2 2" />
            </svg>
            <div>
              <h1 className="bebas text-2xl text-[hsl(43_96%_56%)] leading-none tracking-widest">SundayScore</h1>
              <p className="text-xs text-muted-foreground leading-none mt-0.5">NFL Player Ratings</p>
            </div>
          </div>

          {/* Pills */}
          <div className="hidden md:flex items-center gap-3 text-xs text-muted-foreground">
            <span className="flex items-center gap-1"><Star className="w-3 h-3 text-yellow-400" /> 1–10 Scale</span>
            <span className="flex items-center gap-1"><TrendingUp className="w-3 h-3 text-green-400" /> Advanced Stats</span>
            <span className="flex items-center gap-1"><Shield className="w-3 h-3 text-blue-400" /> Position-Weighted</span>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-8">
        {/* Hero */}
        <div className="text-center mb-10">
          <h2 className="bebas text-4xl md:text-5xl text-foreground mb-2 tracking-widest">SELECT A GAME</h2>
          <p className="text-muted-foreground max-w-xl mx-auto text-sm">
            Choose any completed NFL game to see SundayScore ratings for every player — position-weighted grades based on advanced stats.
          </p>
        </div>

        {/* Selectors */}
        <div className="glass-card rounded-xl p-5 mb-8">
          <div className="flex flex-wrap gap-4 items-end">
            {/* Season */}
            <div className="flex-1 min-w-[120px]">
              <label className="text-xs text-muted-foreground mb-1.5 block font-medium uppercase tracking-wider">Season</label>
              <div className="relative">
                <select
                  data-testid="select-season"
                  value={selectedSeason}
                  onChange={e => setSelectedSeason(Number(e.target.value))}
                  className="w-full appearance-none bg-secondary border border-border rounded-lg px-3 py-2.5 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-primary cursor-pointer"
                >
                  {seasons.map((y: number) => (
                    <option key={y} value={y}>{y} Season</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
              </div>
            </div>

            {/* Week */}
            <div className="flex-1 min-w-[140px]">
              <label className="text-xs text-muted-foreground mb-1.5 block font-medium uppercase tracking-wider">Week</label>
              <div className="relative">
                <select
                  data-testid="select-week"
                  value={selectedWeek}
                  onChange={e => setSelectedWeek(e.target.value)}
                  className="w-full appearance-none bg-secondary border border-border rounded-lg px-3 py-2.5 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-primary cursor-pointer"
                >
                  {weeks.map((w: any) => (
                    <option key={w.value} value={w.value}>{w.label}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
              </div>
            </div>
          </div>
        </div>

        {/* Games grid */}
        {gamesLoading ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {[...Array(6)].map((_, i) => (
              <div key={i} className="glass-card rounded-xl p-5 shimmer h-28" />
            ))}
          </div>
        ) : games.length === 0 ? (
          <div className="text-center py-16 text-muted-foreground">
            <p className="text-lg">No completed games found for this selection.</p>
            <p className="text-sm mt-1">Try selecting a different week or season.</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {games.map((game: any) => (
              <GameCard key={game.id} game={game} onClick={() => navigate(`/game/${game.id}`)} />
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

function GameCard({ game, onClick }: { game: any; onClick: () => void }) {
  const homeScore = parseInt(game.homeTeam?.score || "0");
  const awayScore = parseInt(game.awayTeam?.score || "0");
  const homeWon = homeScore > awayScore;
  const awayWon = awayScore > homeScore;

  return (
    <button
      data-testid={`game-card-${game.id}`}
      onClick={onClick}
      className="glass-card rounded-xl p-5 text-left player-card-hover w-full cursor-pointer group"
    >
      <div className="flex items-center justify-between gap-3">
        {/* Away team */}
        <div className={`flex-1 flex flex-col items-center gap-2 ${awayWon ? 'opacity-100' : 'opacity-60'}`}>
          {game.awayTeam?.logo && (
            <img src={game.awayTeam.logo} alt={game.awayTeam.abbreviation} className="w-10 h-10 object-contain" />
          )}
          <span className="text-xs font-bold text-foreground/80">{game.awayTeam?.abbreviation}</span>
          <span className={`bebas text-3xl leading-none ${awayWon ? 'text-foreground' : 'text-muted-foreground'}`}>
            {game.awayTeam?.score}
          </span>
        </div>

        {/* VS */}
        <div className="text-center">
          <span className="text-xs text-muted-foreground font-bold">@</span>
        </div>

        {/* Home team */}
        <div className={`flex-1 flex flex-col items-center gap-2 ${homeWon ? 'opacity-100' : 'opacity-60'}`}>
          {game.homeTeam?.logo && (
            <img src={game.homeTeam.logo} alt={game.homeTeam.abbreviation} className="w-10 h-10 object-contain" />
          )}
          <span className="text-xs font-bold text-foreground/80">{game.homeTeam?.abbreviation}</span>
          <span className={`bebas text-3xl leading-none ${homeWon ? 'text-foreground' : 'text-muted-foreground'}`}>
            {game.homeTeam?.score}
          </span>
        </div>
      </div>

      {/* CTA */}
      <div className="mt-4 text-center">
        <span className="text-xs text-primary group-hover:text-yellow-300 transition-colors font-semibold">
          View SundayScores →
        </span>
      </div>
    </button>
  );
}

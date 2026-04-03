// ── SundayScore shared API logic for Vercel serverless functions ──────────────
const ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football/nfl";
const ESPN_CORE = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl";
const NFLVERSE_BASE = "https://github.com/nflverse/nflverse-data/releases/download";
const CACHE_TTL = 60 * 60 * 1000;

interface CacheEntry { data: string; fetchedAt: number; }
const gameCache: Record<string, CacheEntry> = {};
export function getCached(id: string): CacheEntry | null {
  const e = gameCache[id];
  return (e && Date.now() - e.fetchedAt < CACHE_TTL) ? e : null;
}
export function setCached(id: string, data: string) {
  gameCache[id] = { data, fetchedAt: Date.now() };
}

// Simple CSV parser that handles quoted fields
function parseCSV(text: string): Record<string, string>[] {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map(h => h.replace(/^"|"$/g, "").trim());
  return lines.slice(1).map(line => {
    const vals: string[] = [];
    let cur = "", inQ = false;
    for (const ch of line) {
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { vals.push(cur); cur = ""; }
      else cur += ch;
    }
    vals.push(cur);
    const row: Record<string, string> = {};
    headers.forEach((h, i) => { row[h] = (vals[i] || "").trim(); });
    return row;
  });
}

async function fetchCSV(url: string): Promise<Record<string, string>[]> {
  const res = await fetch(url, {
    headers: { "User-Agent": "SundayScore/1.0", "Accept": "text/csv,text/plain,*/*" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`CSV fetch ${res.status}: ${url}`);
  const text = await res.text();
  return parseCSV(text);
}

// Helper: parse a single CSV line into values array
function parseCsvLine(line: string): string[] {
  const vals: string[] = [];
  let cur = "", inQ = false;
  for (const ch of line) {
    if (ch === '"') inQ = !inQ;
    else if (ch === ',' && !inQ) { vals.push(cur); cur = ""; }
    else cur += ch;
  }
  vals.push(cur);
  return vals;
}

// TRUE streaming CSV fetch — reads the response body chunk by chunk,
// processes each line without loading the entire file into memory.
// Critical for large files like PBP (~99MB) and participation (~48MB).
async function fetchCSVFiltered(
  url: string,
  filter: (row: Record<string, string>) => boolean
): Promise<Record<string, string>[]> {
  const res = await fetch(url, {
    headers: { "User-Agent": "SundayScore/1.0", "Accept": "text/csv,text/plain,*/*" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`CSV fetch ${res.status}: ${url}`);

  if (!res.body) {
    // Fallback: no body stream, use text() but warn
    const text = await res.text();
    const lines = text.split("\n");
    if (lines.length < 2) return [];
    const headers = lines[0].split(",").map(h => h.replace(/^"|"$/g, "").trim());
    const results: Record<string, string>[] = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      const vals = parseCsvLine(line);
      const row: Record<string, string> = {};
      headers.forEach((h, j) => { row[h] = (vals[j] || "").trim(); });
      if (filter(row)) results.push(row);
    }
    return results;
  }

  // Stream processing: accumulate chunks into lines
  const reader = res.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let headers: string[] | null = null;
  const results: Record<string, string>[] = [];
  let leftover = "";

  while (true) {
    const { done, value } = await reader.read();
    const chunk = done ? "" : decoder.decode(value, { stream: !done });
    const text = leftover + chunk;
    const lines = text.split("\n");
    // Keep the last (potentially incomplete) line for the next chunk
    leftover = done ? "" : lines.pop()!;

    for (const rawLine of lines) {
      const line = rawLine.trimEnd(); // preserve leading whitespace in values, trim \r
      if (!line) continue;

      if (headers === null) {
        // First line = header
        headers = parseCsvLine(line).map(h => h.replace(/^"|"$/g, "").trim());
        continue;
      }

      const vals = parseCsvLine(line);
      const row: Record<string, string> = {};
      headers.forEach((h, j) => { row[h] = (vals[j] || "").replace(/^"|"$/g, "").trim(); });
      if (filter(row)) results.push(row);
    }

    if (done) break;
  }

  return results;
}

// Resolve ESPN gameId -> nflverse game_id (e.g. "2025_01_DAL_PHI") via the schedules CSV
// Returns null on failure so callers can degrade gracefully
const nflverseGameIdCache: Record<string, string> = {};
async function resolveNflverseGameId(espnGameId: string, season: number): Promise<string | null> {
  const cacheKey = `${espnGameId}`;
  if (nflverseGameIdCache[cacheKey]) return nflverseGameIdCache[cacheKey];
  try {
    const url = `${NFLVERSE_BASE}/schedules/games.csv`;
    const rows = await fetchCSV(url);
    for (const row of rows) {
      if (row["espn"] === espnGameId) {
        nflverseGameIdCache[cacheKey] = row["game_id"];
        return row["game_id"];
      }
    }
  } catch {}
  return null;
}

async function fetchJson(url: string): Promise<any> {
  const res = await fetch(url, {
    headers: { "User-Agent": "SundayScore/1.0 NFL Stats App" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return res.json();
}

// Static map of ESPN position ID -> abbreviation
// Discovered via ESPN Core API /positions/{id} endpoint
const ESPN_POSITION_MAP: Record<string, string> = {
  "1":  "WR",
  "2":  "ATH",
  "3":  "K",
  "4":  "C",
  "5":  "DB",
  "6":  "DE",   // some leagues use this
  "7":  "TE",
  "8":  "QB",
  "9":  "RB",
  "10": "FB",
  "11": "OL",
  "12": "DL",
  "13": "ILB",
  "14": "OLB",
  "15": "MLB",
  "16": "WS",
  "17": "SS",
  "18": "CB",   // sometimes
  "19": "NT",
  "20": "OT",
  "21": "OG",
  "22": "PK",
  "23": "P",
  "24": "LS",
  "25": "DT",
  "26": "DE",
  "27": "LB",
  "28": "CB",
  "29": "CB",
  "30": "LB",
  "31": "DE",
  "32": "DT",
  "33": "NT",
  "34": "ILB",
  "35": "OLB",
  "36": "S",
  "37": "SS",
  "38": "FS",
  "39": "SAF",
  "40": "PR",
  "41": "KR",
  "42": "LS",
  "43": "H",
  "44": "K",
  "45": "P",
  "46": "OT",
  "47": "OG",
  "48": "C",
  "73": "G",
  "74": "OT",
  "75": "NT",
  "76": "PR",
  "77": "KR",
  "78": "LS",
  "79": "H",
  "80": "PK",
};

function extractPositionFromRef(ref: string): string | null {
  const m = ref.match(/\/positions\/(\d+)/);
  if (!m) return null;
  return ESPN_POSITION_MAP[m[1]] || null;
}

// Fetch seasons available (2016-current)
function getAvailableSeasons() {
  const current = new Date().getFullYear();
  const seasons = [];
  for (let y = current; y >= 2016; y--) {
    seasons.push(y);
  }
  return seasons;
}

function computeSundayScore(player: any): number {
  const stats = player.stats || {};
  const pos = player.position || "?";

  // Base score starts at 6.5 (like SofaScore)
  let score = 6.5;

  const teamCtx = player._teamCtx || { sacks: 0, rushYards: 0, rushTDs: 0, rushCarries: 0 };

  if (pos === "QB") {
    score = gradeQB(stats);
  } else if (pos === "RB" || pos === "FB") {
    score = gradeRB(stats);
  } else if (pos === "WR" || pos === "TE") {
    score = gradeWRTE(stats, player._wrStats);
  } else if (pos === "C" || pos === "G" || pos === "T" || pos === "OL" || pos === "OT" || pos === "OG" || pos === "LS") {
    score = gradeOL(stats, teamCtx, player._olStats);
  } else if (pos === "DE" || pos === "DT" || pos === "NT" || pos === "DL" || pos === "IDL" || pos === "ED") {
    score = gradeDL(stats, player._dlStats);
  } else if (pos === "LB" || pos === "ILB" || pos === "OLB" || pos === "MLB") {
    score = gradeLB(stats, player._lbStats);
  } else if (pos === "CB") {
    score = gradeCB(stats, player._cbStats);
  } else if (pos === "S" || pos === "SS" || pos === "FS" || pos === "SAF") {
    score = gradeSafety(stats, player._dlStats); // safeties share pass rush data
  } else if (pos === "K") {
    score = gradeKicker(stats);
  } else if (pos === "P") {
    score = gradePunter(stats);
  } else {
    // Generic defensive or unknown
    if (stats.defensive) {
      score = gradeGenericDef(stats);
    }
  }

  // Clamp to 1-10
  return Math.min(10, Math.max(1, Math.round(score * 10) / 10));
}

function parseNum(val: string | undefined): number {
  if (!val) return 0;
  return parseFloat(val) || 0;
}

function gradeQB(stats: any): number {
  const passing = stats.passing || {};
  const rushing = stats.rushing || {};

  // Parse C/ATT
  const completionStr = passing["C/ATT"] || "0/0";
  const [compStr, attStr] = completionStr.split("/");
  const completions = parseFloat(compStr) || 0;
  const attempts = parseFloat(attStr) || 0;
  const compPct = attempts > 0 ? completions / attempts : 0;

  const yards = parseNum(passing["YDS"]);
  const tds = parseNum(passing["TD"]);
  const ints = parseNum(passing["INT"]);
  const qbr = parseNum(passing["QBR"]);
  const passer_rtg = parseNum(passing["RTG"]);

  // Sacks given up
  const sacksStr = passing["SACKS"] || "0-0";
  const sacksAllowed = parseFloat(sacksStr.split("-")[0]) || 0;

  // Rush stats
  const rushYards = parseNum(rushing["YDS"]);
  const rushTds = parseNum(rushing["TD"]);
  const rushCars = parseNum(rushing["CAR"]);

  let score = 5.0;

  // Completion percentage (league avg ~66%)
  if (compPct >= 0.75) score += 1.0;
  else if (compPct >= 0.68) score += 0.5;
  else if (compPct >= 0.62) score += 0.0;
  else if (compPct >= 0.55) score -= 0.3;
  else if (compPct < 0.50 && attempts > 10) score -= 0.8;

  // Passing yards (adjusted for attempts)
  const yardsPerAtt = attempts > 0 ? yards / attempts : 0;
  if (yards >= 350) score += 1.2;
  else if (yards >= 275) score += 0.8;
  else if (yards >= 200) score += 0.4;
  else if (yards >= 150) score += 0.1;
  else if (yards < 100 && attempts > 15) score -= 0.5;

  // Y/A efficiency
  if (yardsPerAtt >= 9.0) score += 0.8;
  else if (yardsPerAtt >= 7.5) score += 0.4;
  else if (yardsPerAtt < 5.5 && attempts > 10) score -= 0.4;

  // Touchdowns
  score += tds * 0.8;

  // Interceptions
  score -= ints * 1.2;

  // QBR (ESPN's metric, 0-100)
  if (qbr > 0) {
    if (qbr >= 75) score += 0.8;
    else if (qbr >= 60) score += 0.4;
    else if (qbr >= 45) score += 0.1;
    else if (qbr < 30) score -= 0.6;
    else if (qbr < 20) score -= 1.0;
  }

  // Passer rating
  if (passer_rtg >= 115) score += 0.5;
  else if (passer_rtg >= 100) score += 0.2;
  else if (passer_rtg < 70 && attempts > 10) score -= 0.4;
  else if (passer_rtg < 50 && attempts > 10) score -= 0.8;

  // Sacks allowed (bad, but not entirely QB's fault)
  score -= sacksAllowed * 0.15;

  // Rushing contribution
  if (rushCars > 0) {
    const rushYPA = rushYards / rushCars;
    if (rushYards >= 60) score += 0.5;
    else if (rushYards >= 30) score += 0.3;
    score += rushTds * 0.5;
  }

  return score;
}

function gradeRB(stats: any): number {
  const rushing = stats.rushing || {};
  const receiving = stats.receiving || {};
  const fumbles = stats.fumbles || {};

  const carries = parseNum(rushing["CAR"]);
  const rushYards = parseNum(rushing["YDS"]);
  const rushTds = parseNum(rushing["TD"]);
  const avg = carries > 0 ? rushYards / carries : 0;

  const rec = parseNum(receiving["REC"]);
  const recYards = parseNum(receiving["YDS"]);
  const recTds = parseNum(receiving["TD"]);
  const targets = parseNum(receiving["TGTS"]);

  const fumbles_lost = parseNum(fumbles["LOST"]);

  let score = 5.5;

  // Rushing volume and efficiency
  if (carries > 0) {
    if (rushYards >= 150) score += 2.5;
    else if (rushYards >= 120) score += 2.0;
    else if (rushYards >= 100) score += 1.5;
    else if (rushYards >= 80) score += 1.0;
    else if (rushYards >= 60) score += 0.6;
    else if (rushYards >= 40) score += 0.2;
    else if (rushYards < 20 && carries >= 8) score -= 0.3;

    // YPC efficiency
    if (avg >= 7.0) score += 0.8;
    else if (avg >= 5.5) score += 0.5;
    else if (avg >= 4.5) score += 0.2;
    else if (avg < 3.0 && carries >= 8) score -= 0.4;

    // TDs
    score += rushTds * 1.0;
  }

  // Receiving value
  if (rec > 0) {
    score += rec * 0.2;
    if (recYards >= 80) score += 0.7;
    else if (recYards >= 50) score += 0.4;
    score += recTds * 0.8;
  }

  // Fumbles
  score -= fumbles_lost * 1.5;

  return score;
}

function gradeWRTE(stats: any, wrPlayerData?: any): number {
  // WR/TE grading uses two tiers:
  //   1. ESPN box score stats (always available)
  //   2. nflverse enrichment: YAC, created receptions (FTN), air yards, routes run
  //
  // Key insight: a WR who runs precise routes creating separation (is_created_reception)
  // even without huge yardage should score higher than one who pads stats on screens.
  // YAC reflects both route running AND run-after-catch ability.

  const receiving = stats.receiving || {};
  const rushing = stats.rushing || {};
  const fumbles = stats.fumbles || {};

  const rec = parseNum(receiving["REC"]);
  const recYards = parseNum(receiving["YDS"]);
  const recTds = parseNum(receiving["TD"]);
  const targets = parseNum(receiving["TGTS"]);
  const avg = rec > 0 ? recYards / rec : 0;
  const catchRate = targets > 0 ? rec / targets : 1;
  const fumbles_lost = parseNum(fumbles["LOST"]);

  // nflverse enrichment
  const yac = wrPlayerData?.yac ?? 0;
  const createdReceptions = wrPlayerData?.createdReceptions ?? 0;
  const airYards = wrPlayerData?.airYards ?? 0;      // intended air yards on targets
  const drops = wrPlayerData?.drops ?? 0;
  const firstDowns = wrPlayerData?.firstDowns ?? 0;
  const hasRichData = wrPlayerData && wrPlayerData.targets !== undefined;

  let score = 5.0;

  if (targets > 0 || rec > 0) {
    // ── 1. RECEIVING YARDS (primary volume signal) ─────────────────────────────
    if (recYards >= 150) score += 3.2;
    else if (recYards >= 120) score += 2.6;
    else if (recYards >= 100) score += 2.1;
    else if (recYards >= 80)  score += 1.6;
    else if (recYards >= 60)  score += 1.1;
    else if (recYards >= 40)  score += 0.7;
    else if (recYards >= 20)  score += 0.3;

    // ── 2. RECEPTIONS (volume) ─────────────────────────────────────────────────
    score += Math.min(rec * 0.2, 1.0);

    // ── 3. TOUCHDOWNS ──────────────────────────────────────────────────────────
    score += recTds * 1.4;

    // ── 4. YARDS PER RECEPTION (big-play efficiency) ───────────────────────────
    if (avg >= 20) score += 0.8;
    else if (avg >= 16) score += 0.5;
    else if (avg >= 12) score += 0.2;

    // ── 5. CATCH RATE (route precision / QB trust) ─────────────────────────────
    if (targets >= 4) {
      if (catchRate >= 0.85) score += 0.7;
      else if (catchRate >= 0.75) score += 0.4;
      else if (catchRate >= 0.65) score += 0.1;
      else if (catchRate < 0.40)  score -= 0.5;
    }

    // ── 6. NFLVERSE ENRICHMENT ─────────────────────────────────────────────────
    if (hasRichData) {
      // YAC per reception — measures RAC ability and route running after the catch
      const yacPerRec = rec > 0 ? yac / rec : 0;
      if (yacPerRec >= 8)      score += 0.6; // exceptional YAC
      else if (yacPerRec >= 5) score += 0.3;
      else if (yacPerRec >= 3) score += 0.1;

      // Created receptions — FTN credits WR for beating coverage, not just catching easy balls
      // A high created% = separated from coverage through route or RAC
      const createdPct = rec > 0 ? createdReceptions / rec : 0;
      if (createdPct >= 0.5 && rec >= 3) score += 0.5;
      else if (createdPct >= 0.3 && rec >= 2) score += 0.2;

      // First downs — sustain drives
      score += Math.min(firstDowns * 0.15, 0.5);

      // Air yards per target — downfield threat bonus (getting open deep)
      const airYardsPerTarget = targets > 0 ? airYards / targets : 0;
      if (airYardsPerTarget >= 15) score += 0.4; // consistently targeted deep
      else if (airYardsPerTarget >= 10) score += 0.2;

      // Drops penalize the player specifically
      score -= drops * 0.8;
    }
  } else {
    // No targets — player may have run decoy routes, cleared space, or blocked
    score = 5.0;
    // If we have route data showing they were on the field but just not targeted,
    // give a small baseline for contributions we can't fully measure
    if (hasRichData && (wrPlayerData.routes || 0) > 10) score = 5.3;
  }

  // ── 7. RUSHING (jet sweeps, end-arounds) ───────────────────────────────────
  const rushYards = parseNum(rushing["YDS"]);
  if (rushYards > 0) score += Math.min(rushYards * 0.025, 0.6);
  const rushTDs = parseNum(rushing["TD"]);
  score += rushTDs * 1.2;

  // ── 8. FUMBLES ─────────────────────────────────────────────────────────────
  score -= fumbles_lost * 1.8;

  return Math.max(1.0, Math.min(10.0, score));
}

function gradeOL(stats: any, ctx: { sacks: number; rushYards: number; rushTDs: number; rushCarries: number }, olPlayerData?: any): number {
  // Individual OL grading using three sources:
  // 1. Play-by-play text: gap-specific run yards, individual penalty names
  // 2. pbp_participation: was_pressure per pass play (from nflverse)
  // 3. FTN charting: is_qb_fault_sack (unit NOT penalized), n_blitzers (blitz-adjusted pressure rate)
  // 4. nflverse snap_counts: total offensive snaps played

  let score = 6.5;

  // ── 1. INDIVIDUAL PENALTIES ────────────────────────────────────────
  if (olPlayerData) {
    const { holdingPenalties, falseStarts, otherPenalties } = olPlayerData;
    score -= holdingPenalties * 1.1;   // Holding = definitive failure, worst signal
    score -= falseStarts * 0.7;        // False start = concentration/assignment lapse
    score -= otherPenalties * 0.4;
  }

  // ── 2. PASS PROTECTION GRADE ───────────────────────────────────
  // Individual signal: sacks (shared unit penalty, each starter charged equally)
  // Team-level signal: total sacks as context
  // Note: was_pressure from pbp_participation is team-level (same flag for all OL),
  // so it cannot differentiate individual linemen. We use sacks + penalties only.
  // sacksAllowed: use individual tracking if we have it, otherwise fall back to
  // team-level sack count from QB stats. If olPlayerData exists but sacksAllowed === 0
  // AND the team allowed sacks AND the player played significant snaps, distribute
  // team sacks evenly (5 starters share blame equally when individual data is absent).
  // Use the most available snap count: from olPlayerData tracking, or from player-level snapsPlayed
  const totalSnaps = olPlayerData?.offenseSnaps || olPlayerData?.passSnaps
    || stats._snapsPlayed || 0;  // stats._snapsPlayed injected below from player.snapsPlayed
  const isStarter = totalSnaps >= 30;  // played at least half the game
  let sacksAllowed: number;
  if (olPlayerData) {
    // Individual sack tracking from play loop. If we also have team-level sacks (ctx.sacks)
    // and the individual count is LESS than team total, use the team count as the minimum
    // (sacks are a UNIT failure — every starter shares the penalty equally).
    const individualSacks = olPlayerData.sacksAllowed;
    if (isStarter && ctx.sacks > 0) {
      // Always use team sack count for starters: individual count can undercount
      // if the play type parsing missed some sack variants
      sacksAllowed = Math.max(individualSacks, ctx.sacks);
    } else {
      sacksAllowed = individualSacks;
    }
  } else {
    sacksAllowed = isStarter ? ctx.sacks : 0;
  }

  // Sack penalty (shared across all starters)
  // Each starter gets the same sack deduction since we can't identify who was beaten
  if (sacksAllowed === 0) score += 0.8;       // Clean sheet — excellent unit protection
  else if (sacksAllowed === 1) score += 0.2;  // Good
  else if (sacksAllowed === 2) score -= 0.3;  // Below average
  else if (sacksAllowed === 3) score -= 0.7;  // Struggled
  else if (sacksAllowed >= 4) score -= 1.2;   // Dominated by pass rush

  // Pass snap volume bonus: starters who played heavy pass-protection duty
  if (olPlayerData?.passSnaps) {
    const passSnaps = olPlayerData.passSnaps;
    const blitzSnaps = olPlayerData.blitzSnaps || 0;
    // Bonus if they handled significant blitz load without extra sacks
    if (blitzSnaps >= 5 && sacksAllowed <= 1) score += 0.2;
  }

  // ── 3. RUN BLOCKING GRADE ─────────────────────────────────────
  // PFF methodology: evaluates SUSTAINING blocks and moving defenders,
  // not just YPC. We approximate this with gap YPC adjusted for box count.
  if (olPlayerData) {
    const { gapCarries, gapYards, gapTDs, highBoxSnaps } = olPlayerData;
    const gapYPC = gapCarries > 0 ? gapYards / gapCarries : 0;
    // High-box adjustment: a 4.0 YPC against 7-in-box is better than 4.0 against 6-in-box
    const boxAdjFactor = gapCarries > 0 ? Math.min(1.3, 1 + (highBoxSnaps / gapCarries) * 0.15) : 1;
    const adjustedYPC = gapYPC * boxAdjFactor;

    if (gapCarries >= 3) {
      // Adjusted YPC vs league average ~4.2
      if (adjustedYPC >= 8.0) score += 1.5;       // Pancaking defenders
      else if (adjustedYPC >= 6.5) score += 1.1;
      else if (adjustedYPC >= 5.5) score += 0.8;
      else if (adjustedYPC >= 4.5) score += 0.4;
      else if (adjustedYPC >= 4.0) score += 0.1;
      else if (adjustedYPC >= 3.0) score -= 0.2;
      else if (adjustedYPC < 2.0) score -= 0.7;   // Losing the battle at the LOS
      else score -= 0.4;

      // Volume signal: OC trusts this gap (kept running it)
      if (gapCarries >= 8) score += 0.35;
      else if (gapCarries >= 5) score += 0.15;

      // Push into the end zone
      if (gapTDs) score += 0.5;
    } else if (gapCarries > 0) {
      if (adjustedYPC >= 5.5) score += 0.3;
      else if (adjustedYPC < 2.0) score -= 0.3;
    }
    // 0 gap carries = pass-heavy game; neutral on run blocking
  } else {
    // Fallback: team rushing signal
    const carries = ctx.rushCarries;
    const ypc = carries > 0 ? ctx.rushYards / carries : 0;
    if (carries >= 8) {
      if (ypc >= 5.5) score += 0.5;
      else if (ypc >= 4.5) score += 0.2;
      else if (ypc < 3.0) score -= 0.3;
    }
  }

  // ── 4. SNAP COUNT WEIGHT ─────────────────────────────────────────
  // Very low snap counts = small sample; pull toward league average 6.5
  if (olPlayerData?.offenseSnaps > 0) {
    const snaps = olPlayerData.offenseSnaps;
    if (snaps < 10) {
      // Heavily regress to mean for very limited snap players
      score = score * (snaps / 30) + 6.5 * (1 - snaps / 30);
    }
  }

  return score;
}

function gradeDL(stats: any, dlPlayerData?: any): number {
  // DL grading: pass rush is the primary grade (sacks + QB hits + pressures).
  // Run stop (TFLs, tackles) is secondary but important for NT/IDL.
  // nflverse qb_hit_1/2 gives us pressures even when no sack occurs.

  const defensive = stats.defensive || {};
  const totalTackles = parseNum(defensive["TOT"]);
  const sacks = parseNum(defensive["SACKS"]);
  const tfl = parseNum(defensive["TFL"]);
  const pd = parseNum(defensive["PD"]);
  const qbHitsEspn = parseNum(defensive["QB HTS"]);
  const defTd = parseNum(defensive["TD"]);

  // nflverse enrichment
  const nflQbHits = dlPlayerData?.qbHits ?? 0;
  const nflTfl = dlPlayerData?.tfl ?? 0;
  const nflSacks = dlPlayerData?.sacksContributed ?? 0;
  const nflForcedFumbles = dlPlayerData?.forcedFumbles ?? 0;
  const hasRichData = dlPlayerData !== undefined;

  const effectiveSacks = hasRichData ? Math.max(sacks, nflSacks) : sacks;
  const effectiveQbHits = hasRichData ? Math.max(qbHitsEspn, nflQbHits) : qbHitsEspn;
  const effectiveTfl = hasRichData ? Math.max(tfl, nflTfl) : tfl;

  let score = 5.5;

  // ── 1. PASS RUSH (primary for edge rushers) ───────────────────────────────
  score += effectiveSacks * 1.8;     // Sacks are the gold standard
  score += effectiveQbHits * 0.55;   // Hit without sack = still disrupted QB
  score += nflForcedFumbles * 0.9;   // Forced fumble = huge play

  // ── 2. RUN STOP (TFLs + tackles) ────────────────────────────────────────
  score += effectiveTfl * 0.7;
  if (totalTackles >= 10) score += 0.9;
  else if (totalTackles >= 7) score += 0.6;
  else if (totalTackles >= 5) score += 0.3;
  else if (totalTackles >= 3) score += 0.1;

  // ── 3. COVERAGE / MISC ───────────────────────────────────────────────────
  score += pd * 0.4;
  score += defTd * 2.0;

  return Math.max(1.0, Math.min(10.0, score));
}

function gradeLB(stats: any, lbPlayerData?: any): number {
  // LB grading uses both ESPN box score AND nflverse pass rusher stats:
  //   - qb_hit_1/2_player_name: pressures/hits without a full sack (LB pass rush)
  //   - tackle_for_loss_1/2: key run stop metric
  //   - forced_fumble: disruption
  // LBs are evaluated across 3 dimensions: tackle, pass rush, coverage

  const defensive = stats.defensive || {};
  const interceptions = stats.interceptions || {};

  const totalTackles = parseNum(defensive["TOT"]);
  const sacks = parseNum(defensive["SACKS"]);
  const tfl = parseNum(defensive["TFL"]);
  const pd = parseNum(defensive["PD"]);
  const qbHitsEspn = parseNum(defensive["QB HTS"]);
  const defTd = parseNum(defensive["TD"]);
  const ints = parseNum(interceptions["INT"]);

  // nflverse enrichment: more granular pass rush / disruption stats
  const nflQbHits = lbPlayerData?.qbHits ?? 0;
  const nflTfl = lbPlayerData?.tfl ?? 0;
  const nflSacks = lbPlayerData?.sacksContributed ?? 0;
  const nflForcedFumbles = lbPlayerData?.forcedFumbles ?? 0;
  const hasRichData = lbPlayerData !== undefined;

  // Use best available (nflverse > ESPN, take max to avoid double-penalty)
  const effectiveQbHits = hasRichData ? Math.max(qbHitsEspn, nflQbHits) : qbHitsEspn;
  const effectiveTfl = hasRichData ? Math.max(tfl, nflTfl) : tfl;
  const effectiveSacks = hasRichData ? Math.max(sacks, nflSacks) : sacks;

  let score = 5.5;

  // ── 1. TACKLES (run stopping, pursuit) ──────────────────────────────────────
  // LBs are expected to make many tackles — this is table stakes
  if (totalTackles >= 15) score += 1.8;
  else if (totalTackles >= 12) score += 1.4;
  else if (totalTackles >= 10) score += 1.0;
  else if (totalTackles >= 8)  score += 0.6;
  else if (totalTackles >= 6)  score += 0.3;
  else if (totalTackles >= 4)  score += 0.1;
  else if (totalTackles < 3)   score -= 0.4;

  // ── 2. PASS RUSH (sacks, QB hits, TFLs) ───────────────────────────────────
  score += effectiveSacks * 1.6;
  score += effectiveQbHits * 0.45;    // QB hits without sack still disrupt offense
  score += effectiveTfl * 0.8;        // TFLs = elite run defense
  score += nflForcedFumbles * 0.8;    // Disruption bonus

  // ── 3. COVERAGE (PDs, INTs) ────────────────────────────────────────────────
  score += pd * 0.5;
  score += ints * 1.8;
  score += defTd * 2.0;

  return Math.max(1.0, Math.min(10.0, score));
}

function gradeCB(stats: any, cbPlayerData?: any): number {
  // Individual CB grading: the key insight is that LOW targets + LOW completions =
  // EXCELLENT coverage. A CB with 0 catches allowed and 0 targets had a dominant game.
  // A CB with 8 tackles but gave up 7 catches had a bad one.
  //
  // Data sources:
  // - ESPN box score: PD, INT, tackles, sacks
  // - play-by-play parsed: times targeted, yards allowed, receptions allowed, TDs allowed
  // - FTN: contested balls won, drops forced
  // - nflverse snap_counts: defense snaps (denominates target rate)

  const defensive = stats.defensive || {};
  const interceptions = stats.interceptions || {};

  const pd = parseNum(defensive["PD"]);
  const ints = parseNum(interceptions["INT"]);
  const totalTackles = parseNum(defensive["TOT"]);
  const tfl = parseNum(defensive["TFL"]);
  const sacks = parseNum(defensive["SACKS"]);
  const defTd = parseNum(defensive["TD"]);

  // ── Baseline: start neutral ────────────────────────────────────────────────
  // CBs start at 6.0 (slightly below average) and earn their way up/down
  let score = 6.0;

  // Use the most precise snap count available:
  // individualPassSnaps (from participation GSIS tracking per-player) > defenseSnaps (from snap_counts)
  const passSnapsForRate = cbPlayerData?.individualPassSnaps ?? cbPlayerData?.defenseSnaps ?? 0;
  const defSnaps = cbPlayerData?.defenseSnaps || cbPlayerData?.individualTotalSnaps || 0;
  const hasRichData = cbPlayerData && cbPlayerData.timesTargeted !== undefined;

  if (hasRichData) {
    const { timesTargeted, receptionsAllowed, yardsAllowed, tdsAllowed,
            passesDefended: pbpPD, contestedCatches, dropsForced } = cbPlayerData;

    // ── 1. TARGET RATE (most important signal) ────────────────────────────────────────
    // Offenses avoid elite CBs. Low targets per pass snap = locked down.
    // Use individualPassSnaps for precision: it counts exactly when THAT player was on field
    const targetRate = passSnapsForRate > 0 ? timesTargeted / passSnapsForRate : 0;

    if (passSnapsForRate >= 15) {
      // Only apply target-rate bonus if they played meaningful coverage snaps
      if (targetRate <= 0.08) score += 1.8;      // Near-untouchable (Mitchell vs KC: ~8.6%)
      else if (targetRate <= 0.15) score += 1.2; // QB avoiding them = elite
      else if (targetRate <= 0.22) score += 0.6; // Rarely targeted = very good
      else if (targetRate <= 0.32) score += 0.2; // Below-average target rate = solid
      else if (targetRate <= 0.42) score -= 0.2; // Average target rate
      else if (targetRate <= 0.55) score -= 0.6; // QB targets them often
      else score -= 1.0;                          // Being attacked all game
    } else if (timesTargeted === 0 && passSnapsForRate > 5) {
      score += 0.6; // Played meaningful snaps, not targeted once
    } else if (timesTargeted === 0) {
      score += 0.3; // Low snaps, not targeted
    }

    // ── 2. COVERAGE QUALITY (yards allowed per target) ───────────────
    // League avg: ~6-8 yards per target for CBs
    if (timesTargeted >= 3) {
      const yardsPerTarget = yardsAllowed / timesTargeted;
      const catchRate = receptionsAllowed / timesTargeted;

      // Catch rate allowed (lower = better)
      if (catchRate <= 0.35) score += 1.0;        // Lockdown
      else if (catchRate <= 0.50) score += 0.5;
      else if (catchRate <= 0.65) score += 0.0;   // Average
      else if (catchRate <= 0.75) score -= 0.4;
      else score -= 0.8;                          // Getting beaten constantly

      // Yards per target (lower = better)
      if (yardsPerTarget <= 4.0) score += 0.6;
      else if (yardsPerTarget <= 6.0) score += 0.3;
      else if (yardsPerTarget <= 9.0) score += 0.0;
      else if (yardsPerTarget <= 13.0) score -= 0.4;
      else score -= 0.8;                          // Giving up huge plays
    } else if (timesTargeted > 0) {
      // Small sample: gentle signal
      if (receptionsAllowed === 0) score += 0.5;  // Didn’t allow a catch
    }

    // ── 3. TDS ALLOWED ───────────────────────────────────────────────
    score -= tdsAllowed * 1.5;  // TD allowed is the worst outcome

    // ── 4. CONTESTED CATCH BATTLES ───────────────────────────────
    // Contested balls = physical 50/50 battles. Winning them = big bonus.
    if (contestedCatches > 0) score -= contestedCatches * 0.5; // lost 50/50
    if (dropsForced > 0) score += dropsForced * 0.4;            // forced the drop
  }

  // ── 5. BOX SCORE PASS DEFENSED + INTERCEPTIONS (always available) ────
  score += pd * 0.7;      // Pass defensed = active, physical coverage
  score += ints * 2.0;    // Interceptions = best possible coverage outcome
  score += defTd * 2.0;   // Pick-6 / defensive TD

  // ── 6. TACKLE QUALITY (CBs making tackles = they allowed catches first) ──
  // Unlike LBs, lots of CB tackles often means they gave up lots of catches.
  // BUT we still credit tackles for stopping the gain (prevent YAC)
  if (totalTackles >= 8) {
    // Many tackles = probably targeted a lot; only give partial credit
    score += hasRichData ? 0.1 : 0.4;
  } else if (totalTackles >= 5) {
    score += hasRichData ? 0.1 : 0.3;
  } else if (totalTackles >= 3) {
    score += 0.1;
  }

  score += tfl * 0.5;   // TFL in run support = bonus
  score += sacks * 1.0; // CB blitz sack = bonus

  // ── 7. QUIET GAME BONUS (not targeted, no tackles = CB lockdown) ──────
  if (!hasRichData) {
    // Fallback when no PBP data: 0 stats could mean not targeted
    if (pd === 0 && ints === 0 && totalTackles === 0) {
      score = 6.5; // elevated: likely not targeted = good coverage
    }
  } else {
    // With rich data: if they had meaningful pass coverage snaps but 0 targets = lockdown
    const wasUntargeted = cbPlayerData.timesTargeted === 0 && passSnapsForRate >= 15;
    if (wasUntargeted) {
      score = Math.max(score, 7.5); // Floor at 7.5 for an elite lockdown performance
    }
  }

  return score;
}

function gradeSafety(stats: any, safetyPlayerData?: any): number {
  // Safeties are evaluated across coverage + tackling + occasional pass rush.
  // nflverse qb_hit data helps capture blitz/safety blitz contributions.

  const defensive = stats.defensive || {};
  const interceptions = stats.interceptions || {};

  const totalTackles = parseNum(defensive["TOT"]);
  const sacks = parseNum(defensive["SACKS"]);
  const tfl = parseNum(defensive["TFL"]);
  const pd = parseNum(defensive["PD"]);
  const qbHitsEspn = parseNum(defensive["QB HTS"]);
  const defTd = parseNum(defensive["TD"]);
  const ints = parseNum(interceptions["INT"]);

  // nflverse enrichment (blitzes, forced fumbles)
  const nflQbHits = safetyPlayerData?.qbHits ?? 0;
  const nflForcedFumbles = safetyPlayerData?.forcedFumbles ?? 0;
  const nflSacks = safetyPlayerData?.sacksContributed ?? 0;
  const nflTfl = safetyPlayerData?.tfl ?? 0;
  const effectiveQbHits = Math.max(qbHitsEspn, nflQbHits);
  const effectiveSacks = Math.max(sacks, nflSacks);
  const effectiveTfl = Math.max(tfl, nflTfl);

  let score = 5.5;

  // ── 1. TACKLES (safeties are key tacklers in the middle) ─────────────────────
  if (totalTackles >= 12) score += 1.3;
  else if (totalTackles >= 9)  score += 0.9;
  else if (totalTackles >= 7)  score += 0.6;
  else if (totalTackles >= 5)  score += 0.3;
  else if (totalTackles >= 3)  score += 0.1;

  // ── 2. COVERAGE (PDs, INTs — the premium output) ─────────────────────────
  score += pd * 0.7;
  score += ints * 2.0;
  score += defTd * 2.0;

  // ── 3. PASS RUSH / DISRUPTION (blitzing safety) ─────────────────────────
  score += effectiveSacks * 1.3;
  score += effectiveQbHits * 0.4;
  score += nflForcedFumbles * 0.8;
  score += effectiveTfl * 0.5;

  return Math.max(1.0, Math.min(10.0, score));
}

function gradeKicker(stats: any): number {
  const kicking = stats.kicking || {};

  const fgStr = kicking["FG"] || "0/0";
  const [fgMade, fgAtt] = fgStr.split("/").map((v: string) => parseFloat(v) || 0);
  const fgPct = fgAtt > 0 ? fgMade / fgAtt : 1;
  const longFG = parseNum(kicking["LONG"]);
  const pts = parseNum(kicking["PTS"]);

  const xpStr = kicking["XP"] || "0/0";
  const [xpMade, xpAtt] = xpStr.split("/").map((v: string) => parseFloat(v) || 0);
  const xpPct = xpAtt > 0 ? xpMade / xpAtt : 1;

  let score = 6.0;

  if (fgAtt > 0) {
    if (fgPct === 1.0) score += 1.0;
    else if (fgPct >= 0.75) score += 0.3;
    else if (fgPct < 0.5) score -= 0.8;
  }

  if (longFG >= 55) score += 0.8;
  else if (longFG >= 50) score += 0.5;
  else if (longFG >= 45) score += 0.2;

  if (xpPct < 1.0 && xpAtt > 0) score -= 0.5;

  if (pts >= 15) score += 0.5;

  return score;
}

function gradePunter(stats: any): number {
  const punting = stats.punting || {};

  const avgStr = punting["AVG"] || "0";
  const avg = parseNum(avgStr);
  const in20 = parseNum(punting["In 20"]);
  const tbs = parseNum(punting["TB"]);
  const no = parseNum(punting["NO"]);

  let score = 6.0;

  if (avg >= 50) score += 1.0;
  else if (avg >= 46) score += 0.5;
  else if (avg >= 42) score += 0.1;
  else if (avg < 38) score -= 0.5;

  // Inside 20s are valuable
  if (no > 0) score += (in20 / no) * 1.0;

  // Touchbacks penalize
  if (no > 0) score -= (tbs / no) * 0.5;

  return score;
}

function gradeGenericDef(stats: any): number {
  const defensive = stats.defensive || {};
  const interceptions = stats.interceptions || {};

  const totalTackles = parseNum(defensive["TOT"]);
  const sacks = parseNum(defensive["SACKS"]);
  const tfl = parseNum(defensive["TFL"]);
  const pd = parseNum(defensive["PD"]);
  const ints = parseNum(interceptions["INT"]);

  let score = 5.5;
  score += totalTackles * 0.1;
  score += sacks * 1.2;
  score += tfl * 0.5;
  score += pd * 0.6;
  score += ints * 1.8;

  return score;
}

export async function fetchGameData(gameId: string, forceRefresh = false): Promise<any> {
  if (!forceRefresh) {
    const cached = getCached(gameId);
    if (cached) return JSON.parse(cached.data);
  }
      // Fetch from ESPN
      const summaryUrl = `${ESPN_BASE}/summary?event=${gameId}`;
      const summaryData = await fetchJson(summaryUrl);

      const header = summaryData.header;
      const competition = header?.competitions?.[0];
      const homeComp = competition?.competitors?.find((c: any) => c.homeAway === "home");
      const awayComp = competition?.competitors?.find((c: any) => c.homeAway === "away");

      // Get box score players
      const boxscorePlayers = summaryData.boxscore?.players || [];

      // Build athlete -> position map from roster (no extra HTTP calls — extract pos ID from $ref)
      const athletePositionMap: Record<string, string> = {};

      await Promise.all(
        (competition?.competitors || []).map(async (comp: any) => {
          const teamId = comp.team?.id;
          if (!teamId) return;
          try {
            const rosterUrl = `${ESPN_CORE}/events/${gameId}/competitions/${gameId}/competitors/${teamId}/roster`;
            const rosterData = await fetchJson(rosterUrl);
            for (const entry of rosterData.entries || []) {
              const playerId = String(entry.playerId);
              const posRef = entry.position?.["$ref"] || "";
              const pos = extractPositionFromRef(posRef);
              if (pos) athletePositionMap[playerId] = pos;
            }
          } catch {}
        })
      );

      // Process player stats
      const teamPlayers: Record<string, any[]> = {};

      for (const teamData of boxscorePlayers) {
        const teamId = teamData.team?.id;
        if (!teamId) continue;
        if (!teamPlayers[teamId]) teamPlayers[teamId] = [];

        const playerStats: Record<string, any> = {};

        for (const cat of teamData.statistics || []) {
          const labels: string[] = cat.labels || [];
          for (const athData of cat.athletes || []) {
            const ath = athData.athlete || {};
            const pid = ath.id;
            if (!pid) continue;
            if (!playerStats[pid]) {
              playerStats[pid] = {
                id: pid,
                name: ath.displayName || `${ath.firstName} ${ath.lastName}`,
                headshot: ath.headshot?.href || null,
                jersey: ath.jersey || "",
                stats: {},
              };
            }
            const stats: Record<string, string> = {};
            labels.forEach((lbl: string, i: number) => {
              stats[lbl] = athData.stats?.[i] || "0";
            });
            playerStats[pid].stats[cat.name] = stats;
          }
        }

        teamPlayers[teamId] = Object.values(playerStats);
      }

      // Pull ALL players from the roster who never appear in box score stats
      // (OL, backups, special teamers — anyone with valid:true who played a snap)
      // We also capture the rosterData for OL context grading below
      const olPositions = new Set(["OT", "G", "C", "OG", "OL", "FB"]);
      // LS is a special teamer, keep it but don't count as OL for team context
      const noBoxScorePositions = new Set(["OT", "G", "C", "OG", "OL", "FB", "LS"]);

      // We'll also accumulate team-level context for OL grading
      // Key: teamId -> { sacks, rushYards, rushTDs, rushCarries }
      const teamRosterData: Record<string, any[]> = {};

      for (const comp of (competition?.competitors || [])) {
        const teamId = String(comp.team?.id);
        if (!teamId) continue;
        if (!teamPlayers[teamId]) teamPlayers[teamId] = [];

        try {
          const rosterUrl = `${ESPN_CORE}/events/${gameId}/competitions/${gameId}/competitors/${teamId}/roster`;
          const rosterData = await fetchJson(rosterUrl);
          teamRosterData[teamId] = rosterData.entries || [];
          const existingIds = new Set(teamPlayers[teamId].map((p: any) => String(p.id)));

          for (const entry of rosterData.entries || []) {
            // valid:false = did not play. didNotPlay:true is explicit DNP.
            // We include everyone who is valid AND not explicitly DNP
            if (entry.didNotPlay === true) continue;
            if (entry.valid === false) continue;

            const playerId = String(entry.playerId);
            if (existingIds.has(playerId)) continue; // already in boxscore

            const posRef = entry.position?.["$ref"] || "";
            const pos = extractPositionFromRef(posRef);
            if (!pos || !noBoxScorePositions.has(pos)) continue;

            // Build a minimal player record
            teamPlayers[teamId].push({
              id: playerId,
              name: entry.displayName || `Player ${playerId}`,
              headshot: null,
              jersey: entry.jersey || "",
              position: pos,
              stats: {},
            });
            existingIds.add(playerId);
          }
        } catch {}
      }

      // Fetch headshots for OL players who got added without them
      // We batch-fetch athlete records for any player missing a headshot
      const playersNeedingHeadshot: { teamId: string; player: any }[] = [];
      for (const [teamId, players] of Object.entries(teamPlayers)) {
        for (const player of players as any[]) {
          if (!player.headshot && player.id) {
            playersNeedingHeadshot.push({ teamId, player });
          }
        }
      }
      // Batch fetch athlete info (limit concurrency to avoid rate limits)
      const BATCH = 10;
      for (let i = 0; i < playersNeedingHeadshot.length; i += BATCH) {
        const batch = playersNeedingHeadshot.slice(i, i + BATCH);
        await Promise.all(batch.map(async ({ player }) => {
          try {
            const athUrl = `${ESPN_CORE}/athletes/${player.id}?lang=en&region=us`;
            const ath = await fetchJson(athUrl);
            if (ath.headshot?.href) player.headshot = ath.headshot.href;
            if (!player.position || player.position === "?") {
              player.position = ath.position?.abbreviation || player.position;
            }
            if (!player.name || player.name.startsWith("Player ")) {
              player.name = ath.displayName || player.name;
            }
          } catch {}
        }));
      }

      // ── Resolve nflverse game_id and fetch supplemental data ──────────────
      // Extract season year from competition date
      const gameYear = new Date(competition?.date || Date.now()).getFullYear();
      // Season year: if game is before April, it's the prior season
      const seasonYear = new Date(competition?.date || Date.now()).getMonth() < 4 ? gameYear - 1 : gameYear;

      let nflverseId: string | null = null;
      let pbpRows: Record<string, string>[] = [];
      let participationRows: Record<string, string>[] = [];
      let ftnRows: Record<string, string>[] = [];
      let snapCountRows: Record<string, string>[] = [];
      let playerStatsRows: Record<string, string>[] = [];
      let depthChartRows: Record<string, string>[] = [];

      try {
        nflverseId = await resolveNflverseGameId(gameId, seasonYear);
      } catch {}

      if (nflverseId) {
        const _nflId = nflverseId; // capture for lambda
        const weekNum = _nflId.split("_")[1];
        // Use fetchCSVFiltered for large files (PBP ~99MB, participation ~48MB) to avoid OOM.
        // Smaller files (ftn, snap_counts, stats_player) are <10MB — fetchCSV is fine.
        const [pbpResult, partResult, ftnResult, snapResult, statsResult] = await Promise.allSettled([
          fetchCSVFiltered(
            `${NFLVERSE_BASE}/pbp/play_by_play_${seasonYear}.csv`,
            r => r["game_id"] === _nflId
          ),
          fetchCSVFiltered(
            `${NFLVERSE_BASE}/pbp_participation/pbp_participation_${seasonYear}.csv`,
            r => r["nflverse_game_id"] === _nflId
          ),
          fetchCSV(`${NFLVERSE_BASE}/ftn_charting/ftn_charting_${seasonYear}.csv`),
          fetchCSV(`${NFLVERSE_BASE}/snap_counts/snap_counts_${seasonYear}.csv`),
          fetchCSV(`${NFLVERSE_BASE}/stats_player/stats_player_week_${seasonYear}.csv`),
        ]);

        if (pbpResult.status === "fulfilled") {
          pbpRows = pbpResult.value; // already filtered
        }
        if (partResult.status === "fulfilled") {
          participationRows = partResult.value; // already filtered
        }
        if (ftnResult.status === "fulfilled") {
          ftnRows = ftnResult.value.filter(r => r["nflverse_game_id"] === _nflId);
        }
        if (snapResult.status === "fulfilled") {
          snapCountRows = snapResult.value.filter(r => r["game_id"] === _nflId);
        }
        if (statsResult.status === "fulfilled") {
          playerStatsRows = statsResult.value.filter(r =>
            r["game_id"] === _nflId ||
            (r["season"] === String(seasonYear) && r["week"] === String(parseInt(weekNum)))
          ).filter(r => r["penalties"] || r["penalty_yards"]);
        }

        // Fetch depth charts — handle TWO different nflverse schemas:
        // 2025+ (daily snapshots): columns = team, dt, pos_abb, pos_rank, espn_id
        // Pre-2025 (weekly):       columns = club_code, week, game_type, depth_position, depth_team, gsis_id
        const teamAbbrs = new Set(
          (competition?.competitors || []).map((c: any) => c.team?.abbreviation as string).filter(Boolean)
        );
        const gameIsoDate = (competition?.date || "").slice(0, 10);
        // Game week from nflverseId format "2024_22_KC_PHI" -> "22"
        const gameWeek = nflverseId?.split("_")[1] || "";
        const gameType = nflverseId?.split("_")[2] ? // reg=numeric, post=non-numeric
          (isNaN(parseInt(nflverseId.split("_")[2])) ? "POST" : "REG") : "REG";

        try {
          const allDepthRows = await fetchCSVFiltered(
            `${NFLVERSE_BASE}/depth_charts/depth_charts_${seasonYear}.csv`,
            (row) => {
              // Detect schema by presence of key columns
              if (row["pos_abb"] !== undefined) {
                // 2025+ schema: team, dt, pos_abb, pos_rank
                return teamAbbrs.has(row["team"]) &&
                  ["LT","RT","LG","RG","C"].includes(row["pos_abb"]) &&
                  row["pos_rank"] === "1";
              } else {
                // Pre-2025 schema: club_code, week, depth_position, depth_team
                return teamAbbrs.has(row["club_code"]) &&
                  ["LT","RT","LG","RG","C"].includes(row["depth_position"]) &&
                  row["depth_team"] === "1";
              }
            }
          );

          if (allDepthRows.length === 0) {
            // No rows found
          } else if (allDepthRows[0]["pos_abb"] !== undefined) {
            // 2025+ schema: pick snapshot by date
            const allDates = [...new Set(allDepthRows.map(r => (r["dt"] || "").slice(0, 10)))].sort();
            const preDates = allDates.filter(d => d <= gameIsoDate);
            const postDates = allDates.filter(d => d > gameIsoDate);
            const bestDate = preDates.length > 0 ? preDates[preDates.length - 1]
              : (postDates.length > 0 ? postDates[0] : "");
            depthChartRows = bestDate
              ? allDepthRows.filter(r => (r["dt"] || "").startsWith(bestDate))
              : allDepthRows;
          } else {
            // Pre-2025 schema: pick by week number closest to game week
            const gameWeekNum = parseInt(gameWeek) || 99;
            // Use the exact game week if available, else latest week before it
            const allWeeks = [...new Set(allDepthRows.map(r => parseInt(r["week"] || "0")))].sort((a,b)=>a-b);
            const preWeeks = allWeeks.filter(w => w <= gameWeekNum);
            const bestWeek = preWeeks.length > 0 ? preWeeks[preWeeks.length - 1]
              : (allWeeks.length > 0 ? allWeeks[0] : null);
            depthChartRows = bestWeek !== null
              ? allDepthRows.filter(r => parseInt(r["week"] || "0") === bestWeek)
              : allDepthRows;
            // Normalize pre-2025 columns to match 2025+ schema expected by the rest of the code
            depthChartRows = depthChartRows.map(r => ({
              team: r["club_code"],
              pos_abb: r["depth_position"],
              pos_rank: r["depth_team"] === "1" ? "1" : r["depth_team"],
              espn_id: "",           // not available in pre-2025; will use gsis_id bridge
              gsis_id: r["gsis_id"],
              dt: "",
              player_name: r["full_name"] || (r["first_name"] + " " + r["last_name"]),
              jersey_number: r["jersey_number"] || "",
            }));
          }
        } catch { /* depth chart is supplemental — degrade gracefully */ }
      }

      // Build nflverse play_id -> FTN charting lookup
      const ftnByPlayId: Record<string, Record<string, string>> = {};
      for (const row of ftnRows) {
        ftnByPlayId[row["nflverse_play_id"]] = row;
      }

      // Build nflverse play_id -> participation row lookup
      const partByPlayId: Record<string, Record<string, string>> = {};
      for (const row of participationRows) {
        partByPlayId[row["play_id"]] = row;
      }

      // Build nflverse play_id -> PBP row lookup (O(1) instead of .find())
      const pbpByNflversePlayId: Record<string, Record<string, string>> = {};
      for (const row of pbpRows) {
        pbpByNflversePlayId[row["play_id"]] = row;
      }

      // ── Pre-build WR/TE route stats from participation ──────────────────────
      // participation.route = route run per play (GO, SLANT, CROSS, OUT, etc.)
      // is_created_reception = WR created the catch through separation (FTN)
      // yards_after_catch = YAC from PBP (YAC = YAC skill, not just catching)
      const wrStatsFromNflverse: Record<string, {
        routes: number;          // total routes run
        targets: number;         // times targeted
        receptions: number;      // catches
        yards: number;           // receiving yards
        yac: number;             // yards after catch
        airYards: number;        // intended air yards on targets
        tds: number;
        drops: number;           // FTN is_drop
        createdReceptions: number; // FTN is_created_reception — separated to create catch
        firstDowns: number;
        routeTypes: Record<string, number>; // route type counts
      }> = {};

      function getWR(name: string) {
        if (!wrStatsFromNflverse[name]) wrStatsFromNflverse[name] = {
          routes: 0, targets: 0, receptions: 0, yards: 0, yac: 0, airYards: 0,
          tds: 0, drops: 0, createdReceptions: 0, firstDowns: 0, routeTypes: {},
        };
        return wrStatsFromNflverse[name];
      }

      // ── Pre-build defender coverage stats from participation ─────────────────
      // defense_names: semicolon-separated list of GSIS IDs for defenders on play
      // defense_positions: semicolon-separated positions matching defense_names order
      // This lets us find the CB/S covering the targeted receiver precisely.
      // Strategy: on a pass play, the defender closest to the receiver = coverage defender.
      // We use defense_names + defense_positions to build a gsis_id → position map per play,
      // then match targeted receiver's coverage defender via pass_defense_1_player_name on PDs,
      // or solo_tackle on completions (same as before, but now we ALSO track coverage snaps by
      // defender type — man coverage snaps, zone coverage snaps, blitz snaps).
      const defenderCoverageSnaps: Record<string, {
        manCoverageSnaps: number;
        zoneCoverageSnaps: number;
        blitzSnaps: number;
        passRushSnaps: number;   // in box/rushing
        coverageTackles: number; // tackles in coverage (vs box tackles)
      }> = {};

      function getDefCov(gsisId: string) {
        if (!defenderCoverageSnaps[gsisId]) defenderCoverageSnaps[gsisId] = {
          manCoverageSnaps: 0, zoneCoverageSnaps: 0, blitzSnaps: 0,
          passRushSnaps: 0, coverageTackles: 0,
        };
        return defenderCoverageSnaps[gsisId];
      }

      // ── Pre-build LB / pass rusher stats from PBP ───────────────────────────
      // qb_hit_1/2_player_name = pressures/hits even without a sack
      // tackle_for_loss_1/2_player_name = TFLs (key for LB run stop grade)
      const passRusherStats: Record<string, {
        qbHits: number;
        sacksContributed: number;
        tfl: number;
        forcedFumbles: number;
      }> = {};

      function getPR(name: string) {
        if (!passRusherStats[name]) passRusherStats[name] = {
          qbHits: 0, sacksContributed: 0, tfl: 0, forcedFumbles: 0,
        };
        return passRusherStats[name];
      }

      if (pbpRows.length > 0) {
        for (const row of pbpRows) {
          const playType = row["play_type"];
          const isPass = row["pass_attempt"] === "1";
          const isRush = row["rush_attempt"] === "1";
          const isSack = row["sack"] === "1";
          const partRow = partByPlayId[row["play_id"]];

          // ── WR/TE route stats ──────────────────────────────────────────────
          if (isPass && !isSack && row["qb_kneel"] !== "1") {
            const receiverName = row["receiver_player_name"] || "";
            const isComplete = row["complete_pass"] === "1";
            const isInterception = row["interception"] === "1";
            const yds = parseFloat(row["yards_gained"] || "0");
            const yac = parseFloat(row["yards_after_catch"] || "0");
            const airYds = parseFloat(row["air_yards"] || "0");
            const isTD = row["pass_touchdown"] === "1";
            const ftnR = ftnByPlayId[row["play_id"]];
            const isDrop = ftnR?.is_drop === "TRUE";
            const isCreated = ftnR?.is_created_reception === "TRUE";
            const isFirstDown = row["first_down_pass"] === "1";

            if (receiverName) {
              const w = getWR(receiverName);
              w.targets += 1;
              w.airYards += airYds;
              if (isComplete) {
                w.receptions += 1;
                w.yards += Math.max(0, yds);
                w.yac += Math.max(0, yac);
                if (isTD) w.tds += 1;
                if (isCreated) w.createdReceptions += 1;
                if (isFirstDown) w.firstDowns += 1;
              }
              if (isDrop) w.drops += 1;
            }

            // Route type from participation (per receiver's route on this play)
            if (partRow && receiverName) {
              const route = partRow["route"] || "";
              if (route && receiverName) {
                const w = getWR(receiverName);
                w.routes += 1;
                w.routeTypes[route] = (w.routeTypes[route] || 0) + 1;
              }
            }
          }

          // ── LB / DL pass rush stats ─────────────────────────────────────────
          if (row["qb_hit"] === "1") {
            const h1 = row["qb_hit_1_player_name"];
            const h2 = row["qb_hit_2_player_name"];
            if (h1) getPR(h1).qbHits += 1;
            if (h2) getPR(h2).qbHits += 1;
          }
          if (isSack) {
            // Credit all sack contributors
            const s1 = row["solo_tackle_1_player_name"] || row["sack_player_name"];
            const s2 = row["solo_tackle_2_player_name"] || row["half_sack_1_player_name"];
            if (s1) getPR(s1).sacksContributed += 1;
            if (s2) getPR(s2).sacksContributed += 0.5;
          }
          if (row["tackled_for_loss"] === "1" || row["tackle_for_loss"] === "1") {
            const t1 = row["tackle_for_loss_1_player_name"];
            const t2 = row["tackle_for_loss_2_player_name"];
            if (t1) getPR(t1).tfl += 1;
            if (t2) getPR(t2).tfl += 0.5;
          }
          const ff1 = row["forced_fumble_player_1_player_name"];
          const ff2 = row["forced_fumble_player_2_player_name"];
          if (ff1) getPR(ff1).forcedFumbles += 1;
          if (ff2) getPR(ff2).forcedFumbles += 1;

          // ── Defender coverage snap type (man/zone/blitz) ───────────────────
          if (partRow && (isPass || isRush)) {
            const defGsisIds = (partRow["defense_players"] || "").split(";").filter(Boolean);
            const defPositions = (partRow["defense_positions"] || "").split(";").filter(Boolean);
            const covType = partRow["defense_man_zone_type"] || "";
            const nBlitz = parseInt(partRow["number_of_pass_rushers"] || ftnByPlayId[row["play_id"]]?.n_pass_rushers || "4");
            const isBlitzPlay = nBlitz > 4;

            defGsisIds.forEach((gsisId, i) => {
              const pos = (defPositions[i] || "").toUpperCase();
              if (!gsisId) return;
              const d = getDefCov(gsisId);
              // Determine if this defender is in coverage or pass rush
              const isCoveragePos = ["CB","FS","SS","S","DB","LCB","RCB","NCB"].includes(pos);
              const isPassRushPos = ["DE","DT","NT","OLB","ILB","MLB"].includes(pos);
              if (isPass) {
                if (isCoveragePos) {
                  if (covType.includes("MAN")) d.manCoverageSnaps += 1;
                  else d.zoneCoverageSnaps += 1;
                  if (isBlitzPlay) d.blitzSnaps += 1;
                } else if (isPassRushPos) {
                  d.passRushSnaps += 1;
                }
              }
            });
          }
        }

        // ── Count routes run from participation (for WRs not targeted) ────────
        // participation.route is the route this specific player ran on this play
        // We need to match offense_names/offense_positions to track per receiver
        for (const partRow of participationRows) {
          const pbpRow = pbpByNflversePlayId[partRow["play_id"]];
          if (!pbpRow || pbpRow["pass_attempt"] !== "1") continue;
          const offNames = (partRow["offense_names"] || "").split(";").filter(Boolean);
          const offPositions = (partRow["offense_positions"] || "").split(";").filter(Boolean);
          const route = partRow["route"] || "";
          // route in participation is ONLY for the targeted/charted receiver, not all
          // So routes per player come from counting their participation snaps on pass plays
          // Use offense_players (GSIS IDs) with offense_positions to count WR pass snap routes
          const offGsisIds = (partRow["offense_players"] || "").split(";").filter(Boolean);
          offGsisIds.forEach((gsisId, i) => {
            const pos = (offPositions[i] || "").toUpperCase();
            if (["WR","TE","FB","RB"].includes(pos)) {
              // This player was on the field for a pass play = ran a route
              // We'll count this as a route run for the player (by GSIS, merged later)
              // Store by GSIS ID for now — will merge to ESPN player in scoring loop
              const name = offNames[i] || "";
              if (name) {
                const w = getWR(name);
                // Only count if not already counted as a target on this play
                // routes field will be set from target count + non-target snaps
                w.routes += 0; // will reconcile below
              }
            }
          });
        }
      }

      // ── Pre-build per-player pass snap count from participation ──────────────
      // OVERWRITE the simpler tracking done previously with the full participation loop
      // (this block was previously only counting GSIS IDs from defense_players)
      const playerOffPassSnaps: Record<string, number> = {}; // espnId -> pass snaps (offense)
      const playerDefPassSnaps: Record<string, number> = {}; // espnId -> pass snaps (defense)
      const playerDefTotalSnaps: Record<string, number> = {}; // espnId -> total def snaps

      // Also build gsis -> jersey -> espnId bridge from participation
      const gsisToJersey: Record<string, string> = {}; // gsis -> jersey number
      const jerseyTeamToEspnId: Record<string, string> = {}; // jersey:teamAbbr -> espnId

      // Build jersey bridge from participation offense/defense numbers
      for (const row of participationRows) {
        const offGsis = (row["offense_players"] || "").split(";").filter(Boolean);
        const offNums = (row["offense_numbers"] || "").split(";").filter(Boolean);
        const defGsis = (row["defense_players"] || "").split(";").filter(Boolean);
        const defNums = (row["defense_numbers"] || "").split(";").filter(Boolean);
        offGsis.forEach((gsis, i) => { if (offNums[i]) gsisToJersey[gsis] = offNums[i]; });
        defGsis.forEach((gsis, i) => { if (defNums[i]) gsisToJersey[gsis] = defNums[i]; });
      }

      // Build jersey+team -> ESPN player ID map from teamPlayers
      // teamAbbr[teamId] = abbreviation, abbrToTeamId[abbr] = teamId (built at ~line 1073)
      // These are defined later in the code but the loop below runs after that point
      // so we re-derive them here from competition data directly
      const _teamAbbrMap: Record<string, string> = {}; // teamId -> abbr
      for (const comp of (competition?.competitors || [])) {
        const tid = String(comp.team?.id);
        const abbr = comp.team?.abbreviation || "";
        if (tid && abbr) _teamAbbrMap[tid] = abbr;
      }

      for (const [teamId, players] of Object.entries(teamPlayers)) {
        const abbr = _teamAbbrMap[teamId] || teamId;
        for (const player of players as any[]) {
          if (player.jersey) {
            jerseyTeamToEspnId[`${player.jersey}:${abbr}`] = player.id;
            jerseyTeamToEspnId[`${player.jersey}:${teamId}`] = player.id;
          }
        }
      }

      // Determine the two team abbreviations in this game
      const _gameTeamAbbrs = Object.values(_teamAbbrMap); // [abbr1, abbr2]

      // Count pass snaps per player from participation
      for (const partRow of participationRows) {
        const pbpRow = pbpByNflversePlayId[partRow["play_id"]];
        if (!pbpRow) continue;
        const isPassPlay = pbpRow["pass_attempt"] === "1" && pbpRow["sack"] !== "1";
        const possTeam = partRow["possession_team"] || "";
        // Defense team = the other team in this game
        const defTeamAbbr = _gameTeamAbbrs.find(a => a !== possTeam) || "";
        const possTeamId = Object.entries(_teamAbbrMap).find(([, a]) => a === possTeam)?.[0] || "";
        const defTeamId = Object.entries(_teamAbbrMap).find(([, a]) => a === defTeamAbbr)?.[0] || "";

        const offGsis = (partRow["offense_players"] || "").split(";").filter(Boolean);
        const defGsis = (partRow["defense_players"] || "").split(";").filter(Boolean);
        const offNums = (partRow["offense_numbers"] || "").split(";").filter(Boolean);
        const defNums = (partRow["defense_numbers"] || "").split(";").filter(Boolean);

        if (isPassPlay) {
          offGsis.forEach((gsis, i) => {
            const jersey = offNums[i] || gsisToJersey[gsis] || "";
            const espnId = jerseyTeamToEspnId[`${jersey}:${possTeamId}`]
              || jerseyTeamToEspnId[`${jersey}:${possTeam}`] || "";
            if (espnId) playerOffPassSnaps[espnId] = (playerOffPassSnaps[espnId] || 0) + 1;
          });
          defGsis.forEach((gsis, i) => {
            const jersey = defNums[i] || gsisToJersey[gsis] || "";
            const espnId = jerseyTeamToEspnId[`${jersey}:${defTeamId}`]
              || jerseyTeamToEspnId[`${jersey}:${defTeamAbbr}`] || "";
            if (espnId) playerDefPassSnaps[espnId] = (playerDefPassSnaps[espnId] || 0) + 1;
          });
        }

        // Total defense snaps
        defGsis.forEach((gsis, i) => {
          const jersey = defNums[i] || gsisToJersey[gsis] || "";
          const espnId = jerseyTeamToEspnId[`${jersey}:${defTeamId}`]
            || jerseyTeamToEspnId[`${jersey}:${defTeamAbbr}`] || "";
          if (espnId) playerDefTotalSnaps[espnId] = (playerDefTotalSnaps[espnId] || 0) + 1;
        });
      }

      // Build snap count lookups with multiple keys for robust matching
      const snapByName: Record<string, Record<string, string>> = {};     // full name
      const snapByLastName: Record<string, Record<string, string>> = {}; // last name only
      const snapByPfrId: Record<string, Record<string, string>> = {};    // pfr_player_id

      for (const row of snapCountRows) {
        const fullName = (row["player"] || "").toLowerCase();
        snapByName[fullName] = row;
        // Last name = last word in full name
        const lastName = fullName.split(" ").pop() || "";
        if (lastName && !snapByLastName[lastName]) snapByLastName[lastName] = row;
        // PFR ID
        if (row["pfr_player_id"]) snapByPfrId[row["pfr_player_id"]] = row;
      }

      // Build nflverse player penalty lookup by name
      const penaltyByName: Record<string, { penalties: number; penaltyYards: number }> = {};
      for (const row of playerStatsRows) {
        const pens = parseInt(row["penalties"] || "0");
        const yds = parseInt(row["penalty_yards"] || "0");
        if (pens > 0) {
          const name = row["player_display_name"]?.toLowerCase();
          if (name) penaltyByName[name] = { penalties: pens, penaltyYards: yds };
        }
      }

      // ── Pre-build CB stats from nflverse PBP (avoids ESPN text truncation) ────────
      // nflverse PBP key fields for CB coverage:
      //   pass_defense_1_player_name — the primary DB who deflected/PD'd the ball (most reliable)
      //   pass_defense_2_player_name — secondary defender on the play
      //   solo_tackle_1_player_name  — tackler AFTER a completion (not the covering CB)
      //   interception_player_name   — who picked it off
      //   receiver_player_name       — targeted receiver (on completions)
      //
      // Strategy:
      //  - Incomplete (PD): attribute to pass_defense_1_player_name (the actual CB)
      //  - Complete:        attribute to solo_tackle_1 (first tackler = likely the covering CB)
      //  - Interception:    attribute to interception_player_name AND pass_defense_1 if set
      //  This gives Mitchell credit for his 3 PDs and marks his targets correctly.
      const cbStatsFromNflverse: Record<string, {
        timesTargeted: number; receptionsAllowed: number; yardsAllowed: number;
        airYardsAllowed: number; tdsAllowed: number; passesDefended: number;
        interceptions: number; contestedCatches: number; dropsForced: number;
      }> = {};

      function initCBEntry() {
        return { timesTargeted: 0, receptionsAllowed: 0, yardsAllowed: 0, airYardsAllowed: 0,
                 tdsAllowed: 0, passesDefended: 0, interceptions: 0, contestedCatches: 0, dropsForced: 0 };
      }
      function getCB(name: string) {
        if (!cbStatsFromNflverse[name]) cbStatsFromNflverse[name] = initCBEntry();
        return cbStatsFromNflverse[name];
      }

      const nflverseHasCBData = pbpRows.length > 0;

      if (nflverseHasCBData) {
        for (const row of pbpRows) {
          if (row["pass_attempt"] !== "1" || row["sack"] === "1" || row["qb_kneel"] === "1") continue;

          const isComplete = row["complete_pass"] === "1";
          const isInterception = row["interception"] === "1";
          const yds = parseFloat(row["yards_gained"] || "0");
          const airYds = parseFloat(row["air_yards"] || "0");
          const isTouchdown = row["touchdown"] === "1" || row["pass_touchdown"] === "1";

          // Pass defense: the CB who deflected/PD'd (most reliable field, populated on PDs)
          const pdPlayer1 = row["pass_defense_1_player_name"] || "";
          const pdPlayer2 = row["pass_defense_2_player_name"] || "";
          // Tackler after completion (not necessarily the covering CB, but best proxy for completions)
          const tackler = row["solo_tackle_1_player_name"] || row["assist_tackle_1_player_name"] || "";

          const ftnR = ftnByPlayId[row["play_id"]];
          const isContested = ftnR?.is_contested_ball === "TRUE";
          const isDrop = ftnR?.is_drop === "TRUE";

          if (isComplete && !isInterception) {
            // Completion: credit the tackling CB (best available proxy for who was covering)
            if (tackler) {
              const s = getCB(tackler);
              s.timesTargeted += 1;
              s.receptionsAllowed += 1;
              s.yardsAllowed += Math.max(0, yds);
              s.airYardsAllowed += Math.max(0, airYds);
              if (isTouchdown) s.tdsAllowed += 1;
              if (isContested) s.contestedCatches += 1;
            }
            // If pass_defense_1 is also populated on a completion, it's a contested catch — credit PD
            if (pdPlayer1 && pdPlayer1 !== tackler) {
              const s = getCB(pdPlayer1);
              // Only a partial target credit (CB was there but ball was caught anyway)
              s.timesTargeted += 1;
              s.receptionsAllowed += 1;
              s.yardsAllowed += Math.max(0, yds);
              s.airYardsAllowed += Math.max(0, airYds);
              if (isTouchdown) s.tdsAllowed += 1;
              if (isContested) s.contestedCatches += 1;
            }
          }

          if (!isComplete && !isInterception) {
            // Incomplete: THE most reliable signal — pass_defense_1 is the CB who broke it up
            // Even when the ball just falls incomplete (no PD), pdPlayer1 may be empty — that's OK,
            // we can only track definitive signals (PDs and targets per participation data)
            if (pdPlayer1) {
              const s = getCB(pdPlayer1);
              s.timesTargeted += 1;
              s.passesDefended += 1;
              if (isDrop) s.dropsForced += 1;
            }
            if (pdPlayer2 && pdPlayer2 !== pdPlayer1) {
              const s = getCB(pdPlayer2);
              s.passesDefended += 1;
            }
            // If no pass defender credited but ball fell incomplete, we still want to count
            // as a target for the nearest defender (use tackler as last resort — e.g. CB
            // was nearby but ball was just thrown away / out of bounds)
            if (!pdPlayer1 && !isDrop && tackler) {
              // Very loose — only if no PD was credited
              getCB(tackler).timesTargeted += 1;
            }
          }

          // Interceptions: credit to the intercepting player
          if (isInterception) {
            const intPlayer = row["interception_player_name"] || pdPlayer1 || tackler;
            if (intPlayer) {
              const s = getCB(intPlayer);
              s.interceptions += 1;
              s.timesTargeted += 1;
              s.passesDefended += 1; // INT = ultimate PD
            }
          }
        }
      }

      // ── Build per-player OL grades from play-by-play text ──────────────────
      // Parse drives already in summaryData — no extra HTTP call needed
      const drives = summaryData.drives?.previous || [];
      const allPlays: any[] = [];
      for (const drive of drives) {
        for (const play of (drive.plays || [])) {
          play._offTeamId = String(drive.team?.id || "");
          allPlays.push(play);
        }
      }

      // Build last-name → player lookup per team (for matching penalty/sack text)
      // e.g. "T.Smith" → playerId
      const teamNameMap: Record<string, Record<string, string>> = {}; // teamId -> abbrevName -> playerId
      for (const [teamId, players] of Object.entries(teamPlayers)) {
        teamNameMap[teamId] = {};
        for (const player of players as any[]) {
          const parts = (player.name || "").split(" ");
          // "Jordan Davis" -> "J.Davis"; "L. Johnson" -> "L.Johnson"
          if (parts.length >= 2) {
            const first = parts[0].replace(/\./g, "");
            const last = parts.slice(1).join(" ").replace(/\./g, "");
            const abbrev = first[0] + "." + last;
            teamNameMap[teamId][abbrev.toLowerCase()] = player.id;
            // Also index by just the last name for fuzzy matching
            teamNameMap[teamId][last.toLowerCase()] = player.id;
          }
        }
      }

      // OL per-player stats accumulator — enriched with nflverse data
      const olStats: Record<string, {
        sacksAllowed: number;        // unit sacks (excl QBFaultSacks)
        pressuresAllowed: number;    // from pbp_participation was_pressure
        passSnaps: number;           // pass protection snaps
        blitzSnaps: number;          // snaps where defense blitzed (harder to protect)
        holdingPenalties: number;    // from nflverse stats + ESPN penalty text
        falseStarts: number;
        otherPenalties: number;
        gapCarries: number;          // runs through this player's gap
        gapYards: number;
        gapTDs: boolean;
        offenseSnaps: number;        // total snaps from snap_counts
        highBoxSnaps: number;        // snaps with 7+ defenders in box (hard run blocking)
      }> = {};

      function getOrInitOL(pid: string) {
        if (!olStats[pid]) olStats[pid] = {
          sacksAllowed: 0, pressuresAllowed: 0, passSnaps: 0, blitzSnaps: 0,
          holdingPenalties: 0, falseStarts: 0, otherPenalties: 0,
          gapCarries: 0, gapYards: 0, gapTDs: false,
          offenseSnaps: 0, highBoxSnaps: 0,
        };
        return olStats[pid];
      }

      // ── Build ESPN ID → individual OL position using nflverse depth chart ──────────
      // depth_charts has exact LT/RT/LG/RG/C per player with espn_id
      // Maps: teamAbbr -> { LT: espnId, RT: espnId, LG: espnId, RG: espnId, C: espnId }
      const teamAbbr: Record<string, string> = {}; // teamId -> abbreviation
      for (const comp of (competition?.competitors || [])) {
        teamAbbr[String(comp.team?.id)] = comp.team?.abbreviation || "";
      }
      const abbrToTeamId: Record<string, string> = {};
      for (const [tid, abbr] of Object.entries(teamAbbr)) abbrToTeamId[abbr] = tid;

      // ── Jersey number → ESPN ID lookup (built FIRST, needed for GSIS bridging below) ──
      const teamJerseyToEspnId: Record<string, Record<string, string>> = {};
      for (const [teamId, players] of Object.entries(teamPlayers)) {
        teamJerseyToEspnId[teamId] = {};
        for (const player of players as any[]) {
          const jersey = String(player.jersey || "");
          if (jersey) teamJerseyToEspnId[teamId][jersey] = player.id;
        }
      }

      // ── Build GSIS → ESPN ID map from participation data (covers pre-2025 depth charts) ──
      // Participation has both GSIS (offense_players) and jersey numbers (offense_numbers)
      // ESPN player records have jersey numbers; so: GSIS -> jersey -> ESPN ID
      const gsisToEspnId: Record<string, string> = {}; // gsis_id -> espn_id
      for (const row of participationRows) {
        const posTeam = row["possession_team"] || "";
        const posTeamId = abbrToTeamId[posTeam] || "";
        const gsisIds = (row["offense_players"] || "").split(";");
        const jerseys = (row["offense_numbers"] || "").split(";");
        for (let i = 0; i < gsisIds.length; i++) {
          const gsis = gsisIds[i];
          const jersey = jerseys[i];
          if (!gsis || !jersey || !posTeamId) continue;
          if (!gsisToEspnId[gsis]) {
            const espnId = (teamJerseyToEspnId[posTeamId] || {})[jersey];
            if (espnId) gsisToEspnId[gsis] = espnId;
          }
        }
        // Also check defense side for CB coverage tracking
        const defTeamId = abbrToTeamId[row["possession_team"] === abbrToTeamId[Object.keys(abbrToTeamId)[0]] ? Object.keys(abbrToTeamId)[1] : Object.keys(abbrToTeamId)[0]] || "";
        const defGsis = (row["defense_players"] || "").split(";");
        const defNums = (row["defense_numbers"] || "").split(";");
        // Find which team is defending (opposite of possession)
        const defAbbr = Object.keys(abbrToTeamId).find(a => a !== (row["possession_team"] || "")) || "";
        const defTeamIdReal = abbrToTeamId[defAbbr] || "";
        for (let i = 0; i < defGsis.length; i++) {
          const gsis = defGsis[i];
          const jersey = defNums[i];
          if (!gsis || !jersey || !defTeamIdReal) continue;
          if (!gsisToEspnId[gsis]) {
            const espnId = (teamJerseyToEspnId[defTeamIdReal] || {})[jersey];
            if (espnId) gsisToEspnId[gsis] = espnId;
          }
        }
      }

      // espnId -> exact OL slot (LT/RT/LG/RG/C)
      const espnIdToOLSlot: Record<string, string> = {};
      // teamId -> { slot -> espnId }  (for gap lookup)
      const teamOLSlot: Record<string, Record<string, string>> = {};

      for (const row of depthChartRows) {
        const slot = row["pos_abb"]; // LT, RT, LG, RG, C
        const team = row["team"];
        const teamId = abbrToTeamId[team];
        if (!teamId || !slot) continue;

        // Resolve ESPN ID: try direct espn_id, then bridge via gsis_id, then jersey number
        let espnId = row["espn_id"] || "";
        if (!espnId && row["gsis_id"]) {
          espnId = gsisToEspnId[row["gsis_id"]] || "";
        }
        if (!espnId && row["jersey_number"]) {
          espnId = (teamJerseyToEspnId[teamId] || {})[row["jersey_number"]] || "";
        }
        if (!espnId) continue;

        if (!espnIdToOLSlot[espnId]) espnIdToOLSlot[espnId] = slot;
        if (!teamOLSlot[teamId]) teamOLSlot[teamId] = {};
        if (!teamOLSlot[teamId][slot]) teamOLSlot[teamId][slot] = espnId;
      }

      // Fallback: if depth chart didn't load, build espnIdToOLSlot from ESPN position
      // (all OT go to OT, all G go to G, all C go to C - less precise)
      for (const [teamId, players] of Object.entries(teamPlayers)) {
        if (!teamOLSlot[teamId]) teamOLSlot[teamId] = {};
        for (const player of players as any[]) {
          const pos = player.position?.toUpperCase();
          if (!espnIdToOLSlot[player.id] && ["OT","G","C","OG","OL"].includes(pos)) {
            // Assign a generic slot as fallback
            espnIdToOLSlot[player.id] = pos === "C" ? "C" : pos === "G" ? "G" : "OT";
          }
        }
      }

      // ── Build snap count per individual player from participation data ──────────
      // (teamJerseyToEspnId already built above — used for OL pass/run snap tracking)
      const playerTotalSnaps: Record<string, number> = {}; // espnId -> total snaps on field
      const playerPassSnaps: Record<string, number> = {};  // espnId -> offensive pass play snaps
      const playerRunSnaps: Record<string, number> = {};   // espnId -> offensive run play snaps

      for (const row of participationRows) {
        const hasPassPlay = !!(row["time_to_throw"]);
        const posTeam = row["possession_team"] || "";
        const posTeamId = abbrToTeamId[posTeam] || "";

        // ── Offense side (OL snap tracking) ──
        const jerseys = (row["offense_numbers"] || "").split(";");
        for (let i = 0; i < jerseys.length; i++) {
          const jersey = jerseys[i];
          if (!jersey || !posTeamId) continue;
          const espnId = (teamJerseyToEspnId[posTeamId] || {})[jersey];
          if (!espnId) continue;
          playerTotalSnaps[espnId] = (playerTotalSnaps[espnId] || 0) + 1;
          if (hasPassPlay) playerPassSnaps[espnId] = (playerPassSnaps[espnId] || 0) + 1;
          else playerRunSnaps[espnId] = (playerRunSnaps[espnId] || 0) + 1;
        }
      }

      // ── Map (run_location, run_gap) → individual player IDs ───────────────────────
      // Uses EXACT depth chart slot: LT/RT/LG/RG/C
      // location: left | middle | right
      // gap:      tackle | guard | end | (empty = pure middle)
      function getGapPlayerIds(teamId: string, location: string, gap: string): string[] {
        const slots = teamOLSlot[teamId] || {};
        const loc = (location || "").toLowerCase();
        const g = (gap || "").toLowerCase();

        if (g === "tackle" || g === "end") {
          const id = loc === "left" ? slots["LT"] : loc === "right" ? slots["RT"] : null;
          return id ? [id] : [];
        }
        if (g === "guard") {
          if (loc === "left") return slots["LG"] ? [slots["LG"]] : [];
          if (loc === "right") return slots["RG"] ? [slots["RG"]] : [];
          // Middle guard = both guards share
          return [slots["LG"], slots["RG"]].filter(Boolean) as string[];
        }
        if (loc === "middle" && !g) {
          // Pure middle = C + both guards (C gets full credit, guards get 0.5 each)
          return [slots["C"], slots["LG"], slots["RG"]].filter(Boolean) as string[];
        }
        return [];
      }

      // All OL IDs per team (for unit-level sack distribution)
      const teamOLByPos: Record<string, Record<string, string[]>> = {};
      // Also build a flat list of all OL starters per team for sack fallback
      const teamOLStarters: Record<string, string[]> = {};
      for (const [teamId, players] of Object.entries(teamPlayers)) {
        teamOLByPos[teamId] = {};
        teamOLStarters[teamId] = [];
        for (const player of players as any[]) {
          // Use athletePositionMap first — roster-only OL have position set there
          const pos = (athletePositionMap[player.id] || player.position || "").toUpperCase();
          if (["OT","G","C","OG","OL"].includes(pos)) {
            if (!teamOLByPos[teamId][pos]) teamOLByPos[teamId][pos] = [];
            teamOLByPos[teamId][pos].push(player.id);
            teamOLStarters[teamId].push(player.id);
          }
        }
      }

      // ── CB per-player stats accumulator ─────────────────────────────────────────
      // Keyed by player name (from tackle credit in PBP desc) and GSIS id when available
      const cbStats: Record<string, {
        primaryCoverageSnaps: number;  // defense snaps from pbp_participation
        timesTargeted: number;         // pass plays where they made the stop (or allowed yards)
        receptionsAllowed: number;
        yardsAllowed: number;
        airYardsAllowed: number;
        tdsAllowed: number;
        passesDefended: number;        // already in ESPN box score but we'll count from PBP too
        interceptions: number;
        contestedCatches: number;      // from FTN is_contested_ball
        dropsForced: number;           // from FTN is_drop on plays they defended
        coverageType: string;          // dominant coverage type (man/zone)
        snapsCoveredManCoverage: number;
        incomplete_no_target: number;  // plays where they were on field but not targeted = good coverage
      }> = {};

      function getOrInitCB(name: string) {
        const key = name.toLowerCase();
        if (!cbStats[key]) cbStats[key] = {
          primaryCoverageSnaps: 0, timesTargeted: 0, receptionsAllowed: 0,
          yardsAllowed: 0, airYardsAllowed: 0, tdsAllowed: 0, passesDefended: 0,
          interceptions: 0, contestedCatches: 0, dropsForced: 0,
          coverageType: "", snapsCoveredManCoverage: 0, incomplete_no_target: 0,
        };
        return cbStats[key];
      }

      // Process plays
      for (const play of allPlays) {
        const text: string = play.text || "";
        const offTeamId = play._offTeamId;
        const playType = play.type?.text || "";
        const yds: number = play.statYardage ?? 0;
        const textLower = text.toLowerCase();
        // ESPN play IDs are numeric; nflverse play IDs may differ
        // Try to match by play text sequence (we'll use the sequenceNumber)
        const espnSeq = String(play.sequenceNumber || "");

        // Look up nflverse data for this play
        // ESPN play ID = gameId + nflverse play_id (e.g. "40177251071" -> "71")
        const espnPlayId = String(play.id || "");
        const nflvPlayId = espnPlayId.replace(gameId, "");
        const ftnRow = ftnByPlayId[nflvPlayId] || null;
        const partRow = partByPlayId[nflvPlayId] || null;

        // ── SACK PLAYS ──────────────────────────────────────────────────────
        // Sacks are a UNIT failure: shared penalty across all 5 OL starters
        // (No free API can tell us which specific lineman was beaten on a sack)
        // FTN: is_qb_fault_sack = QB scrambled into pressure (OL not at fault)
        if (playType === "Sack" || playType === "Sack Opp Fumble Recovery" || (playType === "Penalty" && textLower.includes("sacked"))) {
          const isQbFaultSack = ftnRow?.is_qb_fault_sack === "TRUE";
          if (!isQbFaultSack) {
            // Only charge starters (players with significant snaps), not backups
            const slots = teamOLSlot[offTeamId] || {};
            const starterIds = ["LT","RT","LG","RG","C"].map(s => slots[s]).filter(Boolean) as string[];
            // If depth chart didn't load, fall back to all OL starters
            const toCharge = starterIds.length > 0 ? starterIds
              : teamOLStarters[offTeamId] || Object.values(teamOLByPos[offTeamId] || {}).flat();
            for (const pid of toCharge) {
              getOrInitOL(pid).sacksAllowed += 1;
            }
          }
        }

        // ── RUSH PLAYS ─────────────────────────────────────────────────────
        // Uses nflverse PBP run_location + run_gap to map carries to exact individual
        if (["Rush", "Rushing Touchdown"].includes(playType)) {
          const isQbScramble = textLower.includes("scramble") || textLower.includes("kneels");
          if (!isQbScramble) {
            // Get clean run_location + run_gap from nflverse PBP
            // ESPN play IDs: gameid prefix + nflverse play_id
            // e.g. ESPN "40177251071" for gameId "401772510" -> nflverse "71"
            const espnPlayId = String(play.id || "");
            const nflversePlayId = espnPlayId.replace(gameId, "");
            const pbpRow = pbpByNflversePlayId[nflversePlayId];
            const runLoc = pbpRow?.["run_location"] || "";
            const runGap = pbpRow?.["run_gap"] || "";

            // Get defenders in box for context
            const defInBox = parseInt(ftnRow?.["n_defense_box"] || partRow?.["defenders_in_box"] || "0");
            const isHighBox = defInBox >= 7;

            if (runLoc) {
              // Get the specific player(s) responsible for this gap
              const responsibleIds = getGapPlayerIds(offTeamId, runLoc, runGap);

              for (let ri = 0; ri < responsibleIds.length; ri++) {
                const pid = responsibleIds[ri];
                const s = getOrInitOL(pid);
                // For pure middle (C+LG+RG all share), weight by role:
                // C gets full credit, guards get 0.5 each on pure middle
                const isMidGuard = runGap === "" && (ri === 1 || ri === 2); // guard index in middle
                const shareWeight = isMidGuard ? 0.5 : 1.0;
                s.gapCarries += shareWeight;
                s.gapYards += yds * shareWeight;
                if (playType === "Rushing Touchdown") s.gapTDs = true;
                if (isHighBox) s.highBoxSnaps += shareWeight;
              }
            }
          }
        }

        // ── PASS SNAPS (count individual pass protection volume) ─────────────
        // was_pressure is play-level (team signal), not individual
        // We track pass snaps per individual for snap-count weighting only
        if (["Pass Reception", "Pass Incompletion", "Sack", "Sack Opp Fumble Recovery", "Passing Touchdown", "Pass Interception Return"].includes(playType) || (playType === "Penalty" && textLower.includes("sacked"))) {
          const nBlitzers = parseInt(ftnRow?.n_blitzers || "0");
          const wasBlitzed = nBlitzers >= 5;
          const slots = teamOLSlot[offTeamId] || {};
          const starterIds = ["LT","RT","LG","RG","C"].map(s => slots[s]).filter(Boolean) as string[];
          const allOL = starterIds.length > 0 ? starterIds
            : teamOLStarters[offTeamId] || Object.values(teamOLByPos[offTeamId] || {}).flat();
          for (const pid of allOL) {
            const s = getOrInitOL(pid);
            s.passSnaps += 1;
            if (wasBlitzed) s.blitzSnaps += 1;
          }
        }

        // ── CB COVERAGE TRACKING ──────────────────────────────────────────
        // Use nflverse PBP (parsed fields) > ESPN text (regex) for accuracy.
        // nflverse has solo_tackle_1_player_name, complete_pass, yards_gained etc.
        // ESPN text regex fails for out-of-bounds plays, spike plays, etc.
        if (["Pass Reception", "Pass Incompletion"].includes(playType)) {
          const isComplete = playType === "Pass Reception";

          // Try nflverse PBP first (most reliable)
          const nflvPlayId2 = String(play.id || "").replace(gameId, "");
          const pbpRow2 = pbpByNflversePlayId[nflvPlayId2];

          // Primary tackler: nflverse solo_tackle_1_player_name (e.g. "Q.Mitchell")
          // or assist tackler if primary is empty
          let primaryTackler = pbpRow2?.["solo_tackle_1_player_name"]
            || pbpRow2?.["assist_tackle_1_player_name"]
            || "";

          // Fallback to ESPN text parsing if nflverse PBP not available
          // Pattern: "for N yards (F.Lastname)" at end of play text
          // Must handle: period at end, space before PENALTY, newline, or end-of-string
          if (!primaryTackler) {
            // Try parenthetical at end first: "... (F.Lastname)."
            const tacklerMatch = text.match(/\(([A-Z]\.[\w'\-]+)(?:;[\w.' ]*)?\)[\.\s,]?$/) ||
                                 text.match(/\(([A-Z]\.[\w'\-]+)(?:;[\w.' ]*)?\)/);
            primaryTackler = tacklerMatch?.[1] || "";
          }

          if (isComplete && primaryTackler) {
            const cb = getOrInitCB(primaryTackler);
            cb.timesTargeted += 1;
            cb.receptionsAllowed += 1;
            cb.yardsAllowed += Math.max(0, yds);

            const airYds = parseFloat(pbpRow2?.["air_yards"] || "0") ||
              (textLower.includes("deep") ? 20 : textLower.includes("short") ? 8 : 12);
            cb.airYardsAllowed += airYds;

            // TDs allowed — check if a touchdown was scored
            if ((pbpRow2?.["touchdown"] === "1") || textLower.includes("touchdown")) {
              cb.tdsAllowed += 1;
            }

            if (ftnRow?.is_contested_ball === "TRUE") cb.contestedCatches += 1;
          }

          if (!isComplete && primaryTackler) {
            const cb = getOrInitCB(primaryTackler);
            cb.timesTargeted += 1;
            cb.passesDefended += 1;
            if (ftnRow?.is_drop === "TRUE") cb.dropsForced += 1;
          }

          // Also check for pass defensed credit in ESPN text: "[N.PlayerName]" on incompletions
          if (!isComplete && !primaryTackler) {
            const pdMatch = text.match(/\[([A-Z]\.[\w'-]+)\]/);
            if (pdMatch) {
              const cb = getOrInitCB(pdMatch[1]);
              cb.passesDefended += 1;
              cb.timesTargeted += 1;
            }
          }
        }

        // ── PENALTY PLAYS ───────────────────────────────────────────────────
        // Parse: "PENALTY on TEAM-P.LastName, PenaltyType"
        const penRegex = /PENALTY on ([A-Z]+)-([A-Z]\.\S+),\s*([^,;]+)/gi;
        let penMatch;
        while ((penMatch = penRegex.exec(text)) !== null) {
          const penTeamAbbr = penMatch[1];
          const penPlayerAbbr = penMatch[2]; // e.g. "T.Smith"
          const penType = penMatch[3].trim().toLowerCase();

          // Find which team this is (we need abbr → teamId)
          let penTeamId: string | null = null;
          for (const comp of (competition?.competitors || [])) {
            if (comp.team?.abbreviation?.toUpperCase() === penTeamAbbr) {
              penTeamId = String(comp.team.id);
              break;
            }
          }
          if (!penTeamId) continue;

          // Resolve player name to ID
          const playerAbbrevLower = penPlayerAbbr.toLowerCase();
          const pid = teamNameMap[penTeamId]?.[playerAbbrevLower];
          if (!pid) continue;

          const isOL = ["OT","G","C","OG","OL"].includes((teamPlayers[penTeamId] as any[])?.find((p: any) => p.id === pid)?.position?.toUpperCase());
          if (!isOL) continue;

          if (penType.includes("holding")) {
            getOrInitOL(pid).holdingPenalties += 1;
          } else if (penType.includes("false start") || penType.includes("illegal formation") || penType.includes("illegal shift")) {
            getOrInitOL(pid).falseStarts += 1;
          } else {
            getOrInitOL(pid).otherPenalties += 1;
          }
        }
      }

      // Attach individual OL stats to each OL player
      // Also enrich with nflverse snap counts and nflverse penalty data
      // IMPORTANT: use athletePositionMap for position resolution — box score players
      // don't have a position set yet (it's only set in the scoring loop below).
      for (const [, players] of Object.entries(teamPlayers)) {
        for (const player of players as any[]) {
          const pos = (athletePositionMap[player.id] || player.position || "").toUpperCase();
          if (["OT","G","C","OG","OL","LS"].includes(pos)) {
            const base = olStats[player.id] || {
              sacksAllowed: 0, pressuresAllowed: 0, passSnaps: 0, blitzSnaps: 0,
              holdingPenalties: 0, falseStarts: 0, otherPenalties: 0,
              gapCarries: 0, gapYards: 0, gapTDs: false, offenseSnaps: 0, highBoxSnaps: 0,
            };

            // Enrich from nflverse snap counts
            const snapData = snapByName[player.name?.toLowerCase()];
            if (snapData) {
              base.offenseSnaps = parseInt(snapData["offense_snaps"] || "0");
            }

            // Enrich penalty data from nflverse stats_player (more reliable than ESPN text)
            const penData = penaltyByName[player.name?.toLowerCase()];
            if (penData) {
              // Don't double-count if ESPN text already found penalties
              if (penData.penalties > base.holdingPenalties + base.falseStarts + base.otherPenalties) {
                // nflverse has more - use its total but keep distribution from ESPN text parsing
                const nlvTotal = penData.penalties;
                const espnTotal = base.holdingPenalties + base.falseStarts + base.otherPenalties;
                if (nlvTotal > espnTotal) {
                  base.otherPenalties += nlvTotal - espnTotal;
                }
              }
            }

            // Attach depth slot (LT/LG/C/RG/RT) so client can order OL correctly
            const slot = espnIdToOLSlot[player.id];
            if (slot) player.depthSlot = slot;

            player._olStats = base;
          }

          // Attach CB/S stats
          if (["CB","DB","S","SS","FS","SAF"].includes(pos)) {
            const nameParts = player.name?.split(" ") || [];
            const abbrev = nameParts.length >= 2
              ? `${nameParts[0][0]}.${nameParts.slice(1).join(" ")}`.toLowerCase()
              : player.name?.toLowerCase();
            const lastName = nameParts[nameParts.length - 1]?.toLowerCase();

            // Prefer nflverse PBP data (avoids ESPN text truncation issues)
            // nflverse uses abbreviated names: "Q.Mitchell", "C.DeJean" etc.
            // Build abbreviated name from ESPN full name for lookup
            const nflvAbbrev = nameParts.length >= 2
              ? `${nameParts[0][0]}.${nameParts.slice(1).join(" ")}`
              : player.name || "";

            const nflverseCBData = cbStatsFromNflverse[nflvAbbrev]
              || cbStatsFromNflverse[abbrev.toUpperCase().replace(/^(.)/, (m) => m.toUpperCase())]
              // Also try last name only as fallback
              || Object.entries(cbStatsFromNflverse).find(([k]) =>
                   k.toLowerCase().endsWith(`.${lastName}`) || k.toLowerCase() === lastName
                 )?.[1];

            // Fall back to ESPN-text-parsed cbStats
            const espnCBData = cbStats[abbrev] || cbStats[lastName] ||
              Object.entries(cbStats).find(([k]) => k.includes(lastName || ""))?.[1];

            // Use nflverse if available, otherwise ESPN text
            const cbData = nflverseCBData || espnCBData;

            if (!player._cbStats) player._cbStats = {};
            if (cbData) Object.assign(player._cbStats, cbData);

            // Attach accurate individual pass snap count from participation GSIS tracking
            // This is the KEY signal: targets / defPassSnaps = individual target rate
            const indivDefPassSnaps = playerDefPassSnaps[player.id];
            const indivDefTotalSnaps = playerDefTotalSnaps[player.id];
            if (indivDefPassSnaps !== undefined) {
              player._cbStats.individualPassSnaps = indivDefPassSnaps;
              player._cbStats.individualTotalSnaps = indivDefTotalSnaps || 0;
            }

            // Also attach snap count from nflverse snap_counts
            const snapData = snapByName[player.name?.toLowerCase()]
              || snapByLastName[lastName || ""];
            if (snapData) {
              player._cbStats.defenseSnaps = parseInt(snapData["defense_snaps"] || "0");
            } else if (indivDefTotalSnaps !== undefined) {
              player._cbStats.defenseSnaps = indivDefTotalSnaps;
            }
          }

          // ── Attach WR/TE nflverse enrichment ───────────────────────────────────
          // wrStatsFromNflverse is keyed by nflverse abbreviated name ("J.Jefferson")
          if (["WR","TE","RB","FB"].includes(pos)) {
            const nameParts = player.name?.split(" ") || [];
            const nflvAbbrev = nameParts.length >= 2
              ? `${nameParts[0][0]}.${nameParts.slice(1).join(" ")}`
              : player.name || "";
            const lastName2 = nameParts[nameParts.length - 1]?.toLowerCase() || "";
            const wrData = wrStatsFromNflverse[nflvAbbrev]
              || Object.entries(wrStatsFromNflverse).find(([k]) =>
                   k.toLowerCase().endsWith(`.${lastName2}`)
                 )?.[1];
            if (wrData) player._wrStats = wrData;
          }

          // ── Attach LB/DL nflverse pass rush enrichment ────────────────────────
          // passRusherStats is keyed by nflverse abbreviated name ("H.Reddick")
          if (["LB","ILB","OLB","MLB","DE","DT","NT","DL","IDL","ED","S","SS","FS","SAF"].includes(pos)) {
            const nameParts2 = player.name?.split(" ") || [];
            const nflvAbbrev2 = nameParts2.length >= 2
              ? `${nameParts2[0][0]}.${nameParts2.slice(1).join(" ")}`
              : player.name || "";
            const lastName3 = nameParts2[nameParts2.length - 1]?.toLowerCase() || "";
            const prData = passRusherStats[nflvAbbrev2]
              || Object.entries(passRusherStats).find(([k]) =>
                   k.toLowerCase().endsWith(`.${lastName3}`)
                 )?.[1];
            if (prData) {
              if (["LB","ILB","OLB","MLB"].includes(pos)) player._lbStats = prData;
              else player._dlStats = prData; // DL and Safeties share the same data shape
            }
          }
        }
      }

      // ── Team-level context (still used for team-wide rush/sack data) ──────
      const teamContext: Record<string, { sacks: number; rushYards: number; rushTDs: number; rushCarries: number }> = {};

      for (const [teamId, players] of Object.entries(teamPlayers)) {
        let sacks = 0, rushYards = 0, rushTDs = 0, rushCarries = 0;
        for (const player of players as any[]) {
          if (player.stats?.passing) {
            const sacksStr = player.stats.passing["SACKS"] || "0-0";
            sacks += parseFloat(sacksStr.split("-")[0]) || 0;
          }
          if (player.stats?.rushing) {
            const pos = player.position?.toUpperCase();
            if (["RB", "FB", "WR", "TE"].includes(pos)) {
              rushYards += parseFloat(player.stats.rushing["YDS"]) || 0;
              rushTDs += parseFloat(player.stats.rushing["TD"]) || 0;
              rushCarries += parseFloat(player.stats.rushing["CAR"]) || 0;
            }
          }
        }
        teamContext[teamId] = { sacks, rushYards, rushTDs, rushCarries };
      }

      // Assign positions from our map + compute SundayScores
      for (const [teamId, players] of Object.entries(teamPlayers)) {
        const ctx = teamContext[teamId] || { sacks: 0, rushYards: 0, rushTDs: 0, rushCarries: 0 };
        for (const player of players as any[]) {
          if (athletePositionMap[player.id]) {
            player.position = athletePositionMap[player.id];
          } else if (!player.position || player.position === "?") {
            const cats = Object.keys(player.stats || {});
            if (cats.includes("passing")) player.position = "QB";
            else if (cats.includes("rushing") && !cats.includes("receiving")) player.position = "RB";
            else if (cats.includes("receiving")) player.position = "WR";
            else if (cats.includes("defensive")) player.position = "DEF";
            else if (cats.includes("kicking")) player.position = "K";
            else if (cats.includes("punting")) player.position = "P";
            else player.position = "?";
          }
          player._teamCtx = ctx;

          // ── Snap count — used to determine if player actually played ────────
          // Try nflverse snap_counts first (most accurate), then participation data
          const snapRow = snapByName[player.name?.toLowerCase()];
          const offSnaps = parseInt(snapRow?.["offense_snaps"] || "0");
          const defSnaps = parseInt(snapRow?.["defense_snaps"] || "0");
          const stSnaps  = parseInt(snapRow?.["st_snaps"]      || "0");

          // Try multiple name-matching strategies for nflverse snap_counts
          // (ESPN uses truncated names; nflverse uses full names)
          const playerNameFull = player.name?.toLowerCase() || "";
          // Also try last name only
          const playerLastName = playerNameFull.split(" ").pop() || "";
          // Also try abbreviated first+last (e.g. "C.Hughlett")
          const playerNameParts = player.name?.split(" ") || [];
          const playerAbbrev = playerNameParts.length >= 2
            ? (playerNameParts[0][0] + "." + playerNameParts.slice(1).join(" ")).toLowerCase()
            : playerNameFull;

          const resolvedSnap = snapRow
            || snapByLastName[playerLastName]  // "Sieg" -> finds "Trent Sieg"
            || snapByName[playerLastName];      // single-word name edge case

          if (resolvedSnap) {
            const rOff = parseInt(resolvedSnap["offense_snaps"] || "0");
            const rDef = parseInt(resolvedSnap["defense_snaps"] || "0");
            const rSt  = parseInt(resolvedSnap["st_snaps"]      || "0");
            player.snapsPlayed = rOff + rDef + rSt;
            player.offenseSnapsPlayed = rOff;
            player.defenseSnapsPlayed = rDef;
          } else if (playerTotalSnaps[player.id] !== undefined) {
            // Participation-derived: offense snaps = pass + run snaps on offense
            const pOff = (playerPassSnaps[player.id] || 0) + (playerRunSnaps[player.id] || 0);
            player.snapsPlayed = playerTotalSnaps[player.id];
            player.offenseSnapsPlayed = pOff;
            player.defenseSnapsPlayed = null; // not tracked in participation for defense
          } else if (Object.keys(player.stats || {}).length > 0) {
            player.snapsPlayed = 1;
            player.offenseSnapsPlayed = (player.stats?.passing || player.stats?.rushing || player.stats?.receiving) ? 1 : 0;
            player.defenseSnapsPlayed = player.stats?.defensive ? 1 : 0;
          } else {
            player.snapsPlayed = null;
            player.offenseSnapsPlayed = null;
            player.defenseSnapsPlayed = null;
          }

          // Inject snapsPlayed into stats so gradeOL can use it for isStarter check
          if (!player.stats) player.stats = {};
          player.stats._snapsPlayed = player.snapsPlayed || 0;
          player.sundayScore = computeSundayScore(player);
          delete player.stats._snapsPlayed; // clean up internal field
        }
      }

      const result = {
        gameId,
        date: competition?.date,
        status: competition?.status?.type?.description,
        homeTeam: {
          id: homeComp?.team?.id,
          name: homeComp?.team?.displayName,
          abbreviation: homeComp?.team?.abbreviation,
          logo: homeComp?.team?.logo,
          score: homeComp?.score,
          color: homeComp?.team?.color,
          alternateColor: homeComp?.team?.alternateColor,
        },
        awayTeam: {
          id: awayComp?.team?.id,
          name: awayComp?.team?.displayName,
          abbreviation: awayComp?.team?.abbreviation,
          logo: awayComp?.team?.logo,
          score: awayComp?.score,
          color: awayComp?.team?.color,
          alternateColor: awayComp?.team?.alternateColor,
        },
        players: teamPlayers,
      };


  setCached(gameId, JSON.stringify(result));
  return result;
}

export function getSeasons(): number[] { return getAvailableSeasons(); }

export function getWeeks() {
  const weeks: { value: string; label: string; seasonType: number }[] = [];
  for (let w = 1; w <= 18; w++) weeks.push({ value: `2-${w}`, label: `Week ${w}`, seasonType: 2 });
  weeks.push({ value: "3-1", label: "Wild Card", seasonType: 3 });
  weeks.push({ value: "3-2", label: "Divisional", seasonType: 3 });
  weeks.push({ value: "3-3", label: "Championship", seasonType: 3 });
  weeks.push({ value: "3-5", label: "Super Bowl", seasonType: 3 });
  return weeks;
}

export async function fetchGames(year: string, week: string) {
  const [seasonType, weekNum] = week.split("-");
  const url = `${ESPN_BASE}/scoreboard?seasontype=${seasonType}&week=${weekNum}&dates=${year}&limit=20`;
  const data = await fetchJson(url);
  return (data.events || [])
    .filter((e: any) => ["STATUS_FINAL","STATUS_FULL_TIME"].includes(e.competitions?.[0]?.status?.type?.name))
    .map((e: any) => {
      const comp = e.competitions[0];
      const home = comp.competitors.find((c: any) => c.homeAway === "home");
      const away = comp.competitors.find((c: any) => c.homeAway === "away");
      return {
        id: e.id, date: e.date, name: e.shortName,
        homeTeam: { id: home?.team?.id, name: home?.team?.displayName, abbreviation: home?.team?.abbreviation, logo: home?.team?.logo, score: home?.score },
        awayTeam: { id: away?.team?.id, name: away?.team?.displayName, abbreviation: away?.team?.abbreviation, logo: away?.team?.logo, score: away?.score },
      };
    });
}

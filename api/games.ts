import type { VercelRequest, VercelResponse } from "@vercel/node";
import { fetchGames } from "./_lib.js";

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const { year, week } = req.query as { year: string; week: string };
    if (!year || !week) {
      return res.status(400).json({ error: "year and week required" });
    }
    const games = await fetchGames(year, week);
    res.json({ games });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

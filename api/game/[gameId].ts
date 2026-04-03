import type { VercelRequest, VercelResponse } from "@vercel/node";
import { fetchGameData } from "../_lib.js";

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const gameId = req.query.gameId as string;
    const forceRefresh = req.query.refresh === "true";
    const data = await fetchGameData(gameId, forceRefresh);
    res.json(data);
  } catch (err: any) {
    console.error("Game fetch error:", err);
    res.status(500).json({ error: err.message });
  }
}

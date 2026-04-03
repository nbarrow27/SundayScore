import type { VercelRequest, VercelResponse } from "@vercel/node";
import { getSeasons } from "./_lib.js";

export default function handler(_req: VercelRequest, res: VercelResponse) {
  res.json({ seasons: getSeasons() });
}

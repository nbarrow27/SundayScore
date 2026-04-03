import type { VercelRequest, VercelResponse } from "@vercel/node";
import { getWeeks } from "./_lib.js";

export default function handler(_req: VercelRequest, res: VercelResponse) {
  res.json({ weeks: getWeeks() });
}

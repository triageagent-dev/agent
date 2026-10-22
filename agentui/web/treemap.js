// Panel 1's treemap: the listed applications as tiles grouped by namespace,
// domain or tier, sized by catalog weight or traffic, coloured by readiness.
// A tile with an open issue gets a warning border. Laid out squarified in a
// 1000×600 box and drawn as absolutely placed divs in percent, so it fills
// whatever width the pane has.
import { esc } from "./ui.js";

const W = 1000, H = 600, HEAD = 22, PAD = 3;

// squarify places items ({ v }) in the rectangle, rows along the short side,
// keeping each row's worst aspect ratio as low as it goes (Bruls et al.).
export function squarify(items, x, y, w, h) {
  const out = [];
  const total = items.reduce((a, it) => a + it.v, 0);
  if (!items.length || total <= 0 || w <= 0 || h <= 0) return out;
  const scale = (w * h) / total;
  let rest = items.map((it) => ({ it, a: it.v * scale })).sort((p, q) => q.a - p.a);
  while (rest.length) {
    const side = Math.min(w, h);
    let row = [rest[0]], best = worst(row, side), i = 1;
    for (; i < rest.length; i++) {
      const next = worst(row.concat(rest[i]), side);
      if (next > best) break;
      row.push(rest[i]); best = next;
    }
    rest = rest.slice(row.length);
    const sum = row.reduce((a, r) => a + r.a, 0);
    if (w >= h) {
      const cw = sum / h;
      let cy = y;
      row.forEach((r) => { const ch = r.a / cw; out.push({ it: r.it, x, y: cy, w: cw, h: ch }); cy += ch; });
      x += cw; w -= cw;
    } else {
      const ch = sum / w;
      let cx = x;
      row.forEach((r) => { const cw = r.a / ch; out.push({ it: r.it, x: cx, y, w: cw, h: ch }); cx += cw; });
      y += ch; h -= ch;
    }
  }
  return out;
}
function worst(row, side) {
  const s = row.reduce((a, r) => a + r.a, 0), s2 = s * s, l2 = side * side;
  let m = 0;
  row.forEach((r) => { m = Math.max(m, (l2 * r.a) / s2, s2 / (l2 * r.a)); });
  return m;
}

export const GROUPS = [["ns", "namespace"], ["domain", "domain"], ["tier", "tier"]];
export const SIZES = [["weight", "weight"], ["traffic", "traffic"]];

const groupOf = (r, by) => {
  if (by === "domain") return (r.node && r.node.domain) || "(no domain)";
  if (by === "tier") return r.node && r.node.tier ? "tier " + r.node.tier : "(no tier)";
  return r.ns || "(no namespace)";
};
const fill = (r) => (r.drift ? "transparent" : !r.readyKnown ? "var(--faint2)" : r.ready ? "var(--ok)" : "var(--crit)");
const pct = (n, of) => +((n / of) * 100).toFixed(3) + "%";
const box = (b) => `left:${pct(b.x, W)};top:${pct(b.y, H)};width:${pct(b.w, W)};height:${pct(b.h, H)}`;

// treemapHtml draws rows (each with its index in the app list as data-i, so
// a tap selects it like a list row). issueSvcs names services with an open
// issue.
export function treemapHtml(rows, { by = "ns", size = "weight", issueSvcs = new Set() } = {}) {
  if (!rows.length) return `<div class="empty">no applications to map</div>`;
  const val = (r) => (size === "traffic" ? (r.golden && r.golden.reqPerSec) || 0 : r.weight || 1);
  // Quiet services keep a sliver so they stay visible when sized by traffic.
  const floor = Math.max(...rows.map(val)) * 0.02 || 1;
  const groups = new Map();
  rows.forEach((r, i) => {
    const k = groupOf(r, by);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push({ r, i, v: Math.max(val(r), floor) });
  });
  const gItems = [...groups.entries()].map(([k, tiles]) => ({ k, tiles, v: tiles.reduce((a, t) => a + t.v, 0) }));
  let html = "";
  squarify(gItems, 0, 0, W, H).forEach((g) => {
    const head = g.h > HEAD * 2.2 && g.w > 60 ? HEAD : 0;
    html += `<div class="tmg" style="${box(g)}">${head ? `<span class="tmh">${esc(g.it.k)}</span>` : ""}</div>`;
    const inner = { x: g.x + PAD, y: g.y + head + PAD, w: g.w - PAD * 2, h: g.h - head - PAD * 2 };
    squarify(g.it.tiles, inner.x, inner.y, inner.w, inner.h).forEach((t) => {
      const r = t.it.r, warn = issueSvcs.has(r.name);
      const label = t.w > 70 && t.h > 16 ? `<span class="tml">${esc(r.name)}</span>` : "";
      const tip = `${r.name}${r.ns ? " · " + r.ns : ""}${r.cluster ? " · " + r.cluster : ""} — ${r.state}${warn ? " · open issue" : ""}`;
      html += `<div class="tmt${r.drift ? " drift" : ""}${warn ? " warn" : ""}" data-act="sel" data-list="app" data-i="${t.it.i}" title="${esc(tip)}" style="${box({ x: t.x + 1, y: t.y + 1, w: Math.max(0, t.w - 2), h: Math.max(0, t.h - 2) })};background:${fill(r)}">${label}</div>`;
    });
  });
  return `<div class="tm">${html}</div>`;
}

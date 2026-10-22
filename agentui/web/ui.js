// Shared rendering helpers for the Triage Agent TUI (from the core TUI's ui.js).
export const DASH = "—";
export const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
export const v = (x) => (x == null || x === "" ? DASH : x);
export const sub = (t, attrs = "") => `<div class="sub"${attrs}><span>&#8212; ${esc(t)}</span></div>`;
export const kv = (k, val, cls = "", s = "", w = "") => `<div class="kv ${w}"><span class="k">${esc(k)}</span><span class="v ${cls}">${esc(v(val))}</span>${s ? `<span class="s">${esc(s)}</span>` : ""}</div>`;
// Column widths are given in px at the old 13px base and emitted in em, so
// they scale with the font size (--fs) instead of truncating at 15px.
export const em = (px) => +(px / 13).toFixed(2) + "em";
export const cellW = (w, cls, text, extra = "") => `<span class="c ${cls}" style="width:${em(w)}"${extra}>${esc(text)}</span>`;

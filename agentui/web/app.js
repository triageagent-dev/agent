// Triage Agent TUI — browser client for the agent's local API. The same
// chrome and key model as the core TUI, with two panels: app and src.
// Plain ES module, no build step.
import { esc, v, sub, kv, cellW, em, DASH } from "./ui.js";
import { treemapHtml, GROUPS, SIZES } from "./treemap.js";

// ── constants ───────────────────────────────────────────────────────────────
const POLL_MS = 5000;
const DEFAULT_EVENTS = 100; // events held and drawn until /api/v1/status names the agent's limit (limits.events)
const KEY_TENANT = "triage-agent-tui.tenant";
const KEY_PANEL = "triage-agent-tui.panel";
const KEYS_KEY = "triage-agent-tui.keys"; // on-screen key row: "shown" or absent
const THEME_KEY = "triage-agent-tui.theme"; // colour scheme: "dim" or absent
const PANELS = ["app", "src"];
const PANEL_LABEL = { app: "app", src: "sensors" };
const READY_CYCLE = ["", "not-ready", "ready", "unknown"];
const KIND_ORDER = ["k8s", "host", "ebpf", "node-exporter", "log-exporter", "otlp"];

const store = {
  get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
  set(k, val) { try { localStorage.setItem(k, val); } catch (e) { /* private mode */ } },
  del(k) { try { localStorage.removeItem(k); } catch (e) { /* private mode */ } },
};

function applyTheme(dim) {
  document.documentElement.toggleAttribute("data-theme", dim);
  if (dim) document.documentElement.dataset.theme = "dim";
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.content = dim ? "#17181f" : "#1b1d2b";
}
applyTheme(store.get(THEME_KEY) === "dim");

// ── state ───────────────────────────────────────────────────────────────────
const S = {
  status: null, statusErr: false,
  apps: null, services: new Map(), graph: null, sources: null,
  events: [], eventsLoaded: false,
  keysShown: store.get(KEYS_KEY) === "shown",
  shiftHeld: false,
  panel: PANELS.includes(store.get(KEY_PANEL)) ? store.get(KEY_PANEL) : "app",
  tenant: store.get(KEY_TENANT) || "",
  filterText: "", filterEditing: false, readyFilter: "",
  selApp: -1, appDetail: false,
  appView: "graph", sigWin: "12h", tmGroup: "ns", tmSize: "weight",
  focus2: "src",          // src panel focus: src | evt
  selSrc: "",             // selected source id; "" is "all sources"
  selEvt: null,           // selected event key; null follows the newest
  evtDetail: null,        // event key shown in full
  flash: "", flashT: 0,
  scrollSel: false,
  auth: { required: false, authenticated: true }, locked: false, login: { err: "", until: 0, busy: false }, logoutConfirm: false,
  tntOpen: false, selTnt: 0, tenantRows: [],
  info: false, stats: null, config: null, cfgSel: 0, // the ? agent view
};

// ── helpers ─────────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const nowMs = () => Date.now();
const num = (x) => (typeof x === "number" && isFinite(x) ? x : 0);
const arr = (x) => (Array.isArray(x) ? x : []);
const sevClass = (s) => ({ critical: "fg-crit", warning: "fg-warn", info: "fg-info" }[String(s || "").toLowerCase()] || "fg-dim");
const sevBadge = (s) => {
  const k = String(s || "").toLowerCase();
  if (k === "critical") return "CRIT";
  if (k === "warning") return "WARN";
  if (k === "info") return "INFO";
  return (k || "?").toUpperCase().slice(0, 4);
};
const oneLine = (s) => String(s || "").replace(/\s+/g, " ").trim();
const flash = (msg) => { S.flash = msg; S.flashT = Date.now(); renderCrumb(); setTimeout(() => { if (Date.now() - S.flashT >= 2900) { S.flash = ""; renderCrumb(); } }, 3000); };
const matches = (hay) => !S.filterText || hay.toLowerCase().includes(S.filterText.toLowerCase());

// Timestamps arrive as ISO strings, unix seconds or unix milliseconds.
function toMs(ts) {
  if (ts == null || ts === "") return 0;
  if (typeof ts === "number") return ts < 1e11 ? ts * 1000 : ts;
  const n = Number(ts);
  if (isFinite(n) && String(ts).trim() !== "") return n < 1e11 ? n * 1000 : n;
  const t = new Date(ts).getTime();
  return isFinite(t) ? t : 0;
}
function ago(ts) {
  const t = toMs(ts);
  if (!t) return DASH;
  const s = Math.trunc((nowMs() - t) / 1000);
  if (s < 0) return "0s";
  if (s < 60) return s + "s";
  const m = Math.trunc(s / 60);
  if (m < 60) return m + "m";
  const h = Math.trunc(m / 60);
  if (h < 24) return h + "h";
  return Math.trunc(h / 24) + "d";
}
const agoS = (ts) => { const a = ago(ts); return a === DASH ? "" : a + " ago"; };
function fullTime(ts) {
  const t = toMs(ts);
  return t ? new Date(t).toISOString().replace("T", " ").slice(0, 19) + " UTC" : DASH;
}
function formatCount(n) {
  if (typeof n !== "number" || !isFinite(n)) return DASH;
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(Math.trunc(n));
}
const pct = (x) => (x === 0 ? "0%" : x < 0.1 ? x.toFixed(2) + "%" : x < 10 ? x.toFixed(1) + "%" : x.toFixed(0) + "%");

// ── derived data ────────────────────────────────────────────────────────────
const appKey = (a) => (a.namespace ? a.namespace + "/" : "") + a.name;

// The services an app owns, joined on "ns/service".
const svcsOf = (a) => arr(a.services).map((k) => S.services.get(k)).filter(Boolean);

// RED for one app: the agent's own traffic block when present, else the
// sum of its services.
function trafficOf(a) {
  const ss = svcsOf(a);
  const t = a.traffic;
  let rps = null, errPct = null, p99 = null, at = 0;
  ss.forEach((s) => { at = Math.max(at, toMs(s.at)); });
  if (t && typeof t.rps === "number") { rps = t.rps; errPct = num(t.errPct); p99 = num(t.p99Ms); }
  else if (ss.length) {
    rps = ss.reduce((x, s) => x + num(s.rps), 0);
    const req = ss.reduce((x, s) => x + num(s.req), 0), err = ss.reduce((x, s) => x + num(s.err), 0);
    errPct = req > 0 ? (err / req) * 100 : rps > 0 ? ss.reduce((x, s) => x + num(s.rps) * num(s.errPct), 0) / rps : 0;
    p99 = Math.max(0, ...ss.map((s) => num(s.p99Ms)));
  }
  return rps == null ? null : { rps, errPct, p99, at };
}
// USE and LOG for one app: the worst figure across its services.
function useOf(a) {
  const us = svcsOf(a).filter((s) => s.use);
  if (!us.length) return null;
  const mx = (k) => Math.max(0, ...us.map((s) => num(s.use[k])));
  return { cpuPct: mx("cpuPct"), memPct: mx("memPct"), cpuPsiPct: mx("cpuPsiPct"), memPsiPct: mx("memPsiPct"), ioPsiPct: mx("ioPsiPct"), oomKills: us.reduce((x, s) => x + num(s.use.oomKills), 0), at: Math.max(...us.map((s) => toMs(s.use.at))) };
}
function logOf(a) {
  const ls = svcsOf(a).filter((s) => s.log);
  if (!ls.length) return null;
  const sm = (k) => ls.reduce((x, s) => x + num(s.log[k]), 0);
  return { errors: sm("errors"), warns: sm("warns"), total: sm("total"), at: Math.max(...ls.map((s) => toMs(s.log.at))) };
}
// Series for one app: rps summed, error rate weighted by rps.
function seriesOf(a) {
  const ss = svcsOf(a).filter((s) => arr(s.rpsSeries).length);
  if (!ss.length) return null;
  const n = Math.max(...ss.map((s) => s.rpsSeries.length));
  const rps = [], err = [];
  for (let i = 0; i < n; i++) {
    let r = 0, e = 0;
    ss.forEach((s) => {
      const k = i - (n - s.rpsSeries.length);
      if (k < 0) return;
      const x = num(s.rpsSeries[k]);
      r += x; e += x * num(arr(s.errSeries)[k]);
    });
    rps.push(r); err.push(r > 0 ? e / r : 0);
  }
  return { rps, err };
}

function appRows() {
  return arr(S.apps).filter((a) => {
    if (S.readyFilter && a.ready !== S.readyFilter) return false;
    return matches([a.name, a.namespace, a.tier, a.domain].join(" "));
  }).map((a) => {
    const t = trafficOf(a);
    return { a, key: appKey(a), t };
  });
}
// Row shape the shared treemap draws.
function treemapRows(rows) {
  return rows.map(({ a, t }) => ({
    name: a.name, ns: a.namespace, state: a.ready, ready: a.ready === "ready", readyKnown: a.ready !== "unknown", drift: false,
    weight: parseFloat(a.weight) || 1, golden: { reqPerSec: t ? t.rps : 0 }, node: { domain: a.domain, tier: a.tier },
  }));
}

function srcRows() {
  const all = arr(S.sources && S.sources.sources);
  const rank = (k) => { const i = KIND_ORDER.indexOf(k); return i < 0 ? KIND_ORDER.length : i; };
  const list = all.filter((s) => S.focus2 !== "src" || matches([s.name, s.node, s.kind, s.status].join(" ")))
    .sort((p, q) => rank(p.kind) - rank(q.kind) || String(p.kind).localeCompare(String(q.kind)) || String(p.name).localeCompare(String(q.name)));
  return [{ all: true, id: "" }, ...list];
}
const selSource = () => arr(S.sources && S.sources.sources).find((s) => s.id === S.selSrc) || null;
const evLimit = () => Math.max(1, Number(S.status && S.status.limits && S.status.limits.events) || DEFAULT_EVENTS);
const evKey = (e) => e.id || toMs(e.ts) + "|" + e.family + "|" + e.reason;
function evtRowsAll() {
  const src = selSource();
  const fams = src && arr(src.families).length ? new Set(src.families) : null;
  return S.events.filter((e) => (!fams || fams.has(e.family)) &&
    (S.focus2 !== "evt" || matches([e.family, e.namespace, e.service, e.reason, e.message, e.severity].join(" "))));
}
const evtRows = () => evtRowsAll().slice(0, evLimit());
const selEvtIndex = (rows) => {
  if (S.selEvt == null) return rows.length ? 0 : -1;
  const i = rows.findIndex((e) => evKey(e) === S.selEvt);
  return i < 0 ? (rows.length ? 0 : -1) : i;
};
const selSrcIndex = (rows) => Math.max(0, rows.findIndex((s) => s.id === S.selSrc));

// ── data loading ────────────────────────────────────────────────────────────
async function getJSON(path) {
  const r = await fetch(path, { cache: "no-store", headers: { Accept: "application/json" } });
  if (r.status === 401) toLogin();
  if (!r.ok) throw new Error(path + " " + r.status);
  return r.json();
}
// Every fetch carries the selected tenant; empty is all tenants.
const tq = (path) => path + (S.tenant ? (path.includes("?") ? "&" : "?") + "tenant=" + encodeURIComponent(S.tenant) : "");
const tenantList = () => arr(S.status && S.status.tenants);
let refreshing = false, again = false;
async function refresh() {
  if (S.locked) return;
  if (refreshing) { again = true; return; }
  refreshing = true;
  const tn = S.tenant;
  const stale = () => tn !== S.tenant;
  const jobs = [
    getJSON(tq("./api/v1/status")).then((d) => { if (!stale()) { S.status = d; S.statusErr = false; if (S.events.length > evLimit()) S.events.length = evLimit(); } }, () => { if (!stale()) S.statusErr = true; }),
    getJSON(tq("./api/v1/apps")).then((d) => { if (!stale()) S.apps = arr(d.apps); }, () => {}),
    getJSON(tq("./api/v1/graph")).then((d) => { if (!stale()) S.graph = d; }, () => {}),
    getJSON(tq("./api/v1/sources")).then((d) => { if (!stale()) S.sources = d; }, () => {}),
    getJSON(tq("./api/v1/services")).then((d) => { if (!stale()) S.services = new Map(arr(d.services).map((s) => [s.namespace + "/" + s.service, s])); }, () => {}),
  ];
  if (!S.eventsLoaded) jobs.push(loadEvents());
  await Promise.all(jobs);
  refreshing = false;
  // A stored tenant the agent no longer lists falls back to all.
  // A tenant is always selected: the stored one if still listed, else the first.
  const ts = tenantList();
  if (S.status) {
    const pref = S.tenant || store.get(KEY_TENANT) || "";
    const want = ts.length ? (ts.includes(pref) ? pref : ts[0]) : "";
    if (want !== S.tenant) { applyTenant(want, false); return; }
  }
  render();
  if (again) { again = false; refresh(); }
}
async function loadEvents() {
  const tn = S.tenant;
  try {
    const d = await getJSON(tq("./api/v1/events?limit=" + evLimit()));
    if (tn !== S.tenant) return;
    addEvents(arr(d.events));
    S.eventsLoaded = true;
  } catch (e) { /* retried on the next poll */ }
}
function addEvents(list) {
  const seen = new Set(S.events.map(evKey));
  const fresh = list.filter((e) => e && !seen.has(evKey(e)));
  if (!fresh.length) return;
  S.events = S.events.concat(fresh).sort((p, q) => toMs(q.ts) - toMs(p.ts));
  if (S.events.length > evLimit()) S.events.length = evLimit();
}

// ── live feed: /api/v1/stream (SSE) ────────────────────────────────────────
let es = null, esRetry = 0, esTimer = null;
function connectStream() {
  if (S.locked) return;
  clearTimeout(esTimer);
  if (es) { es.close(); es = null; }
  es = new EventSource(tq("./api/v1/stream"));
  const mine = es;
  es.onopen = () => {
    if (mine !== es) return;
    if (esRetry > 0) loadEvents().then(render); // fill the gap after a drop
    esRetry = 0;
  };
  es.addEventListener("event", (ev) => {
    let e;
    try { e = JSON.parse(ev.data); } catch (err) { return; }
    if (!e || typeof e !== "object") return;
    const seen = S.events.some((x) => evKey(x) === evKey(e));
    if (seen) return;
    S.events.unshift(e);
    if (S.events.length > evLimit()) S.events.length = evLimit();
    if (S.panel === "src") render();
  });
  es.onerror = () => {
    if (mine !== es) return;
    if (es) { es.close(); es = null; }
    // The stream gives no status: a 401 shows up as an error, so ask.
    probeAuth().then((a) => { if (a && a.required && !a.authenticated) toLogin(); });
    const wait = Math.min(30000, 1000 * Math.pow(2, esRetry++));
    esTimer = setTimeout(connectStream, wait);
  };
}

// ── rendering: chrome ──────────────────────────────────────────────────────
function renderTabBar() {
  const st = S.status, link = st && st.link;
  const conn = S.statusErr || !link ? { t: S.statusErr ? "agent unreachable" : "", c: "fg-crit" } : link.standalone ? { t: "standalone", c: "fg-dim" } : link.connected ? { t: "core connected", c: "fg-ok" } : { t: "core offline", c: "fg-crit" };
  $("tabbar").innerHTML =
    `<svg class="ti" viewBox="0 0 200 200" width="20" height="20" style="transform:rotate(-90deg)" aria-hidden="true"><circle cx="100" cy="100" r="84" fill="none" stroke="#8b8bf0" stroke-width="16" stroke-linecap="round" stroke-dasharray="422 528"/><circle cx="100" cy="100" r="56" fill="none" stroke="#22d3ee" stroke-width="16" stroke-linecap="round" stroke-dasharray="264 352"/><circle cx="100" cy="100" r="28" fill="none" stroke="#e879f9" stroke-width="16" stroke-linecap="round" stroke-dasharray="123 176"/></svg>` +
    `<span class="tname">Triage agent</span>` +
    `<span class="fill"></span>` +
    (conn.t ? `<span class="who ${conn.c}"><span class="dot"></span>${esc(conn.t)}</span>` : "") +
    (st && st.cluster ? `<span class="fg-violet">${esc(st.cluster)}</span>` : "") +
    (S.tenant ? `<span class="fg-mag">${esc(S.tenant)}</span>` : "") +
    (st && st.version ? `<span class="ver">agent ${esc(st.version)}</span>` : "");
}

function renderCrumb() {
  const bits = [];
  const chip = (label, val, clear) => `<button type="button" class="fchip" data-act="clear" data-v="${clear}"><span>${esc(label)}</span><span class="fg-strong">${esc(val)}</span><span class="x">&#215;</span></button>`;
  if (S.filterText && !S.filterEditing) bits.push(chip("/", S.filterText, "text"));
  if (S.readyFilter && S.panel === "app") bits.push(chip("state", S.readyFilter, "ready"));
  if (S.flash) bits.push(`<span class="flash">${esc(S.flash)}</span>`);
  $("crumb-info").innerHTML = bits.join("");
  const p = $("prompt");
  if (p.hidden === S.filterEditing) p.hidden = !S.filterEditing;
}

function renderLegend() {
  const P = S.panel;
  // Shifted keys (capitals) are shown only while Shift is held — the
  // hardware key, or the latching ⇧ on the on-screen key row.
  const chip = (key, label, act, opts = {}) => (/^[A-Z?]$/.test(key) && !S.shiftHeld) ? "" :
    `<button type="button" class="chip${opts.on ? " on" : ""}${opts.na ? " na" : ""}" data-act="key" data-k="${esc(act)}"><span class="kk">&lt;${esc(key)}&gt;</span><span class="kl${opts.val ? " val" : ""}">${esc(label)}</span></button>`;
  const extra = [];
  if (P === "app") {
    extra.push(chip("m", S.appView === "treemap" ? "graph" : "treemap", "m"));
    if (S.appView === "treemap") extra.push(chip("g", "by " + S.tmGroup, "g", { val: true }), chip("z", "by " + S.tmSize, "z", { val: true }));
    extra.push(chip("s", "state " + (S.readyFilter || "all"), "s", { val: true }));
  }
  if (S.info) extra.length = 0;
  if (P === "src" && !S.info) extra.push(chip("tab", S.focus2 === "src" ? "events" : "sources", "Tab"));
  extra.push(chip("?", "agent", "?", { on: S.info }));
  const panels = PANELS.map((p, i) => `<button type="button" class="chip${S.panel === p ? " on" : ""}" data-act="key" data-k="${i + 1}"><span class="kk">${i + 1}</span><span class="kl">${PANEL_LABEL[p]}</span></button>`);
  extra.push(chip("T", "tenant", "T", { on: S.tntOpen }));
  if (S.auth.required) extra.push(chip("q", S.logoutConfirm ? "sign out? y/n" : "logout", "q", { on: S.logoutConfirm }));
  $("legend").innerHTML = [
    ...panels,
    ...extra,
    chip("D", document.documentElement.dataset.theme === "dim" ? "bright" : "dim", "D"),
    `<button type="button" class="chip keys-toggle${S.keysShown ? " on" : ""}" data-act="keys" aria-pressed="${S.keysShown}" title="${S.keysShown ? "hide keys" : "show keys"}"><i class="ph-fill ph-keyboard" style="margin:0;font-size:16px;"></i></button>`,
  ].join("");
  $("keybar").classList.toggle("keys-hidden", !S.keysShown);
}

function renderChrome() {
  renderTabBar();
  renderCrumb();
  renderLegend();
}

function renderPanel() {
  const el = $("panel");
  const saved = {};
  el.querySelectorAll("[data-scroll]").forEach((n) => { saved[n.dataset.scroll] = n.scrollTop; });
  const lo = S.logoutConfirm ? logoutBox() : "";
  el.innerHTML = lo + (S.info ? infoPanel() : S.panel === "src" ? srcPanel() : appPanel());
  el.querySelectorAll("[data-scroll]").forEach((n) => { if (saved[n.dataset.scroll] != null) n.scrollTop = saved[n.dataset.scroll]; });
  if (S.scrollSel) { S.scrollSel = false; revealSel(el); }
}
function revealSel(el) {
  el.querySelectorAll(".tr.sel").forEach((n) => {
    const box = n.closest("[data-scroll]");
    if (!box) return;
    const top = n.getBoundingClientRect().top - box.getBoundingClientRect().top + box.scrollTop, bottom = top + n.offsetHeight;
    if (top < box.scrollTop) box.scrollTop = top;
    else if (bottom > box.scrollTop + box.clientHeight) box.scrollTop = bottom - box.clientHeight;
  });
}
function render() {
  if (S.locked) return;
  renderChrome();
  renderPanel();
  renderModal();
}

// ── panel 1: app ───────────────────────────────────────────────────────────
function appPanel() {
  const rows = appRows(), all = arr(S.apps);
  if (S.selApp >= rows.length) S.selApp = rows.length - 1;
  const selRow = S.selApp >= 0 ? rows[S.selApp] || null : null;
  const count = (st) => all.filter((a) => a.ready === st).length;
  const filt = (label, n, state, dot, ring) => `<button type="button" class="filt${S.readyFilter === state ? " on" : ""}" data-act="readyf" data-v="${state}"><span class="dot" style="${ring ? "background:transparent;box-shadow:inset 0 0 0 1.5px var(--faint)" : "background:" + dot}"></span>${label} ${n}</button>`;
  const list = rows.map(({ a, t }, i) => {
    const on = i === S.selApp, st = a.ready;
    const dot = st === "unknown" || !st ? `<span class="rdot ring"></span>` : `<span class="rdot" style="background:${st === "ready" ? "var(--ok)" : "var(--crit)"}"></span>`;
    const errCls = t && t.errPct >= 5 ? "fg-crit" : t && t.errPct >= 1 ? "fg-warn" : "fg-text";
    return `<div class="tr${on ? " sel" : ""}" data-act="sel" data-list="app" data-i="${i}">
      <span class="mark">${on ? "&#9654;" : ""}</span>${dot}
      <span class="c flex">${esc(a.name)}</span>
      ${cellW(72, "fg-label opt", a.namespace || DASH)}
      ${cellW(40, "r fg-violet", a.tier || DASH)}
      ${cellW(56, "r fg-text", t ? t.rps.toFixed(1) : DASH)}
      ${cellW(56, "r " + errCls, t ? t.errPct.toFixed(1) + "%" : DASH)}
    </div>`;
  }).join("");
  const right = S.appDetail && selRow ? appDetail(selRow) : graphPane(rows, selRow);
  return `<div class="grid fill">
    <div class="col g6">
      <div class="tbar"><span class="t">applications(${all.length})</span><span class="fill"></span><span class="hint">tap a row · <b>s</b> cycles state</span></div>
      <div class="filters">
        ${filt("All", all.length, "", "var(--dim)")}
        ${filt("ready", count("ready"), "ready", "var(--ok)")}
        ${filt("not-ready", count("not-ready"), "not-ready", "var(--crit)")}
        ${count("unknown") ? filt("unknown", count("unknown"), "unknown", "", true) : ""}
      </div>
      <div class="th"><span class="mark"></span><span style="width:0.69em;flex-shrink:0"></span><span class="c flex">APP</span>${cellW(72, "opt", "NS")}${cellW(40, "r", "TIER")}${cellW(56, "r", "RPS")}${cellW(56, "r", "ERR%")}</div>
      <div class="rows-fill"><div class="rows tall" data-scroll="app">${list || `<div class="empty">${S.apps ? "no app matches these filters" : "apps unavailable"}</div>`}</div></div>
    </div>
    <div class="col g6">${right}</div>
  </div>`;
}

// The right side: the dependency graph, or the treemap (m toggles).
function graphPane(rows, selRow) {
  const toggle = `<span class="tmopts"><span class="hint"><b>m</b></span>${[["graph", "graph"], ["treemap", "treemap"]].map(([val, l]) => `<button type="button" class="filt${S.appView === val ? " on" : ""}" data-act="appview" data-v="${val}">${l}</button>`).join("")}</span>`;
  if (S.appView === "treemap") {
    const opt = (act, cur, list) => list.map(([val, l]) => `<button type="button" class="filt${cur === val ? " on" : ""}" data-act="${act}" data-v="${val}">${l}</button>`).join("");
    return `<div class="tbar"><span class="t">app treemap(${rows.length})</span><span class="fill"></span>${toggle}</div>
      <div class="filters tmbar"><span class="fg-dim"><b class="fg-violet">g</b> group</span>${opt("tmgroup", S.tmGroup, GROUPS)}<span class="fill"></span><span class="fg-dim"><b class="fg-violet">z</b> size</span>${opt("tmsize", S.tmSize, SIZES)}</div>
      ${treemapHtml(treemapRows(rows), { by: S.tmGroup, size: S.tmSize })}`;
  }
  const G = buildGraph();
  const filtered = !!(S.filterText || S.readyFilter);
  let focus = null, keep = null;
  if (selRow) {
    focus = new Set([selRow.key, ...arr(selRow.a.callsTo), ...arr(selRow.a.calledBy)]);
  } else if (filtered) {
    focus = new Set(rows.map((r) => r.key));
    keep = new Set(focus);
    G.edges.forEach((e) => { if (focus.has(e.src)) keep.add(e.dst); if (focus.has(e.dst)) keep.add(e.src); });
  }
  const shown = keep ? G.verts.filter((x) => keep.has(x.id)) : G.verts;
  const head = keep ? `service graph(${shown.length} of ${G.verts.length})` : `service graph(${G.verts.length})`;
  return `<div class="tbar"><span class="t">${head}</span><span class="fill"></span>${toggle}</div>${graphSvg(G, keep, focus, selRow ? selRow.key : "")}${signalsBox()}`;
}

// The core's "signals" box, from this agent's own events: a bar per bucket
// and the window's count, per family. The event ring is capped, so a long
// window can reach back further than the ring does.
const SIG_WINDOWS = [["1h", 3600], ["6h", 21600], ["12h", 43200], ["24h", 86400]];
const SIG_FAMILIES = [["use", "resource pressure", "#22d3ee"], ["red", "request errors", "#ef4444"], ["log", "log errors", "#f59e0b"]];
const SIG_BUCKETS = 60;
function signalsBox() {
  const win = SIG_WINDOWS.find(([k]) => k === S.sigWin) || SIG_WINDOWS[2];
  const spanMs = win[1] * 1000, bucketMs = spanMs / SIG_BUCKETS, now = Date.now();
  const rows = SIG_FAMILIES.map(([fam, label, color]) => {
    const series = new Array(SIG_BUCKETS).fill(0);
    let total = 0;
    S.events.forEach((e) => {
      if (e.family !== fam) return;
      const age = now - toMs(e.ts);
      if (age < 0 || age >= spanMs) return;
      series[SIG_BUCKETS - 1 - Math.floor(age / bucketMs)]++;
      total++;
    });
    const max = Math.max(1, ...series);
    const bars = series.map((v, i) => { const h = v ? Math.max(1.5, (v / max) * 20) : 0.6; return `<rect x="${i * 3}" y="${(20 - h).toFixed(1)}" width="2.2" height="${h.toFixed(1)}" fill="${v ? color : "var(--rule2)"}"/>`; }).join("");
    return `<div class="gsigrow"><span class="gsl" style="color:${total > 0 ? color : "var(--ok)"}">${label}</span><span class="fg-strong gsv">${total}</span><svg class="gspark" viewBox="0 0 ${SIG_BUCKETS * 3} 20" preserveAspectRatio="none">${bars}</svg></div>`;
  }).join("");
  const buttons = SIG_WINDOWS.map(([k]) => `<button type="button" class="filt${win[0] === k ? " on" : ""}" data-act="sigwin" data-v="${k}">${k}</button>`).join("");
  return `<div class="tbar"><span class="t">signals · last ${win[0]} · ${Math.round(bucketMs / 60000)}m/bucket</span><span class="fill"></span><span class="tmopts">${buttons}</span></div><div class="gsigs">${rows}</div>`;
}

// Vertices are apps (from the graph nodes' app field) or bare workloads;
// edges are summed between vertices, external ones kept as stubs.
function buildGraph() {
  const g = S.graph || {}, verts = new Map(), wl = new Map();
  const appByName = new Map(arr(S.apps).map((a) => [a.name, a]));
  const vid = (ns, workload, app) => {
    if (app) {
      if (String(app).includes("/")) return app;
      const a = arr(S.apps).find((x) => x.name === app && x.namespace === ns) || appByName.get(app);
      return a ? appKey(a) : (ns ? ns + "/" : "") + app;
    }
    return wl.get(ns + "/" + workload) || (ns ? ns + "/" : "") + workload;
  };
  arr(g.nodes).forEach((n) => {
    const id = vid(n.namespace, n.workload, n.app);
    wl.set(n.namespace + "/" + n.workload, id);
    const x = verts.get(id) || { id, label: n.app || n.workload, rps: 0, errW: 0 };
    x.rps += num(n.rps); x.errW += num(n.rps) * num(n.errPct) || num(n.errPct) * 0.0001;
    verts.set(id, x);
  });
  const ensure = (id, label) => { if (!verts.has(id)) verts.set(id, { id, label, rps: 0, errW: 0 }); };
  const em = new Map(), ext = new Map();
  arr(g.edges).forEach((e) => {
    const src = wl.get(e.srcNamespace + "/" + e.srcWorkload) || e.srcNamespace + "/" + e.srcWorkload;
    ensure(src, e.srcWorkload);
    if (e.external || !e.dstWorkload) {
      const l = ext.get(src) || [];
      l.push({ protocol: e.protocol, cps: num(e.callsPerSec) });
      ext.set(src, l);
      return;
    }
    const dst = wl.get(e.dstNamespace + "/" + e.dstWorkload) || e.dstNamespace + "/" + e.dstWorkload;
    ensure(dst, e.dstWorkload);
    if (src === dst) return;
    const k = src + ">" + dst, cur = em.get(k) || { src, dst, cps: 0 };
    cur.cps += num(e.callsPerSec);
    em.set(k, cur);
  });
  const out = [...verts.values()].map((x) => ({ ...x, errPct: x.rps > 0 ? x.errW / x.rps : 0 }));
  return { verts: out, edges: [...em.values()], ext };
}
const errTone = (e, rps) => (rps <= 0 && e <= 0 ? "var(--faint)" : e >= 5 ? "var(--crit)" : e >= 1 ? "var(--warn)" : "var(--ok)");
function graphSvg(G, keep, focus, selId) {
  const sorted = G.verts.filter((x) => !keep || keep.has(x.id)).sort((a, b) => (a.id < b.id ? -1 : 1));
  if (!sorted.length) return `<div class="empty">no service graph</div>`;
  const pos = new Map(sorted.map((n, i) => {
    const a = (i / sorted.length) * Math.PI * 2 - Math.PI / 2;
    return [n.id, { x: 150 + Math.cos(a) * 92, y: 110 + Math.sin(a) * 92, a }];
  }));
  const byId = new Map(sorted.map((n) => [n.id, n]));
  const maxC = Math.max(1e-9, ...G.edges.map((e) => e.cps));
  const hot = (id) => !focus || focus.has(id);
  const edges = G.edges.filter((e) => pos.has(e.src) && pos.has(e.dst)).map((e) => {
    const p = pos.get(e.src), q = pos.get(e.dst), d = byId.get(e.dst);
    const lit = !selId || e.src === selId || e.dst === selId;
    const w = 0.6 + 2.6 * Math.sqrt(e.cps / maxC);
    const tone = d.errPct >= 5 ? "var(--crit)" : d.errPct >= 1 ? "var(--warn)" : "var(--faint2)";
    return `<path d="M${p.x.toFixed(1)},${p.y.toFixed(1)} Q150,110 ${q.x.toFixed(1)},${q.y.toFixed(1)}" fill="none" stroke="${tone}" stroke-width="${w.toFixed(2)}" stroke-opacity="${lit ? 0.85 : 0.15}"><title>${esc(e.src)} → ${esc(e.dst)} · ${e.cps.toFixed(2)} calls/s</title></path>`;
  }).join("");
  const dots = sorted.map((n) => {
    const p = pos.get(n.id);
    const r = 2.5 + Math.min(1, Math.sqrt(n.rps / Math.max(1e-9, ...sorted.map((x) => x.rps)))) * 3.5;
    const lx = 150 + Math.cos(p.a) * 104, ly = 110 + Math.sin(p.a) * 104;
    const anchor = Math.abs(Math.cos(p.a)) < 0.2 ? "middle" : Math.cos(p.a) > 0 ? "start" : "end";
    const stub = G.ext.get(n.id) ? `<line x1="${p.x.toFixed(1)}" y1="${p.y.toFixed(1)}" x2="${(150 + Math.cos(p.a) * 99).toFixed(1)}" y2="${(110 + Math.sin(p.a) * 99).toFixed(1)}" stroke="var(--faint)" stroke-dasharray="1.5 1.5" stroke-width="0.7"><title>${esc(G.ext.get(n.id).map((x) => x.protocol + " " + x.cps.toFixed(2) + "/s").join(", "))} (external)</title></line>` +
      `<circle cx="${(150 + Math.cos(p.a) * 100.5).toFixed(1)}" cy="${(110 + Math.sin(p.a) * 100.5).toFixed(1)}" r="1.4" fill="none" stroke="var(--faint)" stroke-width="0.6"/>` : "";
    return stub + `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${r.toFixed(1)}" fill="${errTone(n.errPct, n.rps)}"${n.id === selId ? ` stroke="var(--white)" stroke-width="1.2"` : ""}${hot(n.id) ? "" : ` fill-opacity="0.35"`}><title>${esc(n.id)} · ${n.rps.toFixed(1)} rps · ${n.errPct.toFixed(1)}% err</title></circle>` +
      (sorted.length <= 40 ? `<text x="${lx.toFixed(1)}" y="${(ly + 2.5).toFixed(1)}" text-anchor="${anchor}" class="glabel${hot(n.id) ? "" : " ctx"}">${esc(n.label)}</text>` : "");
  }).join("");
  return `<svg class="ggraph" viewBox="-30 0 360 220">${edges}${dots}</svg>`;
}

// ── app detail ─────────────────────────────────────────────────────────────
const LEVELS = { mem: [70, 85], cpu: [70, 85], psi: [1, 10], err: [1, 5] };
const levelOf = (key, x) => { const t = LEVELS[key]; return !t ? 0 : x >= t[1] ? 2 : x >= t[0] ? 1 : 0; };
const LEVEL_CLS = ["fg-strong", "fg-warn", "fg-crit"];
const SPARK_W = 30;
function spark(vals, color) {
  const tail = vals.slice(-SPARK_W), G = "▁▂▃▄▅▆▇█";
  const max = Math.max(0, ...tail);
  const pad = `<span class="fg-faint2">${"▁".repeat(Math.max(0, SPARK_W - tail.length))}</span>`;
  const bars = tail.map((x) => (x <= 0 || max <= 0 ? G[0] : G[Math.max(1, Math.min(7, Math.floor((x * 7) / max)))])).join("");
  return `${pad}<span class="spark" style="color:${max > 0 ? color : "var(--faint2)"}">${bars}</span>`;
}
function goldenSparks(a, t) {
  const sr = seriesOf(a);
  if (!sr || !t) return "";
  const lv = levelOf("err", t.errPct);
  const row = (label, val, cls, series, color) => `<div class="sigrow"><span class="sl">${label}</span><span class="sn wide ${cls}">${esc(val)}</span>${spark(series, color)}</div>`;
  let html = row("net", t.rps.toFixed(1) + "rps", "fg-strong", sr.rps, "var(--info)");
  html += row("err", t.errPct.toFixed(1) + "%", LEVEL_CLS[lv], sr.err, "var(--crit)");
  return sub("signals") + html;
}
function goldenRows(a, t, u, l) {
  const rows = [];
  if (t) rows.push(["red", "fg-crit", `${t.rps.toFixed(1)} rps · ${t.errPct.toFixed(1)}% err${t.p99 > 0 ? " · p99 " + Math.round(t.p99) + "ms" : ""}${t.at ? " · " + agoS(t.at) : ""}`]);
  if (u) {
    const worst = Math.max(levelOf("mem", u.memPct), levelOf("cpu", u.cpuPct), levelOf("psi", Math.max(u.cpuPsiPct, u.memPsiPct, u.ioPsiPct)), u.oomKills > 0 ? 2 : 0);
    rows.push(["use", ["fg-ok", "fg-warn", "fg-crit"][worst], `cpu ${pct(u.cpuPct)} · mem ${pct(u.memPct)} · psi cpu ${pct(u.cpuPsiPct)} mem ${pct(u.memPsiPct)} io ${pct(u.ioPsiPct)} · oom ${u.oomKills}${u.at ? " · " + agoS(u.at) : ""}`]);
  }
  if (l) rows.push(["log", l.errors > 0 || l.warns > 0 ? "fg-warn" : "fg-ok", `${l.errors} errors · ${l.warns} warns · ${l.total} lines${l.at ? " · " + agoS(l.at) : ""}`]);
  if (!rows.length) return "";
  return `<div class="grows">${rows.map(([lab, c, txt]) => `<div class="grow"><span class="gl ${c}">${lab}</span><span class="gt">${esc(txt)}</span></div>`).join("")}</div>`;
}
function appDetailHtml(row) {
  const { a, t } = row;
  const st = a.ready === "ready" ? { t: "yes", c: "fg-ok" } : a.ready === "not-ready" ? { t: "no", c: "fg-crit" } : { t: "unknown", c: "fg-faint" };
  let html = `<div class="tbar"><span class="t">app detail &#183; ${esc(a.name)}</span><span class="fill"></span><button type="button" class="close" data-act="key" data-k="Escape">[esc] close</button></div>`;
  html += `<span class="fg-mag">${esc(a.name)}</span><div class="kvs">`;
  html += kv("App:", a.name, "fg-strong b");
  html += kv("Namespace:", a.namespace, "fg-label");
  html += kv("Ready:", st.t, st.c, a.componentsReady ? a.componentsReady + " components" : "");
  if (a.version) html += kv("Version:", a.version, "fg-violet");
  if (a.tier) html += kv("Tier:", a.tier, "fg-text");
  if (a.weight) html += kv("Weight:", a.weight, "fg-text");
  if (a.domain) html += kv("Domain:", a.domain, "fg-text");
  if (a.group) html += kv("Group:", a.group, "fg-text");
  html += `</div>`;
  if (a.description) html += sub("Description") + `<p class="para">${esc(a.description)}</p>`;
  html += goldenSparks(a, t) + goldenRows(a, t, useOf(a), logOf(a));
  const comps = arr(a.components);
  if (comps.length) {
    html += sub("Components");
    html += comps.map((c) => `<div class="kv"><span class="fg-faint">&#8226;</span><span class="v fg-text" style="flex:1">${esc((c.kind || "") + "/" + (c.name || ""))}</span><span class="${/ready|running|healthy/i.test(c.status || "") && !/not/i.test(c.status || "") ? "fg-ok" : "fg-warn"}">${esc(c.status || DASH)}</span></div>`).join("");
  }
  const calls = arr(a.callsTo), calledBy = arr(a.calledBy);
  html += sub("Neighbors");
  html += `<span class="fg-text">Calls (${calls.length}) &#183; Called by (${calledBy.length})</span>`;
  const nb = (n, arrow, color, role) => `<button type="button" class="nb" data-act="jump" data-v="${esc(n)}"><span class="arrow" style="color:${color}">${arrow}</span><span class="c flex">${esc(n)}</span><span class="fg-faint">${esc(role)}</span></button>`;
  if (!calls.length && !calledBy.length) html += `<span class="note">no direct neighbors</span>`;
  html += calls.map((n) => nb(n, "&#8595;", "var(--faint)", "calls")).join("");
  html += calledBy.map((n) => nb(n, "&#8593;", "var(--violet)", "called by")).join("");
  const ext = externalOf(a);
  if (ext.length) {
    html += sub("External");
    html += ext.map((x) => `<div class="kv"><span class="fg-faint">&#8226;</span><span class="v fg-text" style="flex:1">${esc(x.protocol || "external")}</span><span class="fg-dim">${x.cps.toFixed(2)}/s</span></div>`).join("");
  }
  return html;
}
function appDetail(row) { return appDetailHtml(row); }
// External edges leaving this app's workloads.
function externalOf(a) {
  const key = appKey(a), g = S.graph;
  if (!g) return [];
  const mine = new Set(arr(g.nodes).filter((n) => (n.app ? (String(n.app).includes("/") ? n.app : n.namespace + "/" + n.app) : n.namespace + "/" + n.workload) === key).map((n) => n.namespace + "/" + n.workload));
  return arr(g.edges).filter((e) => (e.external || !e.dstWorkload) && mine.has(e.srcNamespace + "/" + e.srcWorkload)).map((e) => ({ protocol: e.protocol, cps: num(e.callsPerSec) }));
}

// ── panel 2: src ───────────────────────────────────────────────────────────
const statusDot = (s) => `<span class="rdot" style="background:${s === "up" ? "var(--ok)" : s === "stale" ? "var(--warn)" : "var(--crit)"}"></span>`;
function sourceLine(s) {
  const d = s.detail && typeof s.detail === "object" ? s.detail : {};
  const parts = [];
  const pick = (keys, label) => { for (const k of keys) if (typeof d[k] === "number") { parts.push(formatCount(d[k]) + " " + label); return true; } return false; };
  const a = pick(["metrics", "metricCount", "series"], "metrics");
  const b = pick(["datapoints", "dataPoints", "points"], "datapoints");
  if (!a && !b) Object.entries(d).filter(([, x]) => typeof x === "string" || typeof x === "number").slice(0, 2).forEach(([k, x]) => parts.push(k + " " + x));
  if (s.lastSeen) parts.push(agoS(s.lastSeen));
  return parts.join(" · ");
}
function srcPanel() {
  const srcs = srcRows(), evts = evtRows(), evtTotal = evtRowsAll().length;
  const leftFocus = S.focus2 === "src", rightFocus = !leftFocus;
  const si = selSrcIndex(srcs), ei = selEvtIndex(evts);
  const real = arr(S.sources && S.sources.sources);
  let lastKind = null;
  const srcList = srcs.map((s, i) => {
    const on = i === si;
    let head = "";
    if (!s.all && s.kind !== lastKind) { lastKind = s.kind; head = sub(s.kind || "other"); }
    if (s.all) {
      const n = (st) => real.filter((x) => x.status === st).length;
      const line = [["up", n("up")], ["stale", n("stale")], ["down", n("down")]].filter(([, c]) => c).map(([k, c]) => c + " " + k).join(" · ");
      return `<div class="tr${on ? " sel" : ""}${on && !leftFocus ? " blur" : ""}" data-act="sel" data-list="src" data-i="${i}"><span class="mark">${on ? "&#9654;" : ""}</span><span class="rdot ring"></span><span class="c flex">all sources <span class="fg-faint">${esc(line)}</span></span></div>`;
    }
    return head + `<div class="tr${on ? " sel" : ""}${on && !leftFocus ? " blur" : ""}" data-act="sel" data-list="src" data-i="${i}">
      <span class="mark">${on ? "&#9654;" : ""}</span>${statusDot(s.status)}
      <span class="c flex">${esc(s.name)}${s.node ? ` <span class="fg-label">${esc(s.node)}</span>` : ""} <span class="fg-faint">${esc(sourceLine(s))}</span></span>
    </div>`;
  }).join("");
  const evtList = evts.map((e, i) => {
    const on = i === ei;
    const who = [e.namespace, e.service].filter(Boolean).join("/");
    return `<div class="tr${on ? " sel" : ""}${on && !rightFocus ? " blur" : ""}" data-act="sel" data-list="evt" data-i="${i}">
      <span class="mark">${on ? "&#9654;" : ""}</span>
      ${cellW(36, "fg-dim", ago(e.ts))}
      ${cellW(40, sevClass(e.severity) + " b", sevBadge(e.severity))}
      ${cellW(64, "fg-violet", e.family || DASH)}
      ${cellW(120, "fg-label opt", who || DASH)}
      ${cellW(100, "fg-strong opt", e.reason || DASH)}
      <span class="c flex">${esc(oneLine(e.message))}</span>
    </div>`;
  }).join("");
  const sel = selSource();
  const left = S.evtDetail && evts[ei] && evKey(evts[ei]) === S.evtDetail ? evtDetail(evts[ei]) : `<div class="col g6">
      <div class="tbar${leftFocus ? "" : " off"}"><span class="t">sources(${real.length})</span></div>
      <div class="rows-fill"><div class="rows tall" data-scroll="src">${srcList}</div></div>
    </div>`;
  return `<div class="grid fill">${left}
    <div class="col g6">
      <div class="tbar${rightFocus ? "" : " off"}"><span class="t">events(${evts.length}${evtTotal > evts.length ? " of " + evtTotal : ""})${sel ? " &#183; " + esc(sel.name) : ""}</span></div>
      ${ledgerBlock(sel)}
      <div class="th"><span class="mark"></span>${cellW(36, "", "AGE")}${cellW(40, "", "SEV")}${cellW(64, "", "FAMILY")}${cellW(120, "opt", "NS/SERVICE")}${cellW(100, "opt", "REASON")}<span class="c flex">MESSAGE</span></div>
      <div class="rows-fill"><div class="rows tall" data-scroll="evt">${evtList || `<div class="empty">no events</div>`}</div></div>
    </div>
  </div>`;
}
// What the collectors did with the selected source's data, so an empty event
// list can be told from data that was read and held back or skipped.
const LEDGER_FIELDS = [["ticks", "reads"], ["no_snapshot", "no snapshot"], ["points", "points"], ["services", "services"], ["filtered", "out of scope"], ["unattributed", "no workload"], ["refused", "refused"]];
const ROLE_FIELDS = [["ticks", "windows"], ["services", "services read"], ["gated", "below the gate"], ["unattributed", "no workload"], ["capped", "cut by the cap"], ["sent", "sent to core"]];
function ledgerLine(c, fields) {
  return fields.map(([k, label]) => `${label} <span class="${k === "no_snapshot" || k === "refused" || k === "unattributed" ? (c[k] > 0 ? "fg-warn" : "fg-faint") : "fg-strong"}">${num(c[k])}</span>`).join(" &#183; ");
}
// Entries the agent kept out of a frame because the core would have refused
// the whole frame over them, named so the source that lacks an identity can be
// found. The same rows show under Info and under the sources.
function dropsBlock() {
  const d = arr(S.stats && S.stats.drops);
  if (!d.length) return "";
  const rows = d.map((x) => `<div class="para"><span class="fg-warn">${esc(x.subject)}</span> <span class="fg-label">${esc(x.frame)}</span> ${esc(x.reason)} <span class="fg-faint">&#215;${num(x.count)} &#183; ${esc(agoS(x.last))}</span></div>`).join("");
  return `${sub("Held back from the core")}<div class="col g6">${rows}</div>`;
}
function ledgerBlock(sel) {
  if (!sel) {
    // All sources: each role's totals, one line per role.
    const seen = new Map();
    arr(S.sources && S.sources.sources).forEach((x) => { if (x.roleLedger && !seen.has(x.kind)) seen.set(x.kind, x.roleLedger); });
    if (!seen.size) return "";
    return `<div class="col g6">${[...seen].map(([k, c]) => `<div class="para"><span class="fg-label">${esc(k)}</span> ${ledgerLine(c, ROLE_FIELDS)}</div>`).join("")}</div>${dropsBlock()}`;
  }
  if (!sel.ledger && !sel.roleLedger) return "";
  const rows = [];
  if (sel.ledger) rows.push(`<div class="para"><span class="fg-label">this source</span> ${ledgerLine(sel.ledger, LEDGER_FIELDS)}</div>`);
  if (sel.roleLedger) rows.push(`<div class="para"><span class="fg-label">all ${esc(sel.kind)} sources</span> ${ledgerLine(sel.roleLedger, ROLE_FIELDS)}</div>`);
  return `<div class="col g6">${rows.join("")}</div>`;
}
function evtDetail(e) {
  return `<div class="col g6">
    <div class="tbar"><span class="t">event detail &#183; ${esc(e.reason || "event")}</span><span class="fill"></span><button type="button" class="close" data-act="key" data-k="Escape">[esc] close</button></div>
    <p class="para fg-strong">${esc(oneLine(e.message) || e.reason)}</p>
    <div class="kvs">
      ${kv("ID:", e.id, "fg-violet")}
      ${kv("Time:", fullTime(e.ts), "fg-strong", agoS(e.ts))}
      ${kv("Family:", e.family, "fg-violet")}
      ${kv("Severity:", sevBadge(e.severity), sevClass(e.severity))}
      ${kv("Reason:", e.reason, "fg-strong")}
      ${e.tenant ? kv("Tenant:", e.tenant, "fg-text") : ""}
      ${e.namespace ? kv("Namespace:", e.namespace, "fg-label") : ""}
      ${e.service ? kv("Service:", e.service, "fg-strong") : ""}
      ${Object.entries(e).filter(([k]) => !["id", "ts", "family", "severity", "reason", "message", "tenant", "namespace", "service"].includes(k)).map(([k, x]) => kv(k + ":", typeof x === "object" ? JSON.stringify(x) : x, "fg-text")).join("")}
    </div>
    ${sub("Message")}<p class="para">${esc(e.message || "(no message)")}</p>
  </div>`;
}

// ── agent info view (?) ────────────────────────────────────────────────────
async function refreshInfo() {
  const [st, cf] = await Promise.all([getJSON("./api/v1/stats").catch(() => null), getJSON("./api/v1/config").catch(() => null)]);
  if (st) S.stats = st;
  if (cf) S.config = cf;
  if (S.info) render();
}
function toggleInfo() {
  S.info = !S.info;
  S.cfgSel = 0;
  if (S.filterEditing) closePrompt(true); else if (S.filterText) { S.filterText = ""; $("filter").value = ""; }
  render();
  if (S.info) refreshInfo();
}
function humanDur(sec) {
  if (typeof sec !== "number" || !isFinite(sec) || sec < 0) return DASH;
  const s = Math.trunc(sec);
  if (s < 60) return s + "s";
  const m = Math.trunc(s / 60);
  if (m < 60) return m + "m " + (s % 60) + "s";
  const h = Math.trunc(m / 60);
  if (h < 24) return h + "h " + (m % 60) + "m";
  return Math.trunc(h / 24) + "d " + (h % 24) + "h";
}
const mib = (b) => (typeof b === "number" ? (b / 1048576).toFixed(1) + " MiB" : DASH);
const byCount = (o) => Object.entries(o && typeof o === "object" ? o : {}).sort((p, q) => q[1] - p[1] || p[0].localeCompare(q[0]));
const flagMatch = (f) => matches([f.name, f.value].join(" "));
function ctlRows() {
  const fl = arr(S.config && S.config.flags).filter(flagMatch);
  return [...fl.filter((f) => f.changed), ...fl.filter((f) => !f.changed)];
}
// The sensor groups with their flags cut by the filter: only changed flags
// are rows (and selectable); unchanged ones are counted.
function sensorGroups() {
  return arr(S.config && S.config.sensors).map((g) => {
    const fl = arr(g.flags).filter(flagMatch);
    return { g, changed: fl.filter((f) => f.changed), unchanged: fl.filter((f) => !f.changed).length };
  });
}
// Every selectable row, in order: the controller's flags, then each group's changed flags.
function cfgRows() {
  return [...ctlRows(), ...sensorGroups().flatMap((x) => x.changed)];
}
function infoPanel() {
  const st = S.stats || {}, pr = st.process || {}, rp = st.replica || {}, pu = st.push || {}, vw = st.view || {}, se = st.sensors || {};
  const peers = arr(rp.peers);
  const link = pu.standalone ? { t: "standalone (no core)", c: "fg-dim" } : pu.connected ? { t: "connected", c: "fg-ok" } : { t: pu.everConnected ? "offline" : "never connected", c: "fg-crit" };
  const sec = (rows) => `<div class="kvs">${rows.join("")}</div>`;
  const stats = `<div class="col g6">
    <div class="tbar"><span class="t">agent</span></div>
    ${sub("Process")}${sec([kv("Uptime:", humanDur(pr.uptimeSec), "fg-strong"), kv("Goroutines:", pr.goroutines, "fg-strong"), kv("Heap:", mib(pr.heapBytes), "fg-strong"), kv("Sys:", mib(pr.sysBytes), "fg-strong"), kv("GC cycles:", pr.gcCycles, "fg-strong"), kv("CPUs:", pr.cpus, "fg-strong"), kv("Go:", pr.go, "fg-violet"), kv("Agent:", pr.version, "fg-violet"), kv("Mode:", pr.mode, "fg-label")])}
    ${sub("Replica")}${sec([kv("Role:", rp.role, "fg-strong"), kv("Peers:", String(peers.length), "fg-strong", peers.join(", "))])}
    ${sub("Core link")}<div class="kvs"><div class="kv w126"><span class="k">State:</span><span class="v ${link.c}"><span class="dot"></span> ${esc(link.t)}</span></div>${[kv("Host:", pu.host, "fg-text", "", "w126"), kv("Tenant:", pu.tenant, "fg-text", "", "w126"), kv("Dropped:", pu.dropped, num(pu.dropped) ? "fg-warn" : "fg-strong", "", "w126"), kv("No tenant:", pu.noTenantDropped, num(pu.noTenantDropped) ? "fg-warn" : "fg-strong", "", "w126"), kv("Malformed:", pu.malformedDropped, num(pu.malformedDropped) ? "fg-warn" : "fg-strong", "", "w126"), ...byCount(pu.sent).map(([k, n]) => kv("sent " + k + ":", formatCount(n), "fg-text", "", "w126"))].join("")}</div>
    ${dropsBlock()}
    ${sub("View")}${sec([kv("Apps:", vw.apps, "fg-strong"), kv("Services:", vw.services, "fg-strong"), kv("Workloads:", vw.workloads, "fg-strong"), kv("Edges:", vw.edges, "fg-strong"), kv("Events kept:", vw.eventsKept, "fg-strong"), ...byCount(vw.events).map(([k, n]) => kv(k + ":", formatCount(n), "fg-text")), kv("Tenants:", arr(vw.tenants).length ? arr(vw.tenants).join(", ") : DASH, "fg-label")])}
  </div>`;
  const ctl = ctlRows(), groups = sensorGroups();
  const total = ctl.length + groups.reduce((x, y) => x + y.changed.length, 0);
  if (S.cfgSel >= total) S.cfgSel = Math.max(0, total - 1);
  let idx = 0;
  const row = (f) => {
    const i = idx++, on = i === S.cfgSel;
    const val = f.value === "" || f.value == null ? DASH : f.value;
    return `<div class="tr${on ? " sel" : ""}" data-act="sel" data-list="cfg" data-i="${i}">
      <span class="mark">${on ? "&#9654;" : ""}</span>
      <span class="c ${f.changed ? "fg-label" : "fg-dim"}" style="width:${em(250)}">${esc(f.name)}</span>
      <span class="c flex ${f.changed ? "fg-accent" : "fg-dim"}">${esc(val)}${f.changed ? ` <span class="fg-faint">default ${esc(f.default === "" || f.default == null ? DASH : f.default)}</span>` : ""}</span>
    </div>${on && f.usage ? `<div class="tr" style="padding-left:2.5em;cursor:default"><span class="c flex fg-dim" style="white-space:normal">${esc(f.usage)}</span></div>` : ""}`;
  };
  const nCh = ctl.filter((f) => f.changed).length;
  let list = "";
  ctl.forEach((f, i) => {
    if (i === 0 || (i === nCh && nCh > 0)) {
      const isCh = i < nCh;
      list += sub((isCh ? "changed " : "defaults ") + (isCh ? nCh : ctl.length - nCh));
    }
    list += row(f);
  });
  // One sensors section: the stats first, then each group's roles, nodes and settings.
  let sens = sub("Sensors") + sec([kv("Nodes:", se.nodes, "fg-strong"), kv("Sensors:", se.sensors, "fg-strong"), kv("OTLP sources:", se.otlpSources, "fg-strong"), kv("Last report:", se.at ? agoS(se.at) : DASH, "fg-strong")]);
  groups.forEach(({ g, changed, unchanged }) => {
    const nodes = arr(g.nodes), n = nodes.length;
    const names = nodes.length > 3 ? nodes.slice(0, 3).join(", ") + " +" + (nodes.length - 3) : nodes.join(", ");
    const conn = num(g.connected);
    sens += `<div class="tr" style="cursor:default"><span class="mark"></span><span class="c flex fg-label">${esc(arr(g.roles).join(", "))} · ${n} ${n === 1 ? "node" : "nodes"}${g.version ? " · " + esc(g.version) : ""}</span></div>`;
    sens += `<div class="tr" style="cursor:default"><span class="mark"></span><span class="c flex fg-dim">${esc(names)}</span><span class="c ${conn < n ? "fg-warn" : "fg-dim"}">connected ${conn}/${n}</span></div>`;
    if (!arr(g.flags).length) sens += `<div class="tr" style="cursor:default"><span class="mark"></span><span class="c flex fg-dim">no settings sent (older sensor)</span></div>`;
    else {
      sens += changed.map(row).join("");
      const un = arr(g.flags).filter((f) => !f.changed).length;
      if (un) sens += `<div class="tr" style="cursor:default"><span class="mark"></span><span class="c flex fg-dim">defaults: ${S.filterText ? unchanged : un} flags</span></div>`;
    }
  });
  const cfg = `<div class="col g6">
    <div class="tbar"><span class="t">controller configuration(${(S.config ? arr(S.config.flags).length : 0)})</span></div>
    <div class="rows-fill"><div class="rows tall" data-scroll="cfg">${list || `<div class="empty">no flags</div>`}${sens}</div></div>
  </div>`;
  return `<div class="grid fill">${stats}${cfg}</div>`;
}

// ── actions ─────────────────────────────────────────────────────────────────
function setPanel(p) {
  if (!PANELS.includes(p)) return;
  if (S.panel !== p) {
    S.panel = p;
    store.set(KEY_PANEL, p);
    S.appDetail = false; S.evtDetail = null; S.info = false;
    if (S.filterEditing) closePrompt(true);
  }
  render();
}
function move(delta) {
  if (S.info) {
    const n = cfgRows().length;
    if (n) { S.cfgSel = Math.max(0, Math.min(n - 1, S.cfgSel + delta)); S.scrollSel = true; render(); }
    return;
  }
  if (S.panel === "app") {
    const n = appRows().length;
    if (!n) return;
    S.selApp = S.selApp < 0 ? 0 : Math.max(0, Math.min(n - 1, S.selApp + delta));
  } else if (S.focus2 === "src") {
    const rows = srcRows(), i = Math.max(0, Math.min(rows.length - 1, selSrcIndex(rows) + delta));
    if (rows[i].id !== S.selSrc) { S.selSrc = rows[i].id; S.selEvt = null; S.evtDetail = null; }
  } else {
    const rows = evtRows();
    if (!rows.length) return;
    const e = rows[Math.max(0, Math.min(rows.length - 1, selEvtIndex(rows) + delta))];
    S.selEvt = evKey(e);
    if (S.evtDetail) S.evtDetail = S.selEvt;
  }
  S.scrollSel = true;
  render();
}
function openRow(list, i) {
  if (list === "app") { S.selApp = i; S.appDetail = true; }
  else if (list === "evt") { const e = evtRows()[i]; if (e) { S.focus2 = "evt"; S.selEvt = evKey(e); S.evtDetail = S.selEvt; } }
  else if (list === "src") { selectSrc(i); }
  render();
}
function selectSrc(i) {
  const s = srcRows()[i];
  if (!s) return;
  S.focus2 = "src";
  if (s.id !== S.selSrc) { S.selSrc = s.id; S.selEvt = null; }
  S.evtDetail = null;
}
function onEscape() {
  if (S.filterEditing) { closePrompt(true); return; }
  if (S.info) { toggleInfo(); return; }
  if (S.panel === "app" && S.appDetail) { S.appDetail = false; render(); return; }
  if (S.panel === "src" && S.evtDetail) { S.evtDetail = null; render(); return; }
}
function resetSel() {
  if (S.info) { S.cfgSel = 0; return; }
  if (S.panel === "app") { S.selApp = -1; S.appDetail = false; }
  else if (S.focus2 === "src") { S.selSrc = ""; S.selEvt = null; S.evtDetail = null; }
  else { S.selEvt = null; S.evtDetail = null; }
}
// Switch tenant: drop what is held, reopen the stream, refetch at once.
function applyTenant(t, persist) {
  S.tenant = t;
  if (persist) { if (t) store.set(KEY_TENANT, t); else store.del(KEY_TENANT); }
  S.apps = null; S.graph = null; S.sources = null; S.services = new Map();
  S.events = []; S.eventsLoaded = false;
  S.selApp = -1; S.appDetail = false; S.selSrc = ""; S.selEvt = null; S.evtDetail = null;
  esRetry = 0;
  connectStream();
  render();
  refresh();
}

// ── tenant picker (T / N), as in the core TUI ──────────────────────────────
async function loadTenants() {
  try { S.tenantRows = arr((await getJSON("./api/v1/tenants")).tenants); }
  catch (e) { if (!S.tenantRows.length) S.tenantRows = tenantList().map((name) => ({ name })); }
  if (S.tntOpen) render();
}
function openTenants() {
  S.tntOpen = true;
  const rows = tntRows();
  S.selTnt = Math.max(0, rows.findIndex((t) => t.name === S.tenant));
  S.scrollSel = true;
  render();
  loadTenants();
}
function closeTenants() { S.tntOpen = false; render(); }
function tntRows() {
  return S.tenantRows.length ? S.tenantRows : tenantList().map((name) => ({ name }));
}
// ↑↓ move, enter scopes, esc / T / N closes; any other key closes the
// picker and goes on to its normal binding.
function tenantsHandleKey(k) {
  const n = tntRows().length;
  const mv = (d) => { if (n) { S.selTnt = Math.max(0, Math.min(n - 1, S.selTnt + d)); S.scrollSel = true; render(); } };
  switch (k) {
    case "j": case "ArrowDown": mv(1); return true;
    case "k": case "ArrowUp": mv(-1); return true;
    case "PageDown": mv(10); return true;
    case "PageUp": mv(-10); return true;
    case "Enter": scopeTenant(S.selTnt); return true;
    case "Escape": case "T": case "N": closeTenants(); return true;
    default: S.tntOpen = false; render(); return false;
  }
}
function scopeTenant(i) {
  const t = tntRows()[i];
  S.selTnt = i;
  S.tntOpen = false;
  if (t && t.name !== S.tenant) applyTenant(t.name, true); else render();
}
const tpNarrow = () => window.matchMedia("(max-width: 899px)").matches;
function renderModal() {
  const el = $("modal");
  if (!S.tntOpen) { if (!el.hidden) { el.hidden = true; el.innerHTML = ""; } return; }
  const saved = {};
  el.querySelectorAll("[data-scroll]").forEach((n) => { saved[n.dataset.scroll] = n.scrollTop; });
  const rows = tntRows();
  if (S.selTnt >= rows.length) S.selTnt = Math.max(0, rows.length - 1);
  const f = (x) => (x == null ? DASH : formatCount(x));
  const cells = rows.map((t) => ({ t, apps: f(t.apps), svc: f(t.services), evt: f(t.events) }));
  const w = (h, k, gap) => Math.max(h.length, ...cells.map((c) => String(k(c)).length)) + gap + "ch";
  const W = { name: w("TENANT", (c) => c.t.name, 0), apps: w("APPS", (c) => c.apps, 2), svc: w("SERVICES", (c) => c.svc, 2), evt: w("EVENTS", (c) => c.evt, 2) };
  const cell = (wd, cls, text) => `<span class="c ${cls}${/\br\b/.test(cls) ? " opt" : ""}" style="width:${wd}">${esc(text)}</span>`;
  const list = cells.map((c, i) => {
    const on = i === S.selTnt, cur = c.t.name === S.tenant;
    return `<div class="tr${on ? " sel" : ""}" data-act="sel" data-list="tnt" data-i="${i}">
      <span class="mark fg-mag">${on ? "&gt;" : ""}</span>
      ${cell(W.name, cur ? "b fg-label" : "", c.t.name)}
      ${cell(W.apps, "r fg-strong", c.apps)}
      ${cell(W.svc, "r fg-strong", c.svc)}
      ${cell(W.evt, "r fg-strong", c.evt)}
    </div>`;
  }).join("");
  const sel = cells[S.selTnt];
  const pane = sel ? `<div class="tp-pane"><div class="tp-in">${sub("Tenant: " + sel.t.name)}<div class="kvs">${kv("Apps:", sel.apps, "fg-strong", "", "w126")}${kv("Services:", sel.svc, "fg-strong", "", "w126")}${kv("Events:", sel.evt, "fg-strong", "", "w126")}</div><span class="fg-faint tp-hint">&#8593;&#8595; move &#183; enter scope &#183; esc close</span></div></div>` : "";
  el.innerHTML = `<div class="modal-back" data-act="tnt-close"></div>
    <div class="tp" role="dialog" aria-modal="true" aria-label="tenant picker">
      <div class="modal tp-box">
        <div class="tbar"><span class="t">tenants(${rows.length})${S.tenant ? " &#183; scoped to " + esc(S.tenant) : ""}</span></div>
        <div class="th"><span class="mark"></span>${cell(W.name, "", "TENANT")}${cell(W.apps, "r", "APPS")}${cell(W.svc, "r", "SERVICES")}${cell(W.evt, "r", "EVENTS")}</div>
        <div class="rows" data-scroll="tnt">${list || `<div class="empty">no tenants</div>`}</div>
      </div>
      ${pane}
    </div>`;
  el.hidden = false;
  el.querySelectorAll("[data-scroll]").forEach((n) => { if (saved[n.dataset.scroll] != null) n.scrollTop = saved[n.dataset.scroll]; });
  if (S.scrollSel) { S.scrollSel = false; revealSel(el); }
}
function cycleReady() {
  S.readyFilter = READY_CYCLE[(READY_CYCLE.indexOf(S.readyFilter) + 1) % READY_CYCLE.length];
  S.selApp = -1; S.appDetail = false;
  flash("state: " + (S.readyFilter || "all"));
  render();
}
function cycleTm(key, list) {
  const ids = list.map((x) => x[0]);
  S[key] = ids[(ids.indexOf(S[key]) + 1) % ids.length];
  render();
}
function jumpTo(key) {
  const rows = appRows();
  let i = rows.findIndex((r) => r.key === key);
  if (i < 0) i = rows.findIndex((r) => r.a.name === key);
  if (i < 0) { flash(key + " is not in the list"); return; }
  S.selApp = i; S.appDetail = true; S.scrollSel = true; render();
}
function openPrompt() {
  S.filterEditing = true;
  renderCrumb();
  const inp = $("filter");
  inp.value = S.filterText;
  inp.placeholder = "filter " + (S.info ? "configuration" : S.panel === "app" ? "apps" : S.focus2 === "src" ? "sources" : "events");
  inp.focus();
}
function closePrompt(clear) {
  S.filterEditing = false;
  const inp = $("filter");
  if (clear) { S.filterText = ""; inp.value = ""; resetSel(); }
  inp.blur();
  render();
}
function setFocus(next) {
  if (next === S.focus2) return;
  S.focus2 = next;
  S.evtDetail = null;
  if (S.filterText) { S.filterText = ""; $("filter").value = ""; }
  render();
}

// Single dispatcher for hardware keys, the on-screen key bar and the legend.
function handleKey(k) {
  if (S.locked) return false;
  if (S.logoutConfirm) {
    if (k === "y" || k === "Y") logout(); else { S.logoutConfirm = false; flash("sign-out cancelled"); render(); }
    return true;
  }
  if (S.tntOpen && tenantsHandleKey(k)) return true;
  if (S.info && ["Enter", "Tab", "ArrowLeft", "ArrowRight", "s", "m", "g", "z"].includes(k)) return true;
  switch (k) {
    case "1": setPanel("app"); break;
    case "2": setPanel("src"); break;
    case "q": if (S.auth.required) { S.logoutConfirm = true; render(); } else return false; break;
    case "?": toggleInfo(); break;
    case "/": openPrompt(); break;
    case "D": {
      const dim = store.get(THEME_KEY) !== "dim";
      if (dim) store.set(THEME_KEY, "dim"); else store.del(THEME_KEY);
      applyTheme(dim); render(); break;
    }
    case "Escape": onEscape(); break;
    case "j": case "ArrowDown": move(1); break;
    case "k": case "ArrowUp": move(-1); break;
    case "PageDown": move(10); break;
    case "PageUp": move(-10); break;
    case "Enter": {
      if (S.panel === "app") { if (!appRows().length) break; openRow("app", Math.max(0, S.selApp)); }
      else if (S.focus2 === "evt") { const rows = evtRows(); if (rows.length) openRow("evt", selEvtIndex(rows)); }
      break;
    }
    case "Tab": case "ArrowLeft": case "ArrowRight":
      if (S.panel === "src") setFocus(k === "ArrowLeft" ? "src" : k === "ArrowRight" ? "evt" : S.focus2 === "src" ? "evt" : "src");
      break;
    case "T": case "N": openTenants(); break;
    case "s": if (S.panel === "app") cycleReady(); else return false; break;
    case "m": if (S.panel === "app") { S.appView = S.appView === "treemap" ? "graph" : "treemap"; render(); } else return false; break;
    case "g": if (S.panel === "app" && S.appView === "treemap") cycleTm("tmGroup", GROUPS); else return false; break;
    case "z": if (S.panel === "app" && S.appView === "treemap") cycleTm("tmSize", SIZES); else return false; break;
    default: return false;
  }
  return true;
}

// ── events ──────────────────────────────────────────────────────────────────
let lastTap = { list: null, i: -1, t: 0 };
function onClick(ev) {
  if (S.locked) return;
  const t = ev.target.closest("[data-act]");
  if (!t) return;
  const act = t.dataset.act;
  if (act === "key") { ev.preventDefault(); handleKey(t.dataset.k); return; }
  if (act === "logout-yes") { logout(); return; }
  if (act === "logout-no") { S.logoutConfirm = false; render(); return; }
  if (act === "tnt-close") { closeTenants(); return; }
  if (act === "sel" && t.dataset.list === "tnt") {
    const i = Number(t.dataset.i), now = Date.now();
    const dbl = lastTap.list === "tnt" && lastTap.i === i && now - lastTap.t < 450;
    lastTap = { list: "tnt", i, t: dbl ? 0 : now };
    if (dbl || tpNarrow()) { scopeTenant(i); return; }
    S.selTnt = i; render(); return;
  }
  if (act === "sel" && t.dataset.list === "cfg") { S.cfgSel = Number(t.dataset.i); render(); return; }
  if (act === "sel") {
    const list = t.dataset.list, i = Number(t.dataset.i), now = Date.now();
    const dbl = lastTap.list === list && lastTap.i === i && now - lastTap.t < 450;
    lastTap = { list, i, t: dbl ? 0 : now };
    if (dbl) { openRow(list, i); return; }
    if (list === "app") { S.selApp = i; if (S.appDetail) S.appDetail = true; }
    else if (list === "src") selectSrc(i);
    else if (list === "evt") {
      S.focus2 = "evt";
      const e = evtRows()[i];
      if (e) { S.selEvt = evKey(e); if (S.evtDetail) S.evtDetail = S.selEvt; }
    }
    render();
    return;
  }
  if (act === "readyf") { S.readyFilter = t.dataset.v; S.selApp = -1; S.appDetail = false; render(); return; }
  if (act === "jump") { jumpTo(t.dataset.v); return; }
  if (act === "sigwin") { S.sigWin = t.dataset.v; render(); return; }
  if (act === "appview") { S.appView = t.dataset.v; render(); return; }
  if (act === "tmgroup") { S.tmGroup = t.dataset.v; render(); return; }
  if (act === "tmsize") { S.tmSize = t.dataset.v; render(); return; }
  if (act === "clear") {
    const w = t.dataset.v;
    if (w === "text") { S.filterText = ""; $("filter").value = ""; }
    if (w === "ready") S.readyFilter = "";
    resetSel(); render(); return;
  }
  if (act === "shift") { setShift(!S.shiftHeld); return; }
  if (act === "keys") {
    S.keysShown = !S.keysShown;
    if (S.keysShown) { store.set(KEYS_KEY, "shown"); $("kbd").focus(); } else { store.del(KEYS_KEY); $("kbd").blur(); }
    renderLegend();
  }
}

const NAMED = new Set(["Escape", "Enter", "Tab", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "PageUp", "PageDown"]);
function setShift(on) {
  if (S.shiftHeld === on) return;
  S.shiftHeld = on;
  const b = document.querySelector('[data-act="shift"]');
  if (b) b.classList.toggle("on", on);
  renderLegend();
}
function onKeyDown(ev) {
  if (S.locked) return;
  const target = ev.target;
  if (ev.key === "Shift") { setShift(true); return; }
  if (ev.ctrlKey || ev.metaKey || ev.altKey) return;
  if (target && target.id === "filter") {
    if (ev.key === "Escape") { ev.preventDefault(); closePrompt(true); }
    else if (ev.key === "Enter") { ev.preventDefault(); closePrompt(false); }
    else if (ev.key === "ArrowDown" || ev.key === "ArrowUp") { ev.preventDefault(); move(ev.key === "ArrowDown" ? 1 : -1); }
    return;
  }
  if (target && target !== $("kbd") && /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName)) return;
  const k = ev.key;
  if (k.length !== 1 && !NAMED.has(k)) return;
  if (handleKey(k) !== false) ev.preventDefault();
}
function onFilterInput(ev) {
  S.filterText = ev.target.value;
  resetSel();
  renderPanel();
  renderLegend();
}
// The soft keyboard on iPad often reports key "Unidentified"; take the typed
// character from the input event instead.
function onKbdInput(ev) {
  const el = ev.target;
  const ch = (ev.data || el.value || "").slice(-1);
  el.value = "";
  if (ch) handleKey(ch);
}

// ── login (the agent may require an admin key) ──────────────────────────────
async function probeAuth() {
  try {
    const r = await fetch("./api/v1/auth", { cache: "no-store", headers: { Accept: "application/json" } });
    return r.ok ? await r.json() : null;
  } catch (e) { return null; }
}
const SPLASH_ART = [
  "████████╗██████╗ ██╗ █████╗  ██████╗ ███████╗",
  "╚══██╔══╝██╔══██╗██║██╔══██╗██╔════╝ ██╔════╝",
  "   ██║   ██████╔╝██║███████║██║  ███╗█████╗  ",
  "   ██║   ██╔══██╗██║██╔══██║██║   ██║██╔══╝  ",
  "   ██║   ██║  ██║██║██║  ██║╚██████╔╝███████╗",
  "   ╚═╝   ╚═╝  ╚═╝╚═╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝",
].join("\n");
function logoutBox() {
  return `<div class="box confirm" style="margin-bottom:12px">
    <div class="fg-strong b">sign out?</div>
    <div class="btnrow"><button type="button" class="btn primary" data-act="logout-yes">y — sign out</button><button type="button" class="btn" data-act="logout-no">n — cancel</button></div>
  </div>`;
}
// Drop everything the session held and show the login screen.
function toLogin() {
  if (S.locked) return;
  S.locked = true;
  S.auth = { required: true, authenticated: false };
  clearTimeout(esTimer);
  if (es) { es.close(); es = null; }
  S.status = null; S.apps = null; S.graph = null; S.sources = null; S.services = new Map();
  S.events = []; S.eventsLoaded = false; S.stats = null; S.config = null; S.tenantRows = [];
  S.info = false; S.tntOpen = false; S.logoutConfirm = false; S.evtDetail = null; S.appDetail = false;
  S.login = { err: "", until: 0, busy: false };
  $("app").hidden = true;
  const sp = $("splash");
  sp.hidden = false;
  sp.innerHTML = `<div class="splash">
    <pre class="splash-art">${SPLASH_ART}</pre>
    <form id="loginform" class="btnrow" autocomplete="off">
      <input id="loginkey" class="logininput" type="password" autocomplete="current-password" placeholder="admin key" aria-label="admin key">
      <button id="loginbtn" type="submit" class="btn primary"><i class="ph-fill ph-sign-in"></i>sign in</button>
    </form>
    <div id="loginmsg" class="fg-crit"></div>
  </div>`;
  $("loginkey").focus();
}
let loginTimer = 0;
function loginState() {
  const m = $("loginmsg"), b = $("loginbtn");
  if (!m || !b) return;
  const wait = Math.ceil((S.login.until - Date.now()) / 1000);
  if (wait > 0) { m.textContent = "too many attempts, retry in " + wait + "s"; b.disabled = true; }
  else { m.textContent = S.login.err; b.disabled = S.login.busy; clearInterval(loginTimer); }
}
async function submitLogin(ev) {
  ev.preventDefault();
  const inp = $("loginkey");
  if (S.login.busy || Date.now() < S.login.until || !inp || !inp.value) return;
  const key = inp.value;
  inp.value = "";
  S.login.busy = true; S.login.err = ""; loginState();
  try {
    const r = await fetch("./api/v1/login", { method: "POST", headers: { "Content-Type": "application/json" }, credentials: "same-origin", body: JSON.stringify({ key }) });
    S.login.busy = false;
    if (r.ok) { startAfterLogin(); return; }
    if (r.status === 401) S.login.err = "wrong key";
    else if (r.status === 429) {
      const n = parseInt(r.headers.get("Retry-After"), 10);
      S.login.until = Date.now() + (isFinite(n) && n > 0 ? n : 1) * 1000;
      clearInterval(loginTimer); loginTimer = setInterval(loginState, 1000);
    } else S.login.err = "login unavailable";
  } catch (e) { S.login.busy = false; S.login.err = "login unavailable"; }
  loginState();
  if ($("loginkey")) $("loginkey").focus();
}
function startAfterLogin() {
  S.locked = false;
  S.auth = { required: true, authenticated: true };
  $("splash").hidden = true; $("splash").innerHTML = "";
  $("app").hidden = false;
  render();
  refresh();
  connectStream();
}
async function logout() {
  S.logoutConfirm = false;
  try { await fetch("./api/v1/logout", { method: "POST", credentials: "same-origin" }); } catch (e) { /* the session is dropped here regardless */ }
  toLogin();
}

async function boot() {
  document.addEventListener("click", onClick);
  document.addEventListener("keydown", onKeyDown);
  document.addEventListener("submit", (ev) => { if (ev.target.id === "loginform") submitLogin(ev); });
  document.addEventListener("keyup", (ev) => { if (ev.key === "Shift") setShift(false); });
  window.addEventListener("blur", () => setShift(false));
  $("filter").addEventListener("input", onFilterInput);
  $("kbd").addEventListener("input", onKbdInput);
  setInterval(() => { if (document.hidden || S.locked) return; refresh(); if (S.info) refreshInfo(); }, POLL_MS);
  document.addEventListener("visibilitychange", () => { if (!document.hidden && !S.locked) refresh(); });
  const a = await probeAuth();
  if (a) S.auth = { required: !!a.required, authenticated: !!a.authenticated };
  if (S.auth.required && !S.auth.authenticated) { toLogin(); return; }
  render();
  refresh();
  connectStream();
}

boot();

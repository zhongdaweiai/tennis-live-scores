#!/usr/bin/env python3
"""Tennis live scores — Matchstat WebSocket -> web page.

Backend keeps one socket.io connection to the Matchstat live feed
(join-live-events-all, sportId=5) and refreshes upcoming fixtures over the
REST API every 15 minutes. The page lists LIVE matches with real-time
set/game/point scores plus upcoming matches for the next 48 hours, updated in
the browser through Server-Sent Events.

Configuration (environment variables):
  RAPIDAPI_KEY          required; used to mint short-lived ws tokens and to
                        fetch fixtures. Never logged, never sent to browsers.
  RAPIDAPI_HOST         default tennis-api-atp-wta-itf.p.rapidapi.com
  MATCHSTAT_SOCKET_URL  default https://live.matchstat.com
"""

from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
from typing import Any

import requests
import socketio
import uvicorn

import covert_addon
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

RAPIDAPI_HOST = os.environ.get(
    "RAPIDAPI_HOST", "tennis-api-atp-wta-itf.p.rapidapi.com"
)
SOCKET_URL = os.environ.get("MATCHSTAT_SOCKET_URL", "https://live.matchstat.com")
FIXTURE_REFRESH_SECONDS = 900
FIXTURE_HORIZON_HOURS = 48
SOCKET_RETRY_SECONDS = 8

STATE_LOCK = threading.Lock()
STATE: dict[str, Any] = {
    "live": {},
    "upcoming": [],
    "socket_status": "starting",
    "socket_connected_utc": None,
    "live_updated_utc": None,
    "fixtures_updated_utc": None,
    "errors": {},
}
VERSION = {"n": 0}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def bump(mutator) -> None:
    with STATE_LOCK:
        mutator(STATE)
        VERSION["n"] += 1


def api_key() -> str:
    key = os.environ.get("RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("RAPIDAPI_KEY is not configured")
    return key


def rapidapi_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(
        f"https://{RAPIDAPI_HOST}{path}",
        params=params or {},
        headers={
            "accept": "application/json",
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key": api_key(),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def parse_live_item(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict) or not item.get("id"):
        return None
    score = str(item.get("score") or "")
    sets = [part for part in score.split(",") if part]
    indicator = str(item.get("indicator") or "")
    flags = indicator.split(",")
    serving = 0
    if len(flags) >= 2:
        if flags[0].strip() == "1":
            serving = 1
        elif flags[1].strip() == "1":
            serving = 2
    return {
        "id": str(item["id"]),
        "status": item.get("status"),
        "tour": str(item.get("tourType") or "").upper(),
        "league": item.get("league"),
        "player1": item.get("participant1"),
        "player2": item.get("participant2"),
        "sets": sets,
        "points": item.get("points"),
        "serving": serving,
        "raw_score": score,
    }


def socket_loop() -> None:
    """Keep one live-feed connection alive forever; fresh token per attempt."""
    while True:
        try:
            token_payload = rapidapi_get("/tennis/v2/extend/api/ws-token")
            token = token_payload.get("token")
            if not token:
                raise RuntimeError(f"ws-token unavailable: {token_payload}")

            client = socketio.Client(
                reconnection=False, logger=False, engineio_logger=False
            )

            @client.event
            def connect() -> None:
                client.emit("join-live-events-all", {"sportId": 5})
                client.emit("join-live-events-all", "tennis")
                bump(
                    lambda state: state.update(
                        socket_status="connected",
                        socket_connected_utc=utc_now_iso(),
                    )
                )

            @client.on("live-events-all-update")
            def on_all_update(data: Any = None) -> None:
                if not isinstance(data, dict):
                    return
                results = data.get("results")
                if not isinstance(results, list):
                    return
                live: dict[str, Any] = {}
                for item in results:
                    parsed = parse_live_item(item)
                    if parsed:
                        live[parsed["id"]] = parsed
                covert_addon.feed(results, utc_now_iso())
                bump(
                    lambda state: state.update(
                        live=live, live_updated_utc=utc_now_iso()
                    )
                )

            client.connect(
                SOCKET_URL,
                auth={"token": str(token)},
                transports=["websocket"],
                wait_timeout=20,
            )
            client.wait()  # blocks until disconnect
            bump(lambda state: state.update(socket_status="disconnected"))
        except Exception as error:  # noqa: BLE001 — keep the loop alive
            message = f"{type(error).__name__}: {error}"[:300]
            bump(
                lambda state: state.update(
                    socket_status="retrying",
                    errors={**state["errors"], "socket": message},
                )
            )
        time.sleep(SOCKET_RETRY_SECONDS)


def fetch_fixtures() -> list[dict[str, Any]]:
    today = dt.datetime.now(dt.timezone.utc).date()
    start = today.isoformat()
    end = (today + dt.timedelta(days=2)).isoformat()
    horizon = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
        hours=FIXTURE_HORIZON_HOURS
    )
    rows: list[dict[str, Any]] = []
    for tour in ("atp", "wta"):
        page_no = 1
        while page_no <= 10:
            payload = rapidapi_get(
                f"/tennis/v2/{tour}/fixtures/{start}/{end}",
                params={
                    "include": "round,tournament.court,tournament.rank",
                    "pageSize": 500,
                    "pageNo": page_no,
                },
            )
            batch = payload.get("data")
            if not isinstance(batch, list):
                break
            for row in batch:
                if not isinstance(row, dict):
                    continue
                player1 = row.get("player1") or {}
                player2 = row.get("player2") or {}
                tournament = row.get("tournament") or {}
                round_row = row.get("round") or {}
                court = (tournament.get("court") or {})
                name1 = player1.get("name")
                name2 = player2.get("name")
                scheduled = row.get("date")
                if not (name1 and name2 and scheduled):
                    continue
                when = dt.datetime.fromisoformat(
                    str(scheduled).replace("Z", "+00:00")
                )
                if when > horizon:
                    continue
                rows.append(
                    {
                        "id": str(row.get("id")),
                        "tour": tour.upper(),
                        "tournament": tournament.get("name"),
                        "surface": court.get("name"),
                        "round": round_row.get("name"),
                        "player1": name1,
                        "player2": name2,
                        "scheduled_utc": when.isoformat(),
                        "is_doubles": "/" in str(name1) or "/" in str(name2),
                    }
                )
            if not payload.get("hasNextPage"):
                break
            page_no += 1
    rows.sort(key=lambda row: row["scheduled_utc"])
    return rows


def fixtures_loop() -> None:
    while True:
        try:
            rows = fetch_fixtures()
            bump(
                lambda state: state.update(
                    upcoming=rows, fixtures_updated_utc=utc_now_iso()
                )
            )
        except Exception as error:  # noqa: BLE001
            message = f"{type(error).__name__}: {error}"[:300]
            bump(
                lambda state: state.update(
                    errors={**state["errors"], "fixtures": message}
                )
            )
        time.sleep(FIXTURE_REFRESH_SECONDS)


def snapshot() -> dict[str, Any]:
    with STATE_LOCK:
        live = list(STATE["live"].values())
        live_names = {
            (match.get("player1"), match.get("player2")) for match in live
        }
        upcoming = [
            row
            for row in STATE["upcoming"]
            if (row["player1"], row["player2"]) not in live_names
            and dt.datetime.fromisoformat(row["scheduled_utc"])
            > dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=3)
        ]
        return {
            "version": VERSION["n"],
            "generated_utc": utc_now_iso(),
            "socket_status": STATE["socket_status"],
            "live_updated_utc": STATE["live_updated_utc"],
            "fixtures_updated_utc": STATE["fixtures_updated_utc"],
            "live": sorted(
                live, key=lambda match: (match["tour"], str(match["league"]))
            ),
            "upcoming": upcoming,
            "errors": STATE["errors"],
        }


app = FastAPI(title="Tennis Live Scores")


@app.on_event("startup")
def start_workers() -> None:
    threading.Thread(target=socket_loop, daemon=True).start()
    threading.Thread(target=fixtures_loop, daemon=True).start()
    covert_addon.start_threads()


@app.get("/covert")
def covert_state() -> JSONResponse:
    return JSONResponse({"status": covert_addon.STATUS,
                         "tickets": covert_addon.ledger_rows()})


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"ok": True, "time_utc": utc_now_iso()})


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse(snapshot())


@app.get("/api/stream")
def api_stream() -> StreamingResponse:
    def event_source():
        last_version = -1
        last_beat = 0.0
        while True:
            now = time.time()
            with STATE_LOCK:
                current = VERSION["n"]
            if current != last_version or now - last_beat > 25:
                last_version = current
                last_beat = now
                yield f"data: {json.dumps(snapshot(), ensure_ascii=False)}\n\n"
            time.sleep(2)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


PAGE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tennis Live Scores</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background:#0d1117; color:#e6edf3; font:15px/1.5 -apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif; padding:20px; max-width:880px; margin:0 auto; }
  h1 { font-size:20px; margin-bottom:4px; }
  .sub { color:#8b949e; font-size:12px; margin-bottom:20px; }
  h2 { font-size:14px; text-transform:uppercase; letter-spacing:.08em; color:#8b949e; margin:26px 0 10px; }
  .card { background:#161b22; border:1px solid #21262d; border-radius:10px; padding:12px 14px; margin-bottom:8px; display:flex; gap:12px; align-items:center; }
  .badge { flex:0 0 auto; font-size:10px; font-weight:700; letter-spacing:.06em; padding:3px 8px; border-radius:999px; }
  .badge.live { background:#da3633; color:#fff; animation:pulse 1.6s infinite; }
  .badge.tour { background:#21262d; color:#8b949e; }
  @keyframes pulse { 50% { opacity:.55; } }
  .meta { flex:1 1 auto; min-width:0; }
  .league { color:#8b949e; font-size:12px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .players div { display:flex; align-items:center; gap:6px; font-weight:600; }
  .serve { color:#3fb950; font-size:10px; }
  .score { flex:0 0 auto; text-align:right; font-variant-numeric:tabular-nums; }
  .sets { font-size:17px; font-weight:700; letter-spacing:.04em; }
  .pts { color:#d29922; font-size:13px; font-weight:700; }
  .when { flex:0 0 auto; color:#8b949e; font-size:13px; text-align:right; min-width:88px; }
  .empty { color:#8b949e; padding:14px; }
  footer { color:#484f58; font-size:11px; margin-top:28px; }
  .dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; vertical-align:middle; }
  .ok { background:#3fb950; } .bad { background:#da3633; }
</style>
</head>
<body>
<h1>🎾 Tennis Live Scores</h1>
<div class="sub">Matchstat live feed · 比分实时推送 · 时间为你的本地时区</div>
<h2 id="live-h">LIVE</h2><div id="live"></div>
<h2 id="up-h">即将开赛（48 小时内）</h2><div id="upcoming"></div>
<footer><span class="dot" id="statedot"></span><span id="stateline">connecting…</span></footer>
<script>
function fmtTime(iso){ const d=new Date(iso); const today=new Date(); const sameDay=d.toDateString()===today.toDateString();
  const hm=d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
  return sameDay?hm:d.toLocaleDateString([], {month:'2-digit',day:'2-digit'})+' '+hm; }
function esc(s){ return String(s??'').replace(/[&<>"]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function render(s){
  const live=document.getElementById('live'), up=document.getElementById('upcoming');
  document.getElementById('live-h').textContent = 'LIVE ('+s.live.length+')';
  live.innerHTML = s.live.length? s.live.map(m=>`
    <div class="card">
      <span class="badge live">LIVE</span><span class="badge tour">${esc(m.tour)}</span>
      <div class="meta">
        <div class="league">${esc(m.league)}</div>
        <div class="players">
          <div>${esc(m.player1)} ${m.serving===1?'<span class="serve">●</span>':''}</div>
          <div>${esc(m.player2)} ${m.serving===2?'<span class="serve">●</span>':''}</div>
        </div>
      </div>
      <div class="score"><div class="sets">${esc(m.sets.join('  '))}</div><div class="pts">${esc(m.points||'')}</div></div>
    </div>`).join('') : '<div class="empty">当前没有进行中的比赛</div>';
  const singles = s.upcoming.filter(u=>!u.is_doubles);
  document.getElementById('up-h').textContent = '即将开赛（48 小时内，单打 '+singles.length+'）';
  up.innerHTML = singles.length? singles.map(u=>`
    <div class="card">
      <span class="badge tour">${esc(u.tour)}</span>
      <div class="meta">
        <div class="league">${esc(u.tournament)}${u.round?' · '+esc(u.round):''}${u.surface?' · '+esc(u.surface):''}</div>
        <div class="players"><div>${esc(u.player1)}</div><div>${esc(u.player2)}</div></div>
      </div>
      <div class="when">${fmtTime(u.scheduled_utc)}</div>
    </div>`).join('') : '<div class="empty">暂无赛程数据</div>';
  const okSock = s.socket_status==='connected';
  document.getElementById('statedot').className='dot '+(okSock?'ok':'bad');
  document.getElementById('stateline').textContent =
    `socket: ${s.socket_status} · live更新: ${s.live_updated_utc? new Date(s.live_updated_utc).toLocaleTimeString():'-'} · 赛程更新: ${s.fixtures_updated_utc? new Date(s.fixtures_updated_utc).toLocaleTimeString():'-'}`;
}
let es;
function connect(){
  es = new EventSource('/api/stream');
  es.onmessage = e => { try { render(JSON.parse(e.data)); } catch(_){} };
  es.onerror = () => { es.close(); setTimeout(connect, 5000); };
}
connect();
setInterval(()=>{ fetch('/api/state').then(r=>r.json()).then(render).catch(()=>{}); }, 30000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(PAGE)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))

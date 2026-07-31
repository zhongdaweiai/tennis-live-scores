# Tennis Live Scores

Live tennis scoreboard fed by the Matchstat WebSocket (live matches with
real-time set/game/point scores) plus the Matchstat REST fixtures API
(upcoming matches, next 48 hours). Single FastAPI process; browsers receive
updates over Server-Sent Events.

## Run locally

```bash
pip install -r requirements.txt
RAPIDAPI_KEY=... python app.py
# open http://localhost:8000
```

## Deploy (Render)

`render.yaml` defines the service (free plan). The only manual step is
setting the `RAPIDAPI_KEY` environment variable in the Render dashboard —
it is deliberately `sync: false` and must never be committed to this repo.

Note on the free plan: the instance sleeps after ~15 minutes without
traffic; the first request after idle takes ~30-60 s and the socket
reconnects on wake. Upgrade the plan if always-on matters.

## Endpoints

- `/` scoreboard page
- `/api/state` JSON snapshot
- `/api/stream` SSE stream
- `/healthz` health check

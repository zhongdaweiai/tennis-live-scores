"""Covert early-hold signal detector - pure state machine, feed-agnostic.

Consumes Matchstat live-event updates (score / points / indicator) and emits a
trigger when the FROZEN rule fires:

  set 1, first 4 games complete, no breaks (2-2), Elo favorite (p >= 0.55)
  had >= 1 labored hold (deuce or break point faced) in their 2 service games,
  underdog held both service games clean.

Semantics validated on the 2026-07-21 24h tape:
  indicator "1,0" -> participant1 serving; points "X-Y" is p1-p2 fixed view;
  game boundary = current-set game score change; winner = incremented side.

Strictness: an event is eligible only if first observed at 0-0 games with a
known server; any inconsistency (score jump > 1 game, set > 1 reached before
4 games observed, missing server) marks it ineligible. One trigger per event.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Callable


def norm_name(x: object) -> str:
    """Accent-stripped, lowercased, token-sorted name key (self-contained)."""
    if not isinstance(x, str):
        return ""
    x = unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode()
    x = re.sub(r"[^a-z ]", " ", x.lower())
    return " ".join(sorted(x.split()))

POINT_RANKS = {"0": 0, "15": 1, "30": 2, "40": 3, "A": 4}


def parse_games(score: str) -> tuple[int, list[tuple[int, int]]] | None:
    """'2-6,1-0' -> (set_number, [(2,6),(1,0)]). None if unparseable."""
    try:
        sets = []
        for part in str(score).split(","):
            a, b = part.strip().split("-")
            sets.append((int(a), int(b)))
        return len(sets), sets
    except (ValueError, AttributeError):
        return None


def server_from_indicator(indicator: str) -> int | None:
    if indicator == "1,0":
        return 1
    if indicator == "0,1":
        return 2
    return None


def is_deuce(points: str) -> bool:
    return points.strip() == "40-40"


def is_break_point(points: str, server: int) -> bool:
    try:
        a, b = points.strip().split("-")
        ra, rb = POINT_RANKS.get(a), POINT_RANKS.get(b)
        if ra is None or rb is None:
            return False
    except ValueError:
        return False
    if server == 1:
        return (rb == 3 and ra < 3) or (rb == 4 and ra == 3)
    return (ra == 3 and rb < 3) or (ra == 4 and rb == 3)


@dataclass
class GameRecord:
    server: int
    deuce: bool = False
    bp_faced: bool = False
    winner: int = 0

    @property
    def quality(self) -> str:
        if self.winner != self.server:
            return "broken"
        return "labored" if (self.deuce or self.bp_faced) else "clean"


@dataclass
class EventState:
    event_id: str
    name: str
    p1: str
    p2: str
    league: str
    eligible: bool = True
    done: bool = False
    games: list[GameRecord] = field(default_factory=list)
    current: GameRecord | None = None
    last_pair: tuple[int, int] = (0, 0)
    first_seen_ok: bool = False
    ineligible_reason: str = ""

    def kill(self, reason: str) -> None:
        self.eligible = False
        self.ineligible_reason = reason


class CovertDetector:
    """Feed updates in; get trigger dicts out via the callback."""

    def __init__(self, elo_lookup: Callable[[str, str], tuple[float, int, int] | None],
                 min_fav_p: float = 0.55) -> None:
        self.events: dict[str, EventState] = {}
        self.elo_lookup = elo_lookup       # (p1, p2) -> (p1_win_prob, n1, n2) or None
        self.min_fav_p = min_fav_p
        self.triggers: list[dict] = []

    def update(self, ts: str, event_id: str, name: str, p1: str, p2: str,
               league: str, score: str, points: str, indicator: str) -> dict | None:
        st = self.events.get(event_id)
        parsed = parse_games(score)
        if parsed is None:
            return None
        set_no, sets = parsed
        ga, gb = sets[0]
        total_g = ga + gb

        if st is None:
            st = EventState(event_id, name, p1, p2, league)
            self.events[event_id] = st
            if not (set_no == 1 and total_g == 0):
                st.kill("first seen mid-match")
            else:
                st.first_seen_ok = True
        if st.done or not st.eligible:
            return None
        if set_no > 1:
            st.kill("reached set 2 before evaluation")
            return None
        if total_g > 4:
            st.kill("past game 4 without evaluation")
            return None

        server = server_from_indicator(indicator)
        pair = (ga, gb)
        if pair != st.last_pair:
            da, db = pair[0] - st.last_pair[0], pair[1] - st.last_pair[1]
            if da + db != 1 or min(da, db) < 0:
                st.kill(f"score jump {st.last_pair}->{pair}")
                return None
            winner = 1 if da == 1 else 2
            if st.current is None:
                st.kill("game ended without observed points")
                return None
            st.current.winner = winner
            st.games.append(st.current)
            st.current = None
            st.last_pair = pair
            if len(st.games) == 4:
                st.done = True
                return self._evaluate(ts, st)
            return None

        # same game: accumulate point-state flags
        if server is None:
            return None
        if st.current is None:
            st.current = GameRecord(server=server)
        elif st.current.server != server and points.strip() == "0-0":
            # transient indicator flip between games with no points yet
            st.current = GameRecord(server=server)
        if is_deuce(points):
            st.current.deuce = True
        if is_break_point(points, st.current.server):
            st.current.bp_faced = True
        return None

    def _evaluate(self, ts: str, st: EventState) -> dict | None:
        looked = self.elo_lookup(st.p1, st.p2)
        rec = {
            "ts": ts, "event_id": st.event_id, "name": st.name, "league": st.league,
            "p1": st.p1, "p2": st.p2,
            "games": [(g.server, g.quality) for g in st.games],
        }
        if looked is None:
            rec["outcome"] = "no_elo"
            return None
        p1_prob, n1, n2 = looked
        if min(n1, n2) < 12:
            rec["outcome"] = "thin_history"
            return None
        fav = 1 if p1_prob >= 0.5 else 2
        p_fav = p1_prob if fav == 1 else 1 - p1_prob
        if p_fav < self.min_fav_p:
            return None
        dog = 2 if fav == 1 else 1
        fav_games = [g for g in st.games if g.server == fav]
        dog_games = [g for g in st.games if g.server == dog]
        if len(fav_games) != 2 or len(dog_games) != 2:
            return None
        if any(g.quality == "broken" for g in st.games):
            return None                       # not on serve -> covert impossible
        fav_labored = sum(1 for g in fav_games if g.quality == "labored")
        dog_clean = sum(1 for g in dog_games if g.quality == "clean")
        if fav_labored >= 1 and dog_clean == 2:
            rec.update({"fav_side": fav, "dog_side": dog, "p_fav": round(p_fav, 4),
                        "fav_name": st.p1 if fav == 1 else st.p2,
                        "dog_name": st.p1 if dog == 1 else st.p2,
                        "fav_labored": fav_labored, "outcome": "TRIGGER"})
            self.triggers.append(rec)
            return rec
        return None

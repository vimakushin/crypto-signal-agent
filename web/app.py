"""FastAPI backend for the internal web UI (TZ section 6/10; see
`.claude/agents/web-dev.md` for the boundaries this file has to respect).

Scope of THIS file, right now: the manual episode-labeling screen only
("сработало / не сработало / рано судить" - TZ section 6, "ручная разметка
результатов срабатываний"). Candidate list, signal history, and
weights/watchlist config editing are separate screens for later tasks, not
started here (see the task instructions this file was written against).

This module never computes a signal or a score itself - it only reads what
storage/episodes.py and storage/db.py already computed/stored, and writes
manual labels back via storage/db.py's save_episode_outcome(). If a screen
ever needs a number this module can't get from storage/ as-is, that's a gap
to flag to python-dev, not something to approximate here.

Run with (from the project root, using the project's own Python):
    python -m uvicorn web.app:app --host 127.0.0.1 --port 8000

Deliberately bound to 127.0.0.1 only (see the run instructions above and
docs/README) - this has no authentication and must never be reachable from
outside the local machine, not even temporarily (CLAUDE.md / web-dev.md).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# web/app.py lives one level below the project root - same sys.path fix as
# storage/episodes.py, notifications/telegram_bot.py, scripts/replay_signals.py
# etc., so `from storage... / from main import ...` below resolve regardless
# of the working directory uvicorn was launched from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from main import load_config  # noqa: E402
from notifications.telegram_bot import _SIGNAL_FORMATTERS  # noqa: E402
from storage.db import (  # noqa: E402
    VALID_EPISODE_OUTCOMES,
    get_connection,
    get_coingecko_price_before,
    get_latest_binance_oi_snapshot,
    get_latest_coingecko_price_history,
    get_latest_coingecko_price_history_by_slug,
    get_latest_snapshot,
    save_episode_outcome,
)
from storage.episodes import (  # noqa: E402
    Episode,
    REVENUE_PRICE_GAP_FORMULA_CHANGE_DATE,
    ReviewableEpisode,
    get_open_episodes,
    get_reviewable_episodes,
)

app = FastAPI(title="crypto-signal-agent - разметка эпизодов")

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "нет данных"
    return f"${value:,.6g}"


def _fmt_mcap(value: float | None) -> str:
    if value is None:
        return "нет данных"
    return f"${value:,.0f}"


templates.env.filters["fmt_price"] = _fmt_price
templates.env.filters["fmt_mcap"] = _fmt_mcap


def _get_db_path(config: dict) -> Path:
    db_path = Path(config["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    return db_path


def _parse_details(details_json: str | None) -> dict:
    if not details_json:
        return {}
    try:
        return json.loads(details_json)
    except (TypeError, ValueError):
        return {}


def _format_signal_details(signal_name: str, details_json: str | None) -> str:
    """Human-readable breakdown of what a signal fired on, reusing the same
    formatters notifications/telegram_bot.py already uses for the Telegram
    digest - not reimplemented here, so the wording/thresholds shown always
    match what the daily digest already says (see this task's instructions:
    "не переписывай этот текст заново").
    """
    details = _parse_details(details_json)
    formatter = _SIGNAL_FORMATTERS.get(signal_name)
    if formatter is None:
        return f"{signal_name}: нет форматтера для этого сигнала"
    try:
        return formatter(details)
    except (TypeError, ValueError, KeyError):
        return f"{signal_name}: не удалось разобрать сохранённые детали"


def _label_for(episode: Episode, first_details: dict) -> str:
    """Coin/ticker text shown to the human - prefer the symbol stashed in
    details_json (revenue_price_gap) when available, otherwise fall back to
    protocol_slug itself (which IS the human-recognizable ticker already for
    oi_divergence, and is at least the DeFiLlama slug for volume_breakout).
    """
    symbol = first_details.get("symbol")
    if symbol:
        return f"{symbol} ({episode.protocol_slug})"
    return episode.protocol_slug


class EpisodeView(BaseModel):
    """Everything one row of the episode-labeling table needs to render -
    the same shape this route would hand to a future React frontend as JSON
    (see web-dev.md: "роут отдаёт то же самое, что позже отдавал бы в JSON
    API для React"), Jinja just renders it as HTML for now.
    """

    signal_name: str
    protocol_slug: str
    label: str
    signal_description: str
    episode_start_date: str
    episode_end_date: str
    duration_days: int
    is_open: bool
    old_formula_badge: bool
    price_start: float | None = None
    price_now: float | None = None
    mcap_start: float | None = None
    mcap_now: float | None = None
    mcap_note: str | None = None
    prior_outcome: str | None = None
    prior_outcome_comment: str | None = None
    prior_outcome_at: str | None = None


def _price_mcap_for(conn, episode: Episode, first_details: dict, last_details: dict) -> dict:
    """Capitalization/price at episode start and "now" - source picked per
    signal_name, exactly as laid out in this task's instructions (each
    signal_name means a different protocol_slug namespace and has its price
    stashed in a different place). Returns a dict merge-able straight into
    EpisodeView's kwargs. Missing data comes back as None (rendered "нет
    данных" by the fmt_price/fmt_mcap Jinja filters) - never approximated or
    defaulted to 0.
    """
    slug = episode.protocol_slug

    if episode.signal_name == "revenue_price_gap":
        snapshot = get_latest_snapshot(conn, slug)
        mcap_now = snapshot["mcap"] if snapshot else None
        mcap_start = first_details.get("mcap_now")
        price_row = get_latest_coingecko_price_history_by_slug(conn, slug)
        price_now = price_row["price"] if price_row else None
        price_before_row = get_coingecko_price_before(conn, slug, episode.episode_start_date)
        price_start = price_before_row["price"] if price_before_row else None
        return dict(
            price_start=price_start, price_now=price_now,
            mcap_start=mcap_start, mcap_now=mcap_now, mcap_note=None,
        )

    if episode.signal_name == "volume_breakout":
        snapshot = get_latest_snapshot(conn, slug)
        mcap_now = snapshot["mcap"] if snapshot else None
        price_start = first_details.get("price_now")
        gecko_id = last_details.get("gecko_id") or first_details.get("gecko_id")
        price_now = None
        if gecko_id:
            price_row = get_latest_coingecko_price_history(conn, gecko_id)
            price_now = price_row["price"] if price_row else None
        return dict(
            price_start=price_start, price_now=price_now,
            mcap_start=None, mcap_now=mcap_now, mcap_note=None,
        )

    if episode.signal_name == "oi_divergence":
        oi_row = get_latest_binance_oi_snapshot(conn, slug)
        price_now = oi_row["price"] if oi_row else None
        price_start = first_details.get("price_now")
        note = "не применимо (Binance Futures)"
        return dict(
            price_start=price_start, price_now=price_now,
            mcap_start=None, mcap_now=None, mcap_note=note,
        )

    # Unknown/future signal_name: say so plainly rather than guess a source.
    return dict(
        price_start=None, price_now=None, mcap_start=None, mcap_now=None,
        mcap_note="источник цены/капитализации для этого сигнала не определён",
    )


def _build_view(conn, episode: Episode, prior: ReviewableEpisode | None = None) -> EpisodeView:
    first_details = _parse_details(episode.first_details_json)
    last_details = _parse_details(episode.last_details_json)

    old_formula_badge = (
        episode.signal_name == "revenue_price_gap"
        and episode.episode_end_date < REVENUE_PRICE_GAP_FORMULA_CHANGE_DATE.isoformat()
    )

    price_mcap = _price_mcap_for(conn, episode, first_details, last_details)

    return EpisodeView(
        signal_name=episode.signal_name,
        protocol_slug=episode.protocol_slug,
        label=_label_for(episode, first_details),
        signal_description=_format_signal_details(episode.signal_name, episode.first_details_json),
        episode_start_date=episode.episode_start_date,
        episode_end_date=episode.episode_end_date,
        duration_days=episode.duration_days,
        is_open=episode.is_open,
        old_formula_badge=old_formula_badge,
        prior_outcome=prior.prior_outcome if prior else None,
        prior_outcome_comment=prior.prior_outcome_comment if prior else None,
        prior_outcome_at=prior.prior_outcome_at if prior else None,
        **price_mcap,
    )


@app.get("/", include_in_schema=False)
def root() -> HTMLResponse:
    return HTMLResponse(
        '<meta http-equiv="refresh" content="0; url=/episodes">'
        '<a href="/episodes">Перейти к разметке эпизодов</a>'
    )


@app.get("/episodes", response_class=HTMLResponse)
def episodes_page(request: Request, show_old_formula: bool = True) -> HTMLResponse:
    config = load_config()
    conn = get_connection(_get_db_path(config))
    try:
        open_episodes = get_open_episodes(conn, config)
        reviewable_episodes = get_reviewable_episodes(conn, config)
        new_views = [_build_view(conn, ep) for ep in open_episodes]
        reviewable_views = [
            _build_view(conn, r.episode, prior=r) for r in reviewable_episodes
        ]
    finally:
        conn.close()

    if not show_old_formula:
        new_views = [v for v in new_views if not v.old_formula_badge]
        reviewable_views = [v for v in reviewable_views if not v.old_formula_badge]

    return templates.TemplateResponse(
        request,
        "episodes.html",
        {
            "new_episodes": new_views,
            "reviewable_episodes": reviewable_views,
            "show_old_formula": show_old_formula,
            "valid_outcomes": VALID_EPISODE_OUTCOMES,
        },
    )


@app.post("/episodes/outcome", response_class=HTMLResponse)
def post_episode_outcome(
    request: Request,
    signal_name: str = Form(...),
    protocol_slug: str = Form(...),
    episode_start_date: str = Form(...),
    outcome: str = Form(...),
    is_open: str = Form(...),
    outcome_comment: str = Form(""),
) -> HTMLResponse:
    """Saves/overwrites one episode's manual label. On success returns an
    empty body - the row's hx-target="closest tr" / hx-swap="outerHTML" (see
    templates/_episode_row.html) then removes the row from the page without
    a full reload. On a bad `outcome` (shouldn't happen from the UI's own
    fixed three buttons, but guarded anyway per save_episode_outcome's
    contract), re-renders the same row with an inline error instead of
    silently failing or 500ing - see base.html's htmx:beforeSwap handler,
    which allows a 400 response body to still swap in.
    """
    config = load_config()
    conn = get_connection(_get_db_path(config))
    error: str | None = None
    try:
        try:
            save_episode_outcome(
                conn,
                signal_name=signal_name,
                protocol_slug=protocol_slug,
                episode_start_date=episode_start_date,
                outcome=outcome,
                outcome_comment=outcome_comment or None,
                labeled_when_open=is_open.lower() in ("true", "1", "on", "yes"),
                outcome_at=datetime.now(timezone.utc).isoformat(),
            )
        except ValueError as exc:
            error = str(exc)
    finally:
        conn.close()

    if error is None:
        return HTMLResponse("")

    return templates.TemplateResponse(
        request,
        "_error_row.html",
        {
            "signal_name": signal_name,
            "protocol_slug": protocol_slug,
            "episode_start_date": episode_start_date,
            "is_open": is_open,
            "valid_outcomes": VALID_EPISODE_OUTCOMES,
            "error": error,
        },
        status_code=400,
    )

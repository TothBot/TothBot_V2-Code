#!/usr/bin/env python3
# DocDCN:     1011001
# DocTitle:   Kraken_WS_Probe
# DocVersion: dv1_0
# DocOwner:   Bill
# DocPath:    github.com/TothBot/TothBot_V2-Code/operations/kraken_ws_probe.py
# DocDate:    04-26-2026
# DocTime:    03:24:45 UTC
# ============================================================
#
# PURPOSE
# ------------------------------------------------------------
# Empirical probe of Kraken WS v2 subscribe rate-limit
# behavior. Resolves analysis ledger (TB00140 NSI v1_1 Section
# 5) Q5 (per-message symbol-count cap), Q6 (steady-state-safe
# theory), Q7 (current universe size USD+USDC+USDT online),
# and Q8 (steady-state msg rate per pair).
#
# Standalone non-trading public-WS read-only probe. Runs as a
# separate process from the production tothbot service. No
# auth. No orders. No private endpoint. No batch_cancel. No
# cancel_all_orders_after. Production tothbot is undisturbed.
#
# First-class controlled operational-support script per
# 0311001 v1_3 HR-DC-001. Lives in TothBot_V2-Code operations/
# subfolder parallel to the tothbot/ Python package. DocDCN
# 1011001 follows the tothbot_sweep.sh dv1_0 convention while
# the operational-scripts coding spec in AA=10 Sub-Domain 1
# Operations remains TBD per 0311001 v1_3 Section 5.2.
#
# USAGE
# ------------------------------------------------------------
#   python kraken_ws_probe.py --phase A [--out PATH]
#   python kraken_ws_probe.py --phase B [--out PATH]
#       [--duration SECONDS] [--max-symbols N]
#   python kraken_ws_probe.py --phase C [--out PATH]
#   python kraken_ws_probe.py --phase D [--out PATH]
#       [--duration SECONDS]
#
# Default output path:
#   /tmp/kraken_ws_probe_<phase>_<UTC-isoformat>.json
#
# Deploy via `git pull` on the VPS. Never via scp from a
# local path outside the governed repository.
#
# ------------------------------------------------------------
# REVISION HISTORY
# ------------------------------------------------------------
#
#   dv1_0  04-26-2026  Initial. TB00141 STREAM 1. NEW
#                      authoring per TB00140 NSI v1_1 Section
#                      1. Empirical probe-script for Kraken
#                      WS v2 subscribe rate-limit behavior.
#                      Four phases (A=burst, B=steady-state,
#                      C=reconnect, D=multi-connection)
#                      selectable via --phase. Pre-phase
#                      AssetPairs discovery filters universe
#                      to USD+USDC+USDT online pairs per D-03
#                      (USD+USDC+USDT, no cap) and D-08
#                      (Top-N retired). Connection parameters
#                      per 0411001 dv1_11 WS-LIB-002 / -003 /
#                      -004. Subscribe wire format per 0411001
#                      dv1_11 Section 4.3 (ohlc) and Section
#                      4.4 (ticker). Resolves analysis ledger
#                      Q5 / Q6 / Q7 / Q8. Non-trading: public
#                      WS endpoint only, no auth, no orders,
#                      D-06 paper-to-live parity preserved by
#                      probe being a separate process from the
#                      tothbot service.
# ============================================================

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import websockets
from websockets.asyncio.client import connect


# ============================================================
# CONSTANTS
# ============================================================

# Public WS endpoint per 0411001 dv1_11 WS-EP-001. The probe
# never opens the private endpoint. Public WS is receive-only
# and requires no authentication.
WS_PUBLIC_URI = "wss://ws.kraken.com/v2"

# Public REST AssetPairs endpoint for pair discovery. No auth.
REST_ASSET_PAIRS = "https://api.kraken.com/0/public/AssetPairs"

# Quote currencies enforced by D-03 (USD + USDC + USDT, no
# cap). Kraken returns "ZUSD" for some legacy USD pairs in
# the AssetPairs response — both forms accepted.
ALLOWED_QUOTES = {"USD", "ZUSD", "USDC", "USDT"}

# WS connect parameters per 0411001 dv1_11 WS-LIB-002 / -003
# / -004. max_size raised to 10 MB so the instrument-channel
# snapshot for 500+ pairs cannot trigger PayloadTooBig
# silent disconnects (the probe does not subscribe to
# instrument, but the same defensive ceiling is applied).
# max_queue=None prevents burst event loss during snapshot
# delivery. ping_interval=None disables library TCP-level
# PING; Kraken keepalive is application-level JSON ping per
# WS-PING-001 — not exercised by this short-lived probe.
WS_MAX_SIZE = 10 * 1024 * 1024
WS_OPEN_TIMEOUT = 10
WS_MAX_QUEUE = None
WS_PING_INTERVAL = None

# Phase A burst N values. Span covers the universe (Q7 ~280
# pairs at TB00135) plus deliberate oversubscription to drive
# the per-message symbol-count cap discovery (Q5).
PHASE_A_N_VALUES = [50, 100, 150, 200, 300, 400, 500]
PHASE_A_CHANNELS = ["ticker", "ohlc"]
PHASE_A_PER_CELL_TIMEOUT_S = 30
PHASE_A_COOLING_SLEEP_S = 10

# Phase B / D runtime defaults.
PHASE_B_DEFAULT_DURATION_S = 7200      # 2 hours
PHASE_B_SNAPSHOT_INTERVAL_S = 300      # 5 minutes
PHASE_D_CONNECTION_COUNT = 2

# Phase C iteration parameters.
PHASE_C_ITERATIONS = 3
PHASE_C_STEADY_WAIT_S = 60
PHASE_C_BETWEEN_SLEEP_S = 30
PHASE_C_RESUB_BURST_TIMEOUT_S = 60

# Kraken ohlc 5-minute candle interval per WS-OHLC-001.
OHLC_INTERVAL = 5

# req_id base — every outbound WS message carries a client-
# assigned req_id per WS-OUT-001 / WS-EXE-006. The probe
# only emits subscribe / unsubscribe (public WS), but
# follows the same convention for parity with WS Manager.
REQ_ID_BASE = 1


# ============================================================
# HELPERS
# ============================================================

def _utc_iso_filename() -> str:
    """Filename-safe UTC isoformat (no colons)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def _utc_iso() -> str:
    """Microsecond-precision UTC isoformat for log records."""
    return datetime.now(timezone.utc).isoformat()


def _default_out_path(phase: str) -> str:
    return f"/tmp/kraken_ws_probe_{phase}_{_utc_iso_filename()}.json"


def fetch_universe() -> dict:
    """REST GET AssetPairs and filter per D-03 + status==online.

    Resolves analysis ledger Q7 (current universe size
    USD+USDC+USDT online). Output is sorted alphabetical by
    wsname for determinism so every phase walks the same
    symbol order across invocations.
    """
    req = urllib.request.Request(
        REST_ASSET_PAIRS,
        headers={"User-Agent": "tothbot-kraken-ws-probe/dv1_0"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read()
    payload = json.loads(body.decode("utf-8"))
    if payload.get("error"):
        raise RuntimeError(f"Kraken REST AssetPairs error: {payload['error']}")
    pairs_raw = payload.get("result", {})
    if not isinstance(pairs_raw, dict):
        raise RuntimeError("Kraken REST AssetPairs malformed: result not dict")

    filtered = []
    for kraken_id, info in pairs_raw.items():
        status = info.get("status")
        quote = info.get("quote")
        wsname = info.get("wsname")
        if status != "online":
            continue
        if quote not in ALLOWED_QUOTES:
            continue
        if not wsname:
            continue
        filtered.append({
            "kraken_id": kraken_id,
            "wsname": wsname,
            "base": info.get("base"),
            "quote": quote,
            "status": status,
        })

    filtered.sort(key=lambda r: r["wsname"])
    return {
        "fetched_at_utc": _utc_iso(),
        "universe_size": len(filtered),
        "rest_pair_count_total": len(pairs_raw),
        "allowed_quotes": sorted(ALLOWED_QUOTES),
        "pairs": filtered,
    }


def build_subscribe_msg(channel: str, symbols: list, req_id: int) -> dict:
    """Subscribe wire format per 0411001 dv1_11 Section 4.3
    (ohlc, WS-OHLC-006) and Section 4.4 (ticker, WS-TKR-004).

    snapshot=True matches production WS Manager subscribe
    behavior, keeping probe traffic representative under D-06
    paper-to-live parity. event_trigger=trades is correct for
    no-position pairs per WS-TKR-002 (the probe holds zero
    positions).
    """
    params = {
        "channel": channel,
        "symbol": symbols,
        "snapshot": True,
    }
    if channel == "ohlc":
        params["interval"] = OHLC_INTERVAL
    if channel == "ticker":
        params["event_trigger"] = "trades"
    return {"method": "subscribe", "params": params, "req_id": req_id}


def build_unsubscribe_msg(channel: str, symbols: list, req_id: int) -> dict:
    params = {"channel": channel, "symbol": symbols}
    if channel == "ohlc":
        params["interval"] = OHLC_INTERVAL
    if channel == "ticker":
        params["event_trigger"] = "trades"
    return {"method": "unsubscribe", "params": params, "req_id": req_id}


async def open_public_ws():
    """connect() per 0411001 dv1_11 WS-LIB-004 template."""
    return await connect(
        WS_PUBLIC_URI,
        max_size=WS_MAX_SIZE,
        open_timeout=WS_OPEN_TIMEOUT,
        max_queue=WS_MAX_QUEUE,
        ping_interval=WS_PING_INTERVAL,
    )


async def wait_for_status_online(ws, timeout_s: float = 15.0):
    """Drain initial frames until the system status:online frame
    arrives per WS-STAT-001 / WS-STAT-005. Returns the status
    message dict (so connection_id can be captured per
    WS-STAT-006) or None on timeout.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        except asyncio.TimeoutError:
            return None
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if msg.get("channel") == "status":
            data = msg.get("data") or []
            if data and data[0].get("system") in ("online", "ok"):
                return msg
    return None


def write_json_output(out_path: str, payload: dict) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def classify_inbound(msg: dict) -> str:
    """Coarse classifier for steady-state msg counting (Q8)."""
    if "method" in msg:
        return f"method:{msg['method']}"
    ch = msg.get("channel")
    if ch:
        if msg.get("type"):
            return f"channel:{ch}:{msg['type']}"
        return f"channel:{ch}"
    if "error" in msg:
        return "error"
    return "unknown"


def _is_rate_limit_error(msg: dict) -> bool:
    """Detect Kraken 'Exceeded msg rate' (or similar) payload.
    Kraken WS v2 returns the rate-limit reason in the error
    field of the affected response or as a top-level error.
    """
    err = msg.get("error")
    if not err:
        return False
    text = str(err).lower()
    return "rate" in text or "exceed" in text


# ============================================================
# PHASE A — BURST (Q5 resolution)
# ============================================================

async def phase_a(universe: dict) -> dict:
    """Per-channel x per-N batched-subscribe ceiling discovery.

    Resolves analysis ledger Q5 (per-message symbol-count
    cap). For each (channel, N) cell: open fresh WS, wait for
    system online, send single batched subscribe, listen up
    to PHASE_A_PER_CELL_TIMEOUT_S OR until full subscription
    confirmed OR until error, record timings and ack counts,
    send batched unsubscribe, close, sleep
    PHASE_A_COOLING_SLEEP_S between cells.
    """
    pairs = universe["pairs"]
    universe_size = universe["universe_size"]
    cells = []
    req_id = REQ_ID_BASE

    for channel in PHASE_A_CHANNELS:
        for n in PHASE_A_N_VALUES:
            cell = {
                "channel": channel,
                "n_requested": n,
                "started_at_utc": _utc_iso(),
            }
            if n > universe_size:
                cell["skipped"] = True
                cell["reason"] = "N_exceeds_universe"
                cell["universe_size"] = universe_size
                cells.append(cell)
                continue

            symbols = [p["wsname"] for p in pairs[:n]]
            cell["n_used"] = len(symbols)

            try:
                ws = await open_public_ws()
            except Exception as exc:
                cell["error_msg"] = f"connect_failed: {exc!r}"
                cell["success_flag"] = False
                cells.append(cell)
                continue

            try:
                status_msg = await wait_for_status_online(ws)
                cell["status_online_received"] = bool(status_msg)
                if status_msg:
                    data = status_msg.get("data") or [{}]
                    cell["connection_id"] = data[0].get("connection_id")

                req_id += 1
                sub = build_subscribe_msg(channel, symbols, req_id)
                t_send = time.monotonic()
                cell["t_send_monotonic"] = t_send
                cell["sent_at_utc"] = _utc_iso()
                await ws.send(json.dumps(sub))

                t_first_ack = None
                t_last_ack = None
                ack_count = 0
                error_msg = None
                deadline = t_send + PHASE_A_PER_CELL_TIMEOUT_S
                expected_acks = len(symbols)

                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        break
                    except (websockets.ConnectionClosed, OSError) as exc:
                        error_msg = f"connection_closed: {exc!r}"
                        break
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("method") == "subscribe":
                        if msg.get("success") is False:
                            error_msg = msg.get(
                                "error",
                                msg.get("result", {}).get("error", "unknown"),
                            )
                        else:
                            now = time.monotonic()
                            ack_count += 1
                            if t_first_ack is None:
                                t_first_ack = now
                            t_last_ack = now
                            if ack_count >= expected_acks:
                                break
                    elif _is_rate_limit_error(msg):
                        error_msg = msg.get("error")

                cell["t_first_ack_monotonic"] = t_first_ack
                cell["t_last_ack_monotonic"] = t_last_ack
                cell["t_first_ack_delta_s"] = (
                    t_first_ack - t_send if t_first_ack else None
                )
                cell["t_last_ack_delta_s"] = (
                    t_last_ack - t_send if t_last_ack else None
                )
                cell["ack_count"] = ack_count
                cell["expected_acks"] = expected_acks
                cell["error_msg"] = error_msg
                cell["success_flag"] = (
                    error_msg is None and ack_count >= expected_acks
                )

                # Best-effort batched unsubscribe before close
                # so the next cell starts from a clean slate
                # (closing the socket also releases server-side
                # subscriptions, but explicit unsubscribe is
                # the documented teardown).
                try:
                    req_id += 1
                    unsub = build_unsubscribe_msg(channel, symbols, req_id)
                    await ws.send(json.dumps(unsub))
                except Exception:
                    pass

            except Exception as exc:
                cell["error_msg"] = f"phase_a_exception: {exc!r}"
                cell["success_flag"] = False
            finally:
                try:
                    await ws.close()
                except Exception:
                    pass

            cell["finished_at_utc"] = _utc_iso()
            cells.append(cell)
            await asyncio.sleep(PHASE_A_COOLING_SLEEP_S)

    return {
        "phase": "A",
        "purpose": "Q5 per-message symbol-count cap discovery",
        "n_values_tested": PHASE_A_N_VALUES,
        "channels_tested": PHASE_A_CHANNELS,
        "per_cell_timeout_s": PHASE_A_PER_CELL_TIMEOUT_S,
        "cooling_sleep_s": PHASE_A_COOLING_SLEEP_S,
        "cells": cells,
    }


# ============================================================
# PHASE B — STEADY-STATE (Q6 + Q8 resolution)
# ============================================================

async def phase_b(universe: dict, duration_s: int,
                  max_symbols) -> dict:
    """One WS, batched-subscribe full universe (or capped),
    run for duration_s, snapshot every PHASE_B_SNAPSHOT_INTERVAL_S.

    Resolves Q6 (steady-state-safe theory) by observing
    rate-limit errors and disconnects across the duration
    window, and Q8 (per-pair msg rate) via per-symbol
    inbound counting.
    """
    pairs = universe["pairs"]
    if max_symbols is not None and max_symbols < len(pairs):
        symbols = [p["wsname"] for p in pairs[:max_symbols]]
        cap_applied = True
    else:
        symbols = [p["wsname"] for p in pairs]
        cap_applied = False

    record = {
        "phase": "B",
        "purpose": "Q6 steady-state-safe theory + Q8 per-pair msg rate",
        "duration_s_requested": duration_s,
        "snapshot_interval_s": PHASE_B_SNAPSHOT_INTERVAL_S,
        "symbols_subscribed": len(symbols),
        "cap_applied": cap_applied,
        "channels": ["ticker", "ohlc-5"],
        "started_at_utc": _utc_iso(),
        "startup_record": None,
        "snapshots": [],
        "rate_limit_errors": [],
        "disconnect_events": [],
        "final_totals": None,
        "finished_at_utc": None,
    }

    outbound_count_by_method = {"subscribe": 0}
    inbound_count_by_type = {}
    per_pair_msg_count = {}

    ws = await open_public_ws()
    try:
        status_msg = await wait_for_status_online(ws)
        startup = {
            "connected_at_utc": _utc_iso(),
            "status_online_received": bool(status_msg),
        }
        if status_msg:
            data = status_msg.get("data") or [{}]
            startup["connection_id"] = data[0].get("connection_id")
        record["startup_record"] = startup

        req_id = REQ_ID_BASE
        for channel in ("ticker", "ohlc"):
            req_id += 1
            sub = build_subscribe_msg(channel, symbols, req_id)
            await ws.send(json.dumps(sub))
            outbound_count_by_method["subscribe"] += 1
        record["startup_subscribe_sent_at_utc"] = _utc_iso()

        run_started = time.monotonic()
        next_snapshot = run_started + PHASE_B_SNAPSHOT_INTERVAL_S
        deadline = run_started + duration_s

        while time.monotonic() < deadline:
            now = time.monotonic()
            timeout = min(deadline - now, max(0.0, next_snapshot - now))
            timeout = max(0.05, timeout)
            raw = None
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                pass
            except (websockets.ConnectionClosed, OSError) as exc:
                record["disconnect_events"].append({
                    "at_utc": _utc_iso(),
                    "elapsed_s": time.monotonic() - run_started,
                    "exc": repr(exc),
                })
                break

            if raw is not None:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    msg = {"_raw_decode_error": True}
                bucket = classify_inbound(msg)
                inbound_count_by_type[bucket] = (
                    inbound_count_by_type.get(bucket, 0) + 1
                )
                if _is_rate_limit_error(msg):
                    record["rate_limit_errors"].append({
                        "at_utc": _utc_iso(),
                        "elapsed_s": time.monotonic() - run_started,
                        "msg": msg,
                    })
                if msg.get("channel") in ("ticker", "ohlc"):
                    data = msg.get("data") or []
                    for d in data:
                        sym = d.get("symbol")
                        if sym:
                            per_pair_msg_count[sym] = (
                                per_pair_msg_count.get(sym, 0) + 1
                            )

            if time.monotonic() >= next_snapshot:
                snap = {
                    "at_utc": _utc_iso(),
                    "elapsed_s": time.monotonic() - run_started,
                    "outbound_by_method": dict(outbound_count_by_method),
                    "inbound_by_type": dict(inbound_count_by_type),
                    "rate_limit_errors_count": len(
                        record["rate_limit_errors"]
                    ),
                    "disconnect_events_count": len(
                        record["disconnect_events"]
                    ),
                    "distinct_pairs_seen": len(per_pair_msg_count),
                }
                record["snapshots"].append(snap)
                next_snapshot += PHASE_B_SNAPSHOT_INTERVAL_S
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    record["final_totals"] = {
        "outbound_by_method": dict(outbound_count_by_method),
        "inbound_by_type": dict(inbound_count_by_type),
        "rate_limit_errors_count": len(record["rate_limit_errors"]),
        "disconnect_events_count": len(record["disconnect_events"]),
        "distinct_pairs_seen": len(per_pair_msg_count),
        "per_pair_msg_count": per_pair_msg_count,
    }
    record["finished_at_utc"] = _utc_iso()
    return record


# ============================================================
# PHASE C — RECONNECT (Q6 reconnect arm)
# ============================================================

async def phase_c(universe: dict) -> dict:
    """Open WS, batched-subscribe full universe, wait for
    steady state, force close, reconnect, re-subscribe,
    record timings. Repeat PHASE_C_ITERATIONS times.

    Resolves the reconnect arm of Q6 — the snapshot burst
    rate at re-subscribe time is the most likely point at
    which the rate limiter reactivates.
    """
    symbols = [p["wsname"] for p in universe["pairs"]]
    iterations = []

    for i in range(PHASE_C_ITERATIONS):
        rec = {
            "iteration": i + 1,
            "started_at_utc": _utc_iso(),
            "symbols_subscribed": len(symbols),
        }

        # First connection: subscribe and wait for steady state.
        try:
            ws1 = await open_public_ws()
        except Exception as exc:
            rec["error_msg"] = f"initial_connect_failed: {exc!r}"
            rec["finished_at_utc"] = _utc_iso()
            iterations.append(rec)
            if i < PHASE_C_ITERATIONS - 1:
                await asyncio.sleep(PHASE_C_BETWEEN_SLEEP_S)
            continue

        try:
            await wait_for_status_online(ws1)
            req_id = REQ_ID_BASE
            for channel in ("ticker", "ohlc"):
                req_id += 1
                await ws1.send(json.dumps(
                    build_subscribe_msg(channel, symbols, req_id)
                ))
            steady_deadline = time.monotonic() + PHASE_C_STEADY_WAIT_S
            initial_msgs = 0
            while time.monotonic() < steady_deadline:
                remaining = steady_deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(ws1.recv(), timeout=remaining)
                    initial_msgs += 1
                except asyncio.TimeoutError:
                    break
                except (websockets.ConnectionClosed, OSError):
                    break
            rec["initial_steady_msgs_received"] = initial_msgs
        finally:
            rec["force_close_at_utc"] = _utc_iso()
            try:
                await ws1.close()
            except Exception:
                pass

        # Reconnect arm.
        ws2 = None
        t_recon_start = time.monotonic()
        try:
            ws2 = await open_public_ws()
            t_reconnected = time.monotonic()
            rec["time_to_reconnect_s"] = t_reconnected - t_recon_start
            rec["reconnected_at_utc"] = _utc_iso()

            await wait_for_status_online(ws2)
            req_id2 = REQ_ID_BASE + 100
            t_resub_start = time.monotonic()
            for channel in ("ticker", "ohlc"):
                req_id2 += 1
                await ws2.send(json.dumps(
                    build_subscribe_msg(channel, symbols, req_id2)
                ))

            ack_count = 0
            rate_errs = 0
            burst_msgs = 0
            t_first_ack = None
            t_last_ack = None
            expected_acks = len(symbols) * 2  # ticker + ohlc per symbol
            burst_deadline = t_resub_start + PHASE_C_RESUB_BURST_TIMEOUT_S
            while time.monotonic() < burst_deadline:
                remaining = burst_deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws2.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                except (websockets.ConnectionClosed, OSError):
                    break
                burst_msgs += 1
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if msg.get("method") == "subscribe":
                    ack_count += 1
                    if t_first_ack is None:
                        t_first_ack = time.monotonic()
                    t_last_ack = time.monotonic()
                if _is_rate_limit_error(msg):
                    rate_errs += 1
                if ack_count >= expected_acks:
                    break

            rec["resubscribe_ack_count"] = ack_count
            rec["resubscribe_expected_acks"] = expected_acks
            rec["time_to_first_resub_ack_s"] = (
                t_first_ack - t_resub_start if t_first_ack else None
            )
            rec["time_to_last_resub_ack_s"] = (
                t_last_ack - t_resub_start if t_last_ack else None
            )
            rec["snapshot_burst_msgs_received"] = burst_msgs
            rec["rate_limit_errors_seen"] = rate_errs
        except Exception as exc:
            rec["error_msg"] = f"reconnect_arm_failed: {exc!r}"
        finally:
            if ws2 is not None:
                try:
                    await ws2.close()
                except Exception:
                    pass

        rec["finished_at_utc"] = _utc_iso()
        iterations.append(rec)
        if i < PHASE_C_ITERATIONS - 1:
            await asyncio.sleep(PHASE_C_BETWEEN_SLEEP_S)

    return {
        "phase": "C",
        "purpose": "Q6 reconnect-arm resolution",
        "iterations_total": PHASE_C_ITERATIONS,
        "steady_wait_s": PHASE_C_STEADY_WAIT_S,
        "between_sleep_s": PHASE_C_BETWEEN_SLEEP_S,
        "iterations": iterations,
    }


# ============================================================
# PHASE D — MULTI-CONNECTION (P2 viability)
# ============================================================

async def phase_d(universe: dict, duration_s: int) -> dict:
    """N concurrent WS connections, alternating-index pair
    distribution across connections, batched subscribe per
    connection, run steady-state observation for duration_s.

    Run only if Phase A / B reveal single-connection
    insufficient. Aggregate inbound / outbound rate is
    compared against the single-connection Phase B baseline
    in subsequent STREAM 2 chat analysis.
    """
    pairs = universe["pairs"]
    n_conns = PHASE_D_CONNECTION_COUNT

    buckets = [[] for _ in range(n_conns)]
    for idx, p in enumerate(pairs):
        buckets[idx % n_conns].append(p["wsname"])

    record = {
        "phase": "D",
        "purpose": "P2 multi-connection viability vs single-conn baseline",
        "connection_count": n_conns,
        "duration_s_requested": duration_s,
        "distribution_strategy": "alternating_index",
        "per_connection_symbol_counts": [len(b) for b in buckets],
        "started_at_utc": _utc_iso(),
        "per_connection": [],
        "aggregate_totals": None,
        "finished_at_utc": None,
    }

    async def run_one(conn_idx: int, symbols: list) -> dict:
        per = {
            "connection_index": conn_idx,
            "symbols_subscribed": len(symbols),
            "outbound_subscribes": 0,
            "inbound_count_by_type": {},
            "rate_limit_errors": 0,
            "disconnect_events": 0,
            "started_at_utc": _utc_iso(),
        }
        try:
            ws = await open_public_ws()
        except Exception as exc:
            per["error_msg"] = f"connect_failed: {exc!r}"
            per["finished_at_utc"] = _utc_iso()
            return per
        try:
            status_msg = await wait_for_status_online(ws)
            if status_msg:
                data = status_msg.get("data") or [{}]
                per["connection_id"] = data[0].get("connection_id")
            req_id = REQ_ID_BASE + (conn_idx * 1000)
            for channel in ("ticker", "ohlc"):
                req_id += 1
                await ws.send(json.dumps(
                    build_subscribe_msg(channel, symbols, req_id)
                ))
                per["outbound_subscribes"] += 1
            deadline = time.monotonic() + duration_s
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                except (websockets.ConnectionClosed, OSError):
                    per["disconnect_events"] += 1
                    break
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                bucket = classify_inbound(msg)
                per["inbound_count_by_type"][bucket] = (
                    per["inbound_count_by_type"].get(bucket, 0) + 1
                )
                if _is_rate_limit_error(msg):
                    per["rate_limit_errors"] += 1
        finally:
            try:
                await ws.close()
            except Exception:
                pass
        per["finished_at_utc"] = _utc_iso()
        return per

    results = await asyncio.gather(
        *[run_one(i, b) for i, b in enumerate(buckets)]
    )
    record["per_connection"] = list(results)

    agg_inbound = {}
    agg_rate_errs = 0
    agg_disconnects = 0
    for r in results:
        for k, v in r.get("inbound_count_by_type", {}).items():
            agg_inbound[k] = agg_inbound.get(k, 0) + v
        agg_rate_errs += r.get("rate_limit_errors", 0)
        agg_disconnects += r.get("disconnect_events", 0)
    record["aggregate_totals"] = {
        "inbound_by_type": agg_inbound,
        "rate_limit_errors_total": agg_rate_errs,
        "disconnect_events_total": agg_disconnects,
    }
    record["finished_at_utc"] = _utc_iso()
    return record


# ============================================================
# MAIN
# ============================================================

async def run(args: argparse.Namespace) -> int:
    started_at_utc = _utc_iso()
    started_monotonic = time.monotonic()
    out_path = args.out or _default_out_path(args.phase)

    try:
        universe = fetch_universe()
    except Exception as exc:
        err_payload = {
            "tool": "kraken_ws_probe",
            "tool_version": "dv1_0",
            "phase": args.phase,
            "started_at_utc": started_at_utc,
            "finished_at_utc": _utc_iso(),
            "error": f"fetch_universe_failed: {exc!r}",
        }
        write_json_output(out_path, err_payload)
        print(
            f"FATAL: AssetPairs fetch failed: {exc!r}",
            file=sys.stderr,
        )
        return 1

    try:
        if args.phase == "A":
            phase_record = await phase_a(universe)
        elif args.phase == "B":
            phase_record = await phase_b(
                universe,
                duration_s=args.duration,
                max_symbols=args.max_symbols,
            )
        elif args.phase == "C":
            phase_record = await phase_c(universe)
        elif args.phase == "D":
            phase_record = await phase_d(universe, duration_s=args.duration)
        else:
            print(f"FATAL: unknown phase {args.phase!r}", file=sys.stderr)
            return 1
    except Exception as exc:
        err_payload = {
            "tool": "kraken_ws_probe",
            "tool_version": "dv1_0",
            "phase": args.phase,
            "started_at_utc": started_at_utc,
            "finished_at_utc": _utc_iso(),
            "pre_phase": universe,
            "error": f"phase_dispatch_failed: {exc!r}",
        }
        write_json_output(out_path, err_payload)
        print(
            f"FATAL: phase {args.phase} dispatch failed: {exc!r}",
            file=sys.stderr,
        )
        return 1

    payload = {
        "tool": "kraken_ws_probe",
        "tool_version": "dv1_0",
        "phase": args.phase,
        "started_at_utc": started_at_utc,
        "finished_at_utc": _utc_iso(),
        "elapsed_s": time.monotonic() - started_monotonic,
        "args": {
            "phase": args.phase,
            "duration": args.duration,
            "max_symbols": args.max_symbols,
            "out": args.out,
        },
        "spec_refs": {
            "ws_endpoint": WS_PUBLIC_URI,
            "ws_lib_params": "0411001 dv1_11 WS-LIB-002 / -003 / -004",
            "subscribe_format": "0411001 dv1_11 Section 4.3 / 4.4",
            "decisions": [
                "D-03 USD+USDC+USDT no cap",
                "D-08 Top-N retired",
                "D-06 paper-to-live parity (probe is non-trading)",
            ],
            "ledger_questions_resolved": {
                "A": ["Q5", "Q7"],
                "B": ["Q6", "Q7", "Q8"],
                "C": ["Q6_reconnect_arm", "Q7"],
                "D": ["P2_viability", "Q7"],
            }.get(args.phase, []),
        },
        "pre_phase": universe,
        "phase_record": phase_record,
    }

    write_json_output(out_path, payload)
    print(f"OK: wrote {out_path}")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="kraken_ws_probe",
        description=(
            "Empirical Kraken WS v2 subscribe rate-limit probe "
            "(non-trading; public WS read-only). Resolves "
            "analysis ledger Q5 / Q6 / Q7 / Q8 per TB00140 NSI "
            "v1_1 Section 5."
        ),
    )
    p.add_argument(
        "--phase",
        required=True,
        choices=["A", "B", "C", "D"],
        help=(
            "A=burst (Q5 per-message symbol-count cap). "
            "B=steady-state (Q6 + Q8). "
            "C=reconnect (Q6 reconnect arm). "
            "D=multi-connection (P2 viability)."
        ),
    )
    p.add_argument(
        "--out",
        default=None,
        help=(
            "Output JSON path. Defaults to "
            "/tmp/kraken_ws_probe_<phase>_<UTC-isoformat>.json"
        ),
    )
    p.add_argument(
        "--duration",
        type=int,
        default=PHASE_B_DEFAULT_DURATION_S,
        help=(
            "Phase B and D duration in seconds (default 7200 = "
            "2 hours)."
        ),
    )
    p.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help=(
            "Phase B optional cap on subscribed symbols. Use "
            "the largest successful N from a prior Phase A run "
            "if universe size exceeds it."
        ),
    )
    return p.parse_args()


def _install_signal_handlers(loop) -> None:
    """Best-effort SIGTERM / SIGINT handler installation. On
    Windows asyncio loops add_signal_handler is not supported,
    in which case we fall back to default Ctrl-C handling.
    """
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda: [t.cancel() for t in asyncio.all_tasks(loop)],
            )
    except (NotImplementedError, AttributeError, RuntimeError):
        pass


async def _main_async(args: argparse.Namespace) -> int:
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)
    return await run(args)


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

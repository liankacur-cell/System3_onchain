#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║        SYSTEM3 v1.0.1 — FUTURES CORE ENGINE                ║
║        Derivatives-First Decision Model                    ║
║        STABILITY MODE — LOCKED                             ║
║        + Trending Scanner + EMA                            ║
║        Target: Termux Android | Single File                ║
╚══════════════════════════════════════════════════════════════╝
"""

import requests
import json
import time
import os
import random
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ─── Load .env ─────────────────────────────────────
def load_env():
    env_path = Path(__file__).parent / ".env"

    if not env_path.exists():
        print("[ENV] .env NOT FOUND")
        return

    print("[ENV] loading:", env_path)

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, val = line.split("=", 1)
        os.environ[key.strip()] = val.strip()

load_env()

# ─── Validasi Telegram ─────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise Exception("TELEGRAM ENV NOT COMPLETE")

print("[TELEGRAM] BOT OK")
print("[TELEGRAM] CHAT:", TELEGRAM_CHAT_ID)

try:
    import websocket
except ImportError:
    websocket = None

# ═══════════════════════════════════════════════════════════════
# 0. KONFIGURASI
# ═══════════════════════════════════════════════════════════════

APP_CONFIG = {
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID"),
}

CONFIG = {
    "version": "1.0.1",
    "cycle_minutes": 45,
    "leverage_safe_zone": [2, 5],
    "tp_long": [3, 6, 10],
    "sl_long": 5,
    "tp_short": [3, 6, 10],
    "sl_short": 5,
    "score_threshold_strong": 75,
    "score_threshold_weak": 50,
    "archive_retention_days": 90,
    "min_volume_24h": 1_000_000,
    "dead_zone_low": 48,
    "dead_zone_high": 52,
    "override_max_score_change": 5,
    "telegram_signal_min_score": 70,
    "cache_ttl_seconds": 2,
    "spike_threshold": 6,
    "github_repo_path": os.path.expanduser("~/System3_onchain"),
    
    "enable_trending_scanner": True,
    "trending_limit": 20,
    "trending_min_volume": 10_000_000,
}

CYCLE_INTERVAL = CONFIG["cycle_minutes"] * 60

PAIR_UNIVERSE_CORE = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "SUIUSDT", "DOGEUSDT", "UNIUSDT", "ZECUSDT"
]

SIGNAL_BUFFER = []
WS_RUNNING = {}

# ═══════════════════════════════════════════════════════════════
# SANITIZER GLOBAL
# ═══════════════════════════════════════════════════════════════

def normalize(x):
    if isinstance(x, dict):
        try:
            return float(next(iter(x.values())))
        except:
            return 0.0
    try:
        return float(x)
    except:
        return 0.0

# ═══════════════════════════════════════════════════════════════
# SAFE REQUEST LAYER
# ═══════════════════════════════════════════════════════════════

class SafeRequest:
    
    @staticmethod
    def get(url, params=None, retries=3):
        for _ in range(retries):
            try:
                r = requests.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    return r
            except:
                time.sleep(0.3 + random.random())
        return None
    
    @staticmethod
    def post(url, json_data=None, retries=3):
        for _ in range(retries):
            try:
                r = requests.post(url, json=json_data, timeout=10)
                if r.status_code == 200:
                    return r
            except:
                time.sleep(0.3 + random.random())
        return None

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET CACHE LAYER
# ═══════════════════════════════════════════════════════════════

class WSCache:
    CACHE = {}

    @staticmethod
    def update(symbol, data):
        WSCache.CACHE[symbol] = {
            "t": time.time(),
            "v": {
                "last_price": normalize(data["last_price"]),
                "price_change_pct": normalize(data["price_change_pct"]),
                "volume": normalize(data["volume"])
            }
        }

    @staticmethod
    def get(symbol):
        entry = WSCache.CACHE.get(symbol)
        if entry:
            return entry.get("v")
        return None

def ws_ticker_stream(symbol):
    global WS_RUNNING

    if websocket is None:
        return

    if WS_RUNNING.get(symbol):
        return

    WS_RUNNING[symbol] = True

    stream = symbol.lower() + "@ticker"
    url = f"wss://stream.binance.com:9443/ws/{stream}"

    def on_message(ws, message):
        try:
            data = json.loads(message)
            WSCache.update(symbol, {
                "last_price": normalize(data["c"]),
                "price_change_pct": normalize(data["P"]),
                "volume": normalize(data["v"])
            })
        except:
            pass

    def on_error(ws, error):
        pass

    def on_close(ws, close_status_code, close_msg):
        WS_RUNNING[symbol] = False
        time.sleep(5)
        threading.Thread(
            target=ws_ticker_stream,
            args=(symbol,),
            daemon=True
        ).start()

    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

# ═══════════════════════════════════════════════════════════════
# DATA GATEWAY LAYER
# ═══════════════════════════════════════════════════════════════

class DataGateway:
    CACHE = {}
    TTL = CONFIG["cache_ttl_seconds"]

    @staticmethod
    def get(symbol, fetch_func):
        ws_data = WSCache.get(symbol)
        if ws_data is not None:
            return ws_data

        now = time.time()
        if symbol in DataGateway.CACHE:
            item = DataGateway.CACHE[symbol]
            if now - item["t"] < DataGateway.TTL:
                return item["v"]

        value = fetch_func(symbol)
        if value is None:
            return None

        DataGateway.CACHE[symbol] = {"t": now, "v": value}
        return value

# ═══════════════════════════════════════════════════════════════
# SPIKE MEMORY LAYER
# ═══════════════════════════════════════════════════════════════

class SpikeMemory:
    HISTORY = {}

    @staticmethod
    def update(symbol, score):
        if symbol not in SpikeMemory.HISTORY:
            SpikeMemory.HISTORY[symbol] = []

        score = normalize(score)

        SpikeMemory.HISTORY[symbol].append({
            "t": time.time(),
            "s": score
        })

        SpikeMemory.HISTORY[symbol] = SpikeMemory.HISTORY[symbol][-5:]

    @staticmethod
    def is_confirmed_spike(symbol):
        history = SpikeMemory.HISTORY.get(symbol, [])
        if len(history) < 3:
            return False
        return sum(h["s"] > CONFIG["spike_threshold"] for h in history) >= 2

# ═══════════════════════════════════════════════════════════════
# INTEGRATION LAYER — Telegram
# ═══════════════════════════════════════════════════════════════

class Telegram:
    BOT_TOKEN = APP_CONFIG["telegram_bot_token"]
    CHAT_ID = APP_CONFIG["telegram_chat_id"]

    @staticmethod
    def send(text):
        try:
            url = f"https://api.telegram.org/bot{Telegram.BOT_TOKEN}/sendMessage"

            r = requests.post(url, json={
                "chat_id": Telegram.CHAT_ID,
                "text": text
            }, timeout=10)

            data = r.json()

            if r.status_code == 200 and data.get("ok"):
                print("✔ TELEGRAM PUSH SUCCESS")
                return True

            print("✖ TELEGRAM FAILED:", data)
            return False

        except Exception as e:
            print("✖ TELEGRAM ERROR:", e)
            return False

class TelegramSummary:
    cycle_count = 0

    @staticmethod
    def send(summary):
        TelegramSummary.cycle_count += 1

        if summary["long"] == 0 and summary["short"] == 0:
            signal_text = "💡 Tidak ada signal valid pada sesi ini"
        else:
            signal_text = f"🟢 LONG: {summary['long']}\n🔴 SHORT: {summary['short']}"

        text = (
            f"📊 SYSTEM3 v{CONFIG['version']}\n"
            f"━━━━━━━━━━━━━━\n"
            f"✅ Cycle #{TelegramSummary.cycle_count} selesai\n"
            f"🔍 Scanned: {summary['scanned']} pair\n"
            f"{signal_text}\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        Telegram.send(text)

# ═══════════════════════════════════════════════════════════════
# INTEGRATION LAYER — GitHub Logger
# ═══════════════════════════════════════════════════════════════

class GitHubSync:
    REPO_PATH = CONFIG["github_repo_path"]

    @staticmethod
    def push(message="system3 update"):
        try:
            subprocess.run(["git", "add", "-A"], cwd=GitHubSync.REPO_PATH, check=True)

            commit = subprocess.run(
                ["git", "commit", "-m", message],
                cwd=GitHubSync.REPO_PATH,
                capture_output=True,
                text=True
            )

            if "nothing to commit" in commit.stdout.lower():
                print("[GIT] nothing to commit, skipping commit step")

            print("[GIT] pushing via SSH...")

            result = subprocess.run(
                ["git", "push", "origin", "main"],
                cwd=GitHubSync.REPO_PATH,
                capture_output=True,
                text=True
            )

            print("[GIT STDOUT]", result.stdout)
            print("[GIT STDERR]", result.stderr)

            if result.returncode == 0:
                print("✔ GITHUB PUSH SUCCESS")
                return True

            print("✖ GITHUB PUSH FAILED")
            return False

        except subprocess.CalledProcessError as e:
            print("✖ GIT ERROR:", str(e))
            return False

# ═══════════════════════════════════════════════════════════════
# INTEGRATION LAYER — Blockchain RPC
# ═══════════════════════════════════════════════════════════════

class RPC:
    BSC = "https://bsc-dataseed.binance.org"

    @staticmethod
    def context():
        try:
            r = SafeRequest.post(RPC.BSC, json_data={
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            })
            if r:
                return {
                    "block": int(r.json()["result"], 16),
                    "status": "active"
                }
        except:
            pass
        return {"block": None, "status": "down"}

# ═══════════════════════════════════════════════════════════════
# 1. DATA INGESTION
# ═══════════════════════════════════════════════════════════════

class DataIngestion:

    BASE_URL_BINANCE = "https://api.binance.com"

    @staticmethod
    def fetch_24hr_ticker(symbol):
        url = f"{DataIngestion.BASE_URL_BINANCE}/api/v3/ticker/24hr"
        r = SafeRequest.get(url, params={"symbol": symbol})
        if r:
            try:
                data = r.json()
                return {
                    "last_price": normalize(data["lastPrice"]),
                    "price_change_pct": normalize(data["priceChangePercent"]),
                    "volume": normalize(data["quoteVolume"]),
                }
            except:
                pass
        return None

    @staticmethod
    def fetch_klines(symbol, interval, limit=100):
        url = f"{DataIngestion.BASE_URL_BINANCE}/api/v3/klines"
        r = SafeRequest.get(url, params={"symbol": symbol, "interval": interval, "limit": limit})
        if r:
            try:
                data = r.json()
                candles = []
                for c in data:
                    candles.append({
                        "open": normalize(c[1]),
                        "high": normalize(c[2]),
                        "low": normalize(c[3]),
                        "close": normalize(c[4]),
                        "volume": normalize(c[5]),
                        "time": c[0],
                    })
                return candles
            except:
                pass
        return None

    @staticmethod
    def fetch_all_klines(symbol):
        tfs = ["5m", "15m", "30m", "1h"]
        results = {}

        def fetch(tf):
            return tf, DataIngestion.fetch_klines(symbol, tf)

        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = [ex.submit(fetch, tf) for tf in tfs]
            for f in futures:
                tf, data = f.result()
                results[tf] = data

        return results

    @staticmethod
    def fetch_open_interest(symbol):
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        r = SafeRequest.get(url, params={"symbol": symbol})
        if r:
            try:
                return normalize(r.json()["openInterest"])
            except:
                pass
        return None

    @staticmethod
    def fetch_funding_rate(symbol):
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        r = SafeRequest.get(url, params={"symbol": symbol, "limit": 1})
        if r:
            try:
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    return normalize(data[0]["fundingRate"]) * 100
            except:
                return None
        return None

# ═══════════════════════════════════════════════════════════════
# TRENDING SCANNER
# ═══════════════════════════════════════════════════════════════

class TrendingScanner:

    @staticmethod
    def get_top_trending(limit=20):

        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"

        r = SafeRequest.get(url)

        if not r:
            return []

        try:
            data = r.json()

            candidates = []

            for item in data:

                symbol = item.get("symbol", "")

                if not symbol.endswith("USDT"):
                    continue

                volume = normalize(item.get("quoteVolume"))

                if volume < CONFIG["trending_min_volume"]:
                    continue

                change_pct = abs(
                    normalize(item.get("priceChangePercent"))
                )

                score = volume * 0.4 + change_pct * 100000

                candidates.append(
                    (symbol, score)
                )

            candidates.sort(
                key=lambda x: x[1],
                reverse=True
            )

            return [
                x[0]
                for x in candidates[:limit]
            ]

        except:
            return []

# ═══════════════════════════════════════════════════════════════
# MARKET FILTER LAYER
# ═══════════════════════════════════════════════════════════════

class MarketFilter:

    @staticmethod
    def spike_check(symbol, ticker, candles):
        if not ticker or not candles or len(candles) < 5:
            return False, 0

        price_move = normalize(ticker["price_change_pct"])

        closes = [normalize(c["close"]) for c in candles[-5:]]
        avg_close = sum(closes) / len(closes) if closes else 0
        vol = (max(closes) - min(closes)) / avg_close * 100 if avg_close > 0 else 0
        vol = normalize(vol)

        score = price_move * 0.6 + vol * 0.4

        if score > CONFIG["spike_threshold"]:
            SpikeMemory.update(symbol, score)
            return True, score

        return False, score

# ═══════════════════════════════════════════════════════════════
# 2. DERIVATIVES INTELLIGENCE
# ═══════════════════════════════════════════════════════════════

class DerivativesIntelligence:

    @staticmethod
    def analyze(oi_current, oi_previous, funding_rate, price_change_pct):
        
        oi_current = normalize(oi_current)
        oi_previous = normalize(oi_previous)
        funding_rate = normalize(funding_rate)
        price_change_pct = normalize(price_change_pct)
        
        if oi_previous > 0 and oi_current > 0:
            oi_change_pct = ((oi_current - oi_previous) / oi_previous) * 100
        else:
            oi_change_pct = 0
        
        if oi_change_pct > 2 and price_change_pct > 0:
            oi_trend = "rising_bullish"
        elif oi_change_pct > 2 and price_change_pct < 0:
            oi_trend = "rising_bearish"
        elif oi_change_pct < -2:
            oi_trend = "declining"
        else:
            oi_trend = "stable"
        
        funding_status = "neutral"
        if funding_rate > 0.05:
            funding_status = "long_crowded"
        elif funding_rate < -0.05:
            funding_status = "short_crowded"
        
        risk_level = "low"
        if abs(oi_change_pct) > 5:
            risk_level = "high"
        elif abs(oi_change_pct) > 2:
            risk_level = "medium"
        
        if oi_trend == "rising_bullish" and funding_status != "long_crowded":
            derivatives_signal = "bullish"
        elif oi_trend == "rising_bearish" and funding_status != "short_crowded":
            derivatives_signal = "bearish"
        elif oi_trend == "declining" and price_change_pct < 0:
            derivatives_signal = "bearish"
        elif oi_trend == "declining" and price_change_pct > 0:
            derivatives_signal = "bullish"
        else:
            derivatives_signal = "neutral"
        
        crowd_trap = False
        if funding_status == "long_crowded" and price_change_pct > 5:
            crowd_trap = True
        if funding_status == "short_crowded" and price_change_pct < -5:
            crowd_trap = True
        
        if abs(price_change_pct) > 5 and abs(oi_change_pct) < 1:
            derivatives_signal = "neutral"
            crowd_trap = True
        
        score = 50
        
        if oi_trend == "rising_bullish":
            score += 20
        elif oi_trend == "rising_bearish":
            score -= 20
        elif oi_trend == "declining":
            if price_change_pct > 0:
                score -= 10
            else:
                score += 10
        
        if funding_status == "long_crowded":
            score -= 15
        elif funding_status == "short_crowded":
            score += 15
        
        if risk_level == "high":
            score -= 10
        
        if crowd_trap:
            score -= 20
        
        score = max(0, min(100, score))
        
        return {
            "oi_trend": oi_trend,
            "oi_change_pct": round(oi_change_pct, 2),
            "funding_status": funding_status,
            "funding_rate": round(funding_rate, 4),
            "risk_level": risk_level,
            "derivatives_signal": derivatives_signal,
            "crowd_trap": crowd_trap,
            "score": score,
        }


# ═══════════════════════════════════════════════════════════════
# 3. TIMEFRAME STRUCTURE ENGINE (EMA)
# ═══════════════════════════════════════════════════════════════

class TimeframeEngine:

    @staticmethod
    def calculate_ema(values, period):
        if len(values) < period:
            return values[-1]

        multiplier = 2 / (period + 1)
        ema = values[0]

        for price in values[1:]:
            ema = ((price - ema) * multiplier) + ema

        return ema

    @staticmethod
    def analyze_candles(candles):
        if not candles or len(candles) < 20:
            return {"direction": "neutral", "strength": 0}

        closes = [normalize(c["close"]) for c in candles]

        ema20 = TimeframeEngine.calculate_ema(closes, 20)
        ema50 = TimeframeEngine.calculate_ema(closes, 50)

        if ema20 > ema50 * 1.003:
            direction = "bullish"
        elif ema20 < ema50 * 0.997:
            direction = "bearish"
        else:
            direction = "neutral"

        recent_high = max(closes[-10:])
        recent_low = min(closes[-10:])
        if recent_high != recent_low:
            position_pct = (closes[-1] - recent_low) / (recent_high - recent_low) * 100
        else:
            position_pct = 50

        if direction == "bullish":
            strength = min(position_pct + 30, 100)
        elif direction == "bearish":
            strength = min((100 - position_pct) + 30, 100)
        else:
            strength = 50

        return {
            "direction": direction,
            "strength": round(strength),
        }

    @staticmethod
    def analyze_all_timeframes(kline_data):
        results = {}
        for tf in ["5m", "15m", "30m", "1h"]:
            if tf in kline_data:
                results[tf] = TimeframeEngine.analyze_candles(kline_data[tf])
            else:
                results[tf] = {"direction": "neutral", "strength": 0}
        return results

    @staticmethod
    def calculate_structure_score(tf_results):
        directions = [tf_results[tf]["direction"] for tf in ["5m", "15m", "30m", "1h"]]
        bullish_count = directions.count("bullish")
        bearish_count = directions.count("bearish")

        if bullish_count >= 3:
            return 75 + bullish_count * 5
        elif bearish_count >= 3:
            return 25 - bearish_count * 5
        elif directions.count("neutral") >= 3:
            return 45
        else:
            return 50


# ═══════════════════════════════════════════════════════════════
# 4. VOLATILITY FILTER
# ═══════════════════════════════════════════════════════════════

class VolatilityFilter:

    @staticmethod
    def calculate_atr(candles, period=14):
        if len(candles) < period + 1:
            return normalize(candles[-1]["high"]) - normalize(candles[-1]["low"]) if candles else 0

        true_ranges = []
        for i in range(1, len(candles)):
            high = normalize(candles[i]["high"])
            low = normalize(candles[i]["low"])
            prev_close = normalize(candles[i-1]["close"])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)

        if len(true_ranges) >= period:
            return sum(true_ranges[-period:]) / period
        return sum(true_ranges) / len(true_ranges) if true_ranges else 0

    @staticmethod
    def analyze(candles_1h):
        if not candles_1h or len(candles_1h) < 20:
            return {"state": "normal", "block": False, "score": 50}

        closes = [normalize(c["close"]) for c in candles_1h]
        atr = VolatilityFilter.calculate_atr(candles_1h)
        avg_price = sum(closes[-10:]) / 10
        volatility_ratio = (atr / avg_price) * 100 if avg_price > 0 else 0

        if volatility_ratio > 3.0:
            state = "extreme"
            block = True
            score = 30
        elif volatility_ratio > 2.0:
            state = "high"
            block = False
            score = 40
        elif volatility_ratio > 1.0:
            state = "normal"
            block = False
            score = 60
        else:
            state = "low"
            block = False
            score = 50

        return {
            "state": state,
            "volatility_ratio": round(volatility_ratio, 2),
            "block": block,
            "score": score,
        }


# ═══════════════════════════════════════════════════════════════
# 5. CONFLUENCE SCORING
# ═══════════════════════════════════════════════════════════════

class ConfluenceScoring:

    @staticmethod
    def calculate(derivatives_score, structure_score, volatility_score):
        final = (
            normalize(derivatives_score) * 0.45
            + normalize(structure_score) * 0.40
            + normalize(volatility_score) * 0.15
        )
        final = max(0, min(100, final))
        return int(final)


# ═══════════════════════════════════════════════════════════════
# 6. DECISION ENGINE (3-Layer)
# ═══════════════════════════════════════════════════════════════

class DecisionEngine:

    @staticmethod
    def decide(final_score, raw_score, tf_1h_direction, derivatives, volatility_block):
        
        if volatility_block:
            return "NO TRADE", "Extreme volatility block"
        
        if derivatives["crowd_trap"]:
            return "NO TRADE", "Crowd trap detected"
        
        if CONFIG["dead_zone_low"] <= raw_score <= CONFIG["dead_zone_high"]:
            return "NO TRADE", "Dead zone lock"
        
        funding_status = derivatives["funding_status"]
        oi_change_pct = derivatives["oi_change_pct"]
        risk_level = derivatives["risk_level"]
        
        if funding_status == "long_crowded":
            if tf_1h_direction != "bearish" or final_score < 70:
                return "NO TRADE", "Long crowded, no long allowed"
        
        if funding_status == "short_crowded":
            if tf_1h_direction != "bullish" or final_score < 70:
                return "NO TRADE", "Short crowded, no short allowed"
        
        if oi_change_pct > 5 and tf_1h_direction == "bullish":
            return "NO TRADE", "OI spike long trap risk"
        
        if oi_change_pct < -5 and tf_1h_direction == "bearish":
            return "NO TRADE", "OI drop short trap risk"
        
        if risk_level == "high" and final_score < 80:
            return "NO TRADE", "High risk, score insufficient"
        
        if final_score < CONFIG["score_threshold_weak"]:
            return "NO TRADE", f"Score rendah ({final_score})"
        
        if final_score < CONFIG["score_threshold_strong"]:
            return "NO TRADE", f"Sinyal lemah ({final_score})"
        
        if tf_1h_direction == "neutral":
            return "NO TRADE", "TF 1H neutral"
        
        if tf_1h_direction == "bullish":
            direction = "LONG"
        elif tf_1h_direction == "bearish":
            direction = "SHORT"
        else:
            return "NO TRADE", "Arah tidak jelas"
        
        return direction, f"Confirmed ({final_score})"


# ═══════════════════════════════════════════════════════════════
# 7. RISK & TP/SL ENGINE
# ═══════════════════════════════════════════════════════════════

class RiskEngine:

    @staticmethod
    def calculate_levels(entry_price, direction):
        entry_price = normalize(entry_price)
        
        if direction == "LONG":
            tp1 = entry_price * (1 + 3/100)
            tp2 = entry_price * (1 + 6/100)
            tp3 = entry_price * (1 + 10/100)
            sl = entry_price * (1 - 5/100)
            tp_pct = CONFIG["tp_long"]
            sl_pct = CONFIG["sl_long"]
        elif direction == "SHORT":
            tp1 = entry_price * (1 - 3/100)
            tp2 = entry_price * (1 - 6/100)
            tp3 = entry_price * (1 - 10/100)
            sl = entry_price * (1 + 5/100)
            tp_pct = CONFIG["tp_short"]
            sl_pct = CONFIG["sl_short"]
        else:
            return None

        return {
            "entry_price": round(entry_price, 4),
            "tp1": round(tp1, 4),
            "tp2": round(tp2, 4),
            "tp3": round(tp3, 4),
            "sl": round(sl, 4),
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
        }

    @staticmethod
    def generate_entry_zones(candles_5m, candles_15m, candles_30m, direction):
        if not candles_5m or not candles_15m or not candles_30m:
            return None

        current_price = normalize(candles_5m[-1]["close"])
        atr_15m = RiskEngine._calc_atr(candles_15m)
        atr_30m = RiskEngine._calc_atr(candles_30m)

        if direction == "LONG":
            return {
                "entry_1_breakout": round(current_price, 4),
                "entry_2_pullback": round(current_price - atr_15m * 0.5, 4),
                "entry_3_deep": round(current_price - atr_30m * 1.0, 4),
            }
        else:
            return {
                "entry_1_breakout": round(current_price, 4),
                "entry_2_pullback": round(current_price + atr_15m * 0.5, 4),
                "entry_3_deep": round(current_price + atr_30m * 1.0, 4),
            }

    @staticmethod
    def _calc_atr(candles, period=14):
        if len(candles) < period + 1:
            return normalize(candles[-1]["high"]) - normalize(candles[-1]["low"]) if candles else 0

        true_ranges = []
        for i in range(1, len(candles)):
            high = normalize(candles[i]["high"])
            low = normalize(candles[i]["low"])
            prev_close = normalize(candles[i-1]["close"])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)

        if len(true_ranges) >= period:
            return sum(true_ranges[-period:]) / period
        return sum(true_ranges) / len(true_ranges) if true_ranges else 0


# ═══════════════════════════════════════════════════════════════
# 8. OI TRACKER
# ═══════════════════════════════════════════════════════════════

class OITracker:

    @staticmethod
    def get_previous_oi(symbol):
        try:
            os.makedirs("cache", exist_ok=True)
            with open(f"cache/oi_{symbol}.json", "r") as f:
                data = json.load(f)
                if time.time() - data.get("timestamp", 0) < 3600:
                    return data.get("oi")
        except (FileNotFoundError, json.JSONDecodeError):
            return None
        except:
            pass
        return None

    @staticmethod
    def save_current_oi(symbol, oi):
        try:
            os.makedirs("cache", exist_ok=True)
            with open(f"cache/oi_{symbol}.json", "w") as f:
                json.dump({
                    "oi": oi,
                    "timestamp": time.time(),
                    "symbol": symbol
                }, f)
        except:
            pass


# ═══════════════════════════════════════════════════════════════
# 9. MAIN SYSTEM3 ENGINE
# ═══════════════════════════════════════════════════════════════

class System3:

    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.state = {}

    def get_state(self, symbol):
        if symbol not in self.state:
            self.state[symbol] = {
                "last_score": 0,
                "last_decision": None,
                "last_time": 0
            }
        return self.state[symbol]

    @staticmethod
    def stability_gate(new, old):
        if abs(new - old) < 2:
            return old
        return new

    def run_cycle(self, symbol="BTCUSDT"):
        print(f"\n{'='*60}")
        print(f"  SYSTEM3 v{CONFIG['version']} (FUTURES CORE) | {self.timestamp}")
        print(f"  PAIR: {symbol}")
        print(f"{'='*60}")

        print("\n[1] DATA GATEWAY...")

        ticker = DataGateway.get(symbol, DataIngestion.fetch_24hr_ticker)
        if not ticker:
            return self._no_trade("ERROR: ticker fetch failed", symbol)

        if normalize(ticker["volume"]) < CONFIG["min_volume_24h"]:
            return self._no_trade("Volume rendah", symbol)

        klines = DataIngestion.fetch_all_klines(symbol)

        if any(not klines.get(tf) or len(klines.get(tf)) < 20 for tf in ["5m", "15m", "30m", "1h"]):
            return self._no_trade("DATA INCOMPLETE SAFE FAIL", symbol)

        print("\n[MARKET FILTER] Spike check...")
        spike, spike_score = MarketFilter.spike_check(symbol, ticker, klines["1h"])
        print(f"    Spike Score: {spike_score:.2f}")
        
        if spike:
            return self._no_trade(f"SPIKE BLOCK {spike_score:.2f}", symbol)

        oi_sekarang = DataGateway.get(symbol, DataIngestion.fetch_open_interest)
        if oi_sekarang is None:
            return self._no_trade("ERROR: OI fetch failed", symbol)

        oi_sebelumnya = OITracker.get_previous_oi(symbol)
        OITracker.save_current_oi(symbol, oi_sekarang)

        funding = DataGateway.get(symbol, DataIngestion.fetch_funding_rate)

        print("\n[2] DERIVATIVES INTELLIGENCE (45%)...")
        deriv = DerivativesIntelligence.analyze(
            oi_current=oi_sekarang,
            oi_previous=oi_sebelumnya,
            funding_rate=funding,
            price_change_pct=ticker["price_change_pct"],
        )
        print(f"    OI Trend    : {deriv['oi_trend']} ({deriv['oi_change_pct']:+.2f}%)")
        print(f"    Funding     : {deriv['funding_status']}")
        print(f"    Risk        : {deriv['risk_level']}")
        print(f"    Crowd Trap  : {deriv['crowd_trap']}")
        print(f"    Deriv Score : {deriv['score']}")

        print("\n[3] TIMEFRAME STRUCTURE (40%)...")
        tf_results = TimeframeEngine.analyze_all_timeframes(klines)
        for tf in ["5m", "15m", "30m", "1h"]:
            r = tf_results[tf]
            print(f"    [{tf}] {r['direction']:8} strength={r['strength']}")
        structure_score = TimeframeEngine.calculate_structure_score(tf_results)
        print(f"    Structure Score: {structure_score}")

        print("\n[4] VOLATILITY FILTER (10%)...")
        vol = VolatilityFilter.analyze(klines["1h"])
        print(f"    State: {vol['state']} ({vol['volatility_ratio']}%)")
        print(f"    Block: {vol['block']}")

        rpc_ctx = RPC.context()
        if rpc_ctx["block"]:
            print(f"\n[RPC] BSC Block: {rpc_ctx['block']}")

        print("\n[5] CONFLUENCE SCORING...")
        raw_score = ConfluenceScoring.calculate(
            derivatives_score=deriv["score"],
            structure_score=structure_score,
            volatility_score=vol["score"],
        )

        state = self.get_state(symbol)
        final_score = int(raw_score * 0.8 + state["last_score"] * 0.2)
        final_score = System3.stability_gate(final_score, state["last_score"])
        print(f"    Raw: {raw_score} | Smoothed: {final_score}")

        print("\n[6] DECISION ENGINE...")
        direction, reason = DecisionEngine.decide(
            final_score=final_score,
            raw_score=raw_score,
            tf_1h_direction=tf_results["1h"]["direction"],
            derivatives=deriv,
            volatility_block=vol["block"],
        )

        if state["last_decision"] is not None:
            age_decay = time.time() - state.get("last_time", 0)
            if abs(final_score - state["last_score"]) < CONFIG["override_max_score_change"] and age_decay < 900:
                direction = state["last_decision"]
                reason = "STABLE OVERRIDE"

        state["last_score"] = final_score
        state["last_decision"] = direction
        state["last_time"] = time.time()

        print(f"    DECISION: {direction} | Reason: {reason}")

        if direction in ["LONG", "SHORT"]:
            print("\n[7] RISK & TP/SL ENGINE...")
            levels = RiskEngine.calculate_levels(ticker["last_price"], direction)
            entries = RiskEngine.generate_entry_zones(
                klines["5m"], klines["15m"], klines["30m"], direction
            )
            if levels:
                print(f"    Entry: {levels['entry_price']}")
                print(f"    TP1: {levels['tp1']} (+{levels['tp_pct'][0]}%)")
                print(f"    TP2: {levels['tp2']} (+{levels['tp_pct'][1]}%)")
                print(f"    TP3: {levels['tp3']} (+{levels['tp_pct'][2]}%)")
                print(f"    SL:  {levels['sl']} (-{levels['sl_pct']}%)")
        else:
            levels = None
            entries = None

        output = {
            "timestamp": self.timestamp,
            "symbol": symbol,
            "direction": direction,
            "final_score": final_score,
            "price": ticker["last_price"],
            "derivatives": deriv,
            "tf_results": {tf: {"direction": r["direction"], "strength": r["strength"]}
                          for tf, r in tf_results.items()},
            "volatility": vol,
            "rpc": rpc_ctx,
            "levels": levels,
            "entries": entries,
            "reason": reason,
        }

        if "levels" not in output:
            output["levels"] = None
        if "entries" not in output:
            output["entries"] = None

        self._display_signal(output)
        self._save_signal(output)
        
        SIGNAL_BUFFER.append(output)
        
        return output

    def _no_trade(self, reason, symbol):
        output = {
            "timestamp": self.timestamp,
            "symbol": symbol,
            "direction": "NO TRADE",
            "reason": reason,
            "final_score": 0,
        }
        print(f"\n  → NO TRADE: {reason}")
        return output

    def _display_signal(self, output):
        print(f"\n{'─'*50}")

        if output["direction"] == "NO TRADE":
            print(f"  ⚪ NO TRADE - {output['symbol']}")
            print(f"  Reason: {output['reason']}")
        elif output["direction"] == "LONG":
            emoji = "🟢"
            levels = output.get("levels", {})

            print(f"  {emoji} SINYAL {output['direction']} - {output['symbol']}")
            print(f"  {'─'*40}")
            print(f"  Entry    : {levels.get('entry_price', 'N/A')}")
            print(f"  TP1      : {levels.get('tp1', 'N/A')}")
            print(f"  TP2      : {levels.get('tp2', 'N/A')}")
            print(f"  TP3      : {levels.get('tp3', 'N/A')}")
            print(f"  SL       : {levels.get('sl', 'N/A')}")
            print(f"  Score    : {output['final_score']}/100")

            msg = (
                f"🟢 *LONG SIGNAL*\n"
                f"━━━━━━━━━━━━━━\n"
                f"💱 Pair: {output['symbol']}\n"
                f"💰 Entry: {levels.get('entry_price', 'N/A')}\n"
                f"🎯 TP1: {levels.get('tp1', 'N/A')}\n"
                f"🎯 TP2: {levels.get('tp2', 'N/A')}\n"
                f"🎯 TP3: {levels.get('tp3', 'N/A')}\n"
                f"🛑 SL: {levels.get('sl', 'N/A')}\n"
                f"⚡ Score: {output['final_score']}/100\n"
                f"📝 Reason: {output.get('reason','')}"
            )
            Telegram.send(msg)
        elif output["direction"] == "SHORT":
            emoji = "🔴"
            levels = output.get("levels", {})

            print(f"  {emoji} SINYAL {output['direction']} - {output['symbol']}")
            print(f"  {'─'*40}")
            print(f"  Entry    : {levels.get('entry_price', 'N/A')}")
            print(f"  TP1      : {levels.get('tp1', 'N/A')}")
            print(f"  TP2      : {levels.get('tp2', 'N/A')}")
            print(f"  TP3      : {levels.get('tp3', 'N/A')}")
            print(f"  SL       : {levels.get('sl', 'N/A')}")
            print(f"  Score    : {output['final_score']}/100")

            msg = (
                f"🔴 *SHORT SIGNAL*\n"
                f"━━━━━━━━━━━━━━\n"
                f"💱 Pair: {output['symbol']}\n"
                f"💰 Entry: {levels.get('entry_price', 'N/A')}\n"
                f"🎯 TP1: {levels.get('tp1', 'N/A')}\n"
                f"🎯 TP2: {levels.get('tp2', 'N/A')}\n"
                f"🎯 TP3: {levels.get('tp3', 'N/A')}\n"
                f"🛑 SL: {levels.get('sl', 'N/A')}\n"
                f"⚡ Score: {output['final_score']}/100\n"
                f"📝 Reason: {output.get('reason','')}"
            )
            Telegram.send(msg)

        print(f"{'─'*50}\n")

    def _save_signal(self, output):
        os.makedirs("signals", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        os.makedirs("daily", exist_ok=True)
        os.makedirs("archive", exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"signals/signal_{timestamp}_{output['symbol']}.json"

        with open(filename, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"  [SAVED] {filename}")


# ═══════════════════════════════════════════════════════════════
# 10. MAIN EXECUTION LOOP
# ═══════════════════════════════════════════════════════════════

def flush_github():
    os.makedirs("batch", exist_ok=True)

    with open("batch/cycle.json", "w") as f:
        json.dump(SIGNAL_BUFFER, f, indent=2, default=str)

    telegram_ok = Telegram.send("SYSTEM3 cycle completed")
    git_ok = GitHubSync.push("SYSTEM3 cycle update")

    print("\n================ CHECKLIST ================")
    print(f"✔ TELEGRAM : {'SUCCESS' if telegram_ok else 'FAILED'}")
    print(f"✔ GITHUB   : {'SUCCESS' if git_ok else 'FAILED'}")
    print("==========================================\n")

def main():
    global SIGNAL_BUFFER
    
    subprocess.run(
        ["git", "remote", "set-url", "origin",
         "git@github.com:liankacur-cell/System3_onchain.git"],
        cwd=CONFIG["github_repo_path"]
    )
    
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   SYSTEM3 v1.0.1 — FUTURES CORE ENGINE                 ║")
    print("║   + Trending Scanner + EMA                             ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    print("▸ Starting WebSocket streams...")
    for symbol in PAIR_UNIVERSE_CORE:
        threading.Thread(
            target=ws_ticker_stream,
            args=(symbol,),
            daemon=True
        ).start()
    time.sleep(2)
    print("  WebSocket streams active.")
    print()

    next_cycle = time.time() + CYCLE_INTERVAL

    while True:
        SIGNAL_BUFFER = []
        
        if CONFIG["enable_trending_scanner"]:
            trending_pairs = TrendingScanner.get_top_trending(
                CONFIG["trending_limit"]
            )
        else:
            trending_pairs = []
        
        scan_pairs = list(
            dict.fromkeys(
                PAIR_UNIVERSE_CORE + trending_pairs
            )
        )
        
        print("\nTRENDING PAIRS:")
        print(trending_pairs)
        
        system3 = System3()
        total_signals = 0
        long_count = 0
        short_count = 0
        no_trade_count = 0

        print("▸ SCANNING PAIRS")
        print("─" * 60)
        for i, symbol in enumerate(scan_pairs, 1):
            print(f"\n[{i}/{len(scan_pairs)}] Processing {symbol}...")
            try:
                result = system3.run_cycle(symbol)
                total_signals += 1
                if result["direction"] == "LONG":
                    long_count += 1
                elif result["direction"] == "SHORT":
                    short_count += 1
                else:
                    no_trade_count += 1
                time.sleep(1)
            except Exception as e:
                print(f"  [ERROR] {symbol}: {e}")

        cycle_summary = {
            "scanned": total_signals,
            "long": long_count,
            "short": short_count,
        }

        print("\n" + "="*60)
        print("  CYCLE COMPLETE")
        print("="*60)
        print(f"  LONG     : {long_count}")
        print(f"  SHORT    : {short_count}")
        print(f"  NO TRADE : {no_trade_count}")
        print("="*60)

        TelegramSummary.send(cycle_summary)
        flush_github()

        sleep_time = next_cycle - time.time()
        next_cycle += CYCLE_INTERVAL

        if sleep_time > 0:
            print(f"\n⏳ Waiting next cycle: {int(sleep_time)} seconds")
            time.sleep(sleep_time)
        else:
            print(f"\n⚠️ Cycle lagging by {abs(sleep_time):.2f}s")


if __name__ == "__main__":
    main()

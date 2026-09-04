#!/usr/bin/env python3
"""
main.py — 股票即時分析 FastAPI 後端（穩定精簡版）
======================================
在你自己的電腦/伺服器上運行（非 Claude 沙盒），因此可以正常連線
Yahoo Finance 取得即時報價與歷史數據。

設計原則：只用 fastapi / yfinance / pandas / numpy / requests / apscheduler
呢幾個成熟穩定嘅套件，唔用 pandas_ta 或 backtrader（呢兩個套件容易同
numpy 版本衝突，之前嘅版本就係卡喺呢度），所有技術指標用 pandas/numpy
手寫計算，安裝更穩陣。

功能：
  1. GET  /api/quote/{ticker}    即時報價
  2. GET  /api/signals/{ticker}  短期 + 中長期買賣訊號 + 圖表數據
  3. GET  /api/backtest/{ticker} 簡化歷史回測（勝率 / 策略報酬 vs 買入持有）
  4. GET  /api/watchlist         查看追蹤清單
  5. POST /api/watchlist         新增/移除追蹤股票
  6. 背景排程：每 N 分鐘掃描追蹤清單，訊號變更時推送 Telegram 通知；
     同時掃描精選72隻股票清單，短線/長期買入/賣出有新股票入選時即刻推送。

啟動：
    pip install -r requirements.txt
    uvicorn main:app --reload
然後打開瀏覽器： http://127.0.0.1:8000
"""

import json
import os
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

STATE_FILE = BASE_DIR / "signal_state.json"
SCAN_STATE_FILE = BASE_DIR / "scan_state.json"
WATCHLIST_FILE = BASE_DIR / "watchlist.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
# 第二個 Telegram Bot（同步接收同一批通知，留空即不會發送）
TELEGRAM_BOT_TOKEN_2 = os.environ.get("TELEGRAM_BOT_TOKEN_2", "")
TELEGRAM_CHAT_ID_2 = os.environ.get("TELEGRAM_CHAT_ID_2", "")
POLL_INTERVAL_MINUTES = int(os.environ.get("POLL_INTERVAL_MINUTES", "30"))

app = FastAPI(title="股票即時訊號分析 API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# 股票代號／名稱對照表 —— 用於代號輸入自動建議 + 建議買入/賣出掃描範圍
# 只係一份精選常見港股／美股清單（非全市場），純為方便搜尋同掃描效能。
# ---------------------------------------------------------------------------
STOCK_UNIVERSE = [
    # ---- 港股 HK ----
    {"ticker": "0700.HK", "name": "騰訊控股", "name_en": "Tencent", "market": "HK"},
    {"ticker": "9988.HK", "name": "阿里巴巴", "name_en": "Alibaba", "market": "HK"},
    {"ticker": "3690.HK", "name": "美團", "name_en": "Meituan", "market": "HK"},
    {"ticker": "1810.HK", "name": "小米集團", "name_en": "Xiaomi", "market": "HK"},
    {"ticker": "9618.HK", "name": "京東集團", "name_en": "JD.com", "market": "HK"},
    {"ticker": "9999.HK", "name": "網易", "name_en": "NetEase", "market": "HK"},
    {"ticker": "9888.HK", "name": "百度集團", "name_en": "Baidu", "market": "HK"},
    {"ticker": "3988.HK", "name": "中國銀行", "name_en": "Bank of China", "market": "HK"},
    {"ticker": "1398.HK", "name": "中國工商銀行", "name_en": "ICBC", "market": "HK"},
    {"ticker": "0939.HK", "name": "中國建設銀行", "name_en": "China Construction Bank", "market": "HK"},
    {"ticker": "1288.HK", "name": "中國農業銀行", "name_en": "Agricultural Bank of China", "market": "HK"},
    {"ticker": "3968.HK", "name": "招商銀行", "name_en": "China Merchants Bank", "market": "HK"},
    {"ticker": "2628.HK", "name": "中國人壽", "name_en": "China Life Insurance", "market": "HK"},
    {"ticker": "2318.HK", "name": "中國平安", "name_en": "Ping An Insurance", "market": "HK"},
    {"ticker": "0941.HK", "name": "中國移動", "name_en": "China Mobile", "market": "HK"},
    {"ticker": "0762.HK", "name": "中國聯通", "name_en": "China Unicom", "market": "HK"},
    {"ticker": "0728.HK", "name": "中國電信", "name_en": "China Telecom", "market": "HK"},
    {"ticker": "0386.HK", "name": "中國石油化工", "name_en": "Sinopec", "market": "HK"},
    {"ticker": "0857.HK", "name": "中國石油股份", "name_en": "PetroChina", "market": "HK"},
    {"ticker": "0883.HK", "name": "中國海洋石油", "name_en": "CNOOC", "market": "HK"},
    {"ticker": "1088.HK", "name": "中國神華", "name_en": "China Shenhua Energy", "market": "HK"},
    {"ticker": "0388.HK", "name": "香港交易所", "name_en": "HKEX", "market": "HK"},
    {"ticker": "0005.HK", "name": "滙豐控股", "name_en": "HSBC", "market": "HK"},
    {"ticker": "1299.HK", "name": "友邦保險", "name_en": "AIA Group", "market": "HK"},
    {"ticker": "0001.HK", "name": "長江和記實業", "name_en": "CK Hutchison", "market": "HK"},
    {"ticker": "0016.HK", "name": "新鴻基地產", "name_en": "Sun Hung Kai Properties", "market": "HK"},
    {"ticker": "0175.HK", "name": "吉利汽車", "name_en": "Geely Auto", "market": "HK"},
    {"ticker": "2015.HK", "name": "理想汽車", "name_en": "Li Auto", "market": "HK"},
    {"ticker": "9866.HK", "name": "蔚來", "name_en": "NIO", "market": "HK"},
    {"ticker": "2382.HK", "name": "舜宇光學科技", "name_en": "Sunny Optical", "market": "HK"},
    {"ticker": "1211.HK", "name": "比亞迪", "name_en": "BYD", "market": "HK"},
    {"ticker": "2020.HK", "name": "安踏體育", "name_en": "Anta Sports", "market": "HK"},
    {"ticker": "0027.HK", "name": "銀河娛樂", "name_en": "Galaxy Entertainment", "market": "HK"},
    {"ticker": "1928.HK", "name": "金沙中國", "name_en": "Sands China", "market": "HK"},
    {"ticker": "0688.HK", "name": "中國海外發展", "name_en": "China Overseas Land", "market": "HK"},
    {"ticker": "6098.HK", "name": "碧桂園服務", "name_en": "Country Garden Services", "market": "HK"},
    # ---- 美股 US ----
    {"ticker": "AAPL", "name": "蘋果", "name_en": "Apple", "market": "US"},
    {"ticker": "MSFT", "name": "微軟", "name_en": "Microsoft", "market": "US"},
    {"ticker": "GOOGL", "name": "谷歌", "name_en": "Alphabet", "market": "US"},
    {"ticker": "AMZN", "name": "亞馬遜", "name_en": "Amazon", "market": "US"},
    {"ticker": "NVDA", "name": "輝達", "name_en": "Nvidia", "market": "US"},
    {"ticker": "META", "name": "Meta平台", "name_en": "Meta Platforms", "market": "US"},
    {"ticker": "TSLA", "name": "特斯拉", "name_en": "Tesla", "market": "US"},
    {"ticker": "AVGO", "name": "博通", "name_en": "Broadcom", "market": "US"},
    {"ticker": "AMD", "name": "超微半導體", "name_en": "AMD", "market": "US"},
    {"ticker": "NFLX", "name": "網飛", "name_en": "Netflix", "market": "US"},
    {"ticker": "BABA", "name": "阿里巴巴(美股)", "name_en": "Alibaba ADR", "market": "US"},
    {"ticker": "JD", "name": "京東(美股)", "name_en": "JD.com ADR", "market": "US"},
    {"ticker": "PDD", "name": "拼多多", "name_en": "PDD Holdings", "market": "US"},
    {"ticker": "NIO", "name": "蔚來(美股)", "name_en": "NIO ADR", "market": "US"},
    {"ticker": "XPEV", "name": "小鵬汽車", "name_en": "XPeng", "market": "US"},
    {"ticker": "LI", "name": "理想汽車(美股)", "name_en": "Li Auto ADR", "market": "US"},
    {"ticker": "JPM", "name": "摩根大通", "name_en": "JPMorgan Chase", "market": "US"},
    {"ticker": "BAC", "name": "美國銀行", "name_en": "Bank of America", "market": "US"},
    {"ticker": "V", "name": "維薩", "name_en": "Visa", "market": "US"},
    {"ticker": "MA", "name": "萬事達卡", "name_en": "Mastercard", "market": "US"},
    {"ticker": "JNJ", "name": "強生", "name_en": "Johnson & Johnson", "market": "US"},
    {"ticker": "PG", "name": "寶潔", "name_en": "Procter & Gamble", "market": "US"},
    {"ticker": "KO", "name": "可口可樂", "name_en": "Coca-Cola", "market": "US"},
    {"ticker": "PEP", "name": "百事可樂", "name_en": "PepsiCo", "market": "US"},
    {"ticker": "WMT", "name": "沃爾瑪", "name_en": "Walmart", "market": "US"},
    {"ticker": "DIS", "name": "迪士尼", "name_en": "Disney", "market": "US"},
    {"ticker": "XOM", "name": "埃克森美孚", "name_en": "ExxonMobil", "market": "US"},
    {"ticker": "CVX", "name": "雪佛龍", "name_en": "Chevron", "market": "US"},
    {"ticker": "PFE", "name": "輝瑞", "name_en": "Pfizer", "market": "US"},
    {"ticker": "INTC", "name": "英特爾", "name_en": "Intel", "market": "US"},
    {"ticker": "CRM", "name": "賽富時", "name_en": "Salesforce", "market": "US"},
    {"ticker": "ORCL", "name": "甲骨文", "name_en": "Oracle", "market": "US"},
    {"ticker": "ADBE", "name": "奧多比", "name_en": "Adobe", "market": "US"},
    {"ticker": "COST", "name": "好市多", "name_en": "Costco", "market": "US"},
    {"ticker": "QCOM", "name": "高通", "name_en": "Qualcomm", "market": "US"},
    {"ticker": "UBER", "name": "優步", "name_en": "Uber", "market": "US"},
]


# ---------------------------------------------------------------------------
# 追蹤清單 / 訊號狀態 讀寫
# ---------------------------------------------------------------------------
def load_watchlist() -> list:
    if WATCHLIST_FILE.exists():
        return json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
    default = ["AAPL", "0700.HK"]
    WATCHLIST_FILE.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
    return default


def save_watchlist(items: list) -> None:
    WATCHLIST_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def load_scan_state() -> dict:
    if SCAN_STATE_FILE.exists():
        return json.loads(SCAN_STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_scan_state(state: dict) -> None:
    SCAN_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 技術指標（手寫，只用 pandas/numpy，避免額外套件版本衝突）
# ---------------------------------------------------------------------------
def calc_indicators(df: pd.DataFrame) -> dict:
    close = df["Close"]
    volume = df["Volume"]

    sma5 = close.rolling(5).mean()
    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi14 = 100 - (100 / (1 + rs))
    rsi14 = rsi14.fillna(100)

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    std20 = close.rolling(20).std()
    bb_upper = sma20 + 2 * std20
    bb_lower = sma20 - 2 * std20

    vol_avg20 = volume.rolling(20).mean()

    # KDJ
    low9 = df["Low"].rolling(9).min()
    high9 = df["High"].rolling(9).max()
    rsv = ((close - low9) / (high9 - low9).replace(0, np.nan) * 100).fillna(50)
    kdj_k = rsv.ewm(com=2).mean()
    kdj_d = kdj_k.ewm(com=2).mean()
    kdj_j = 3 * kdj_k - 2 * kdj_d

    # ATR(14)：用嚟估算合理嘅建議買入/賣出價位波幅範圍
    prev_close = close.shift(1)
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr14 = tr.rolling(14).mean()

    return dict(
        close=close, volume=volume, sma5=sma5, sma20=sma20, sma50=sma50, sma200=sma200,
        rsi14=rsi14, macd_line=macd_line, macd_signal=signal_line, histogram=histogram,
        bb_upper=bb_upper, bb_lower=bb_lower, vol_avg20=vol_avg20,
        kdj_k=kdj_k, kdj_d=kdj_d, kdj_j=kdj_j, atr14=atr14,
    )


def label_from_score(score: float) -> str:
    if score >= 30:
        return "建議買入"
    if score <= -30:
        return "建議賣出"
    return "觀望"


def suggest_trade_levels(ind: dict, direction: str) -> Optional[dict]:
    """根據 ATR(14) 波幅同布林帶／MA20 支持阻力，為「建議買入」／「建議賣出」訊號
    估算合理嘅參考價位（唔係精準買賣點，只係波幅範圍估算，僅供參考）。
    direction: "buy" 或 "sell"。"""
    close = ind.get("close")
    if close is None or close.empty or pd.isna(close.iloc[-1]):
        return None
    price = float(close.iloc[-1])

    def _last(series):
        return float(series.iloc[-1]) if series is not None and len(series) and pd.notna(series.iloc[-1]) else None

    atr_val = _last(ind.get("atr14"))
    if atr_val is None or atr_val <= 0:
        # ATR 未夠 14 日數據時，用近20日收市價標準差做後備估算（下限為現價 2%）
        try:
            fallback = float(close.tail(20).std())
            if pd.isna(fallback):
                fallback = 0.0
        except Exception:
            fallback = 0.0
        atr_val = max(price * 0.02, fallback)

    bb_lo, bb_hi, ma20 = _last(ind.get("bb_lower")), _last(ind.get("bb_upper")), _last(ind.get("sma20"))

    if direction == "buy":
        # 建議買入區間：現價附近至回踩支持位（布林下軌／MA20／ATR 估算）之間，取較貼近現價者
        support_candidates = [v for v in [bb_lo, ma20, price - 0.8 * atr_val] if v is not None]
        entry_low = min(support_candidates) if support_candidates else price - 0.8 * atr_val
        entry_low = round(min(entry_low, price), 2)
        entry_high = round(price + 0.15 * atr_val, 2)
        if entry_low > entry_high:
            entry_low, entry_high = entry_high, entry_low
        stop_loss = round(entry_low - 1.0 * atr_val, 2)
        take_profit = round(price + 2.0 * atr_val, 2)
        return {
            "direction": "buy",
            "entry_low": entry_low,
            "entry_high": entry_high,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
        }

    # direction == "sell"
    resistance_candidates = [v for v in [bb_hi, ma20, price + 0.8 * atr_val] if v is not None]
    exit_high = max(resistance_candidates) if resistance_candidates else price + 0.8 * atr_val
    exit_high = round(max(exit_high, price), 2)
    exit_low = round(price - 0.15 * atr_val, 2)
    if exit_low > exit_high:
        exit_low, exit_high = exit_high, exit_low
    rebound_watch = round(exit_high + 1.0 * atr_val, 2)
    downside_target = round(price - 2.0 * atr_val, 2)
    return {
        "direction": "sell",
        "exit_low": exit_low,
        "exit_high": exit_high,
        "rebound_watch": rebound_watch,
        "downside_target": downside_target,
    }


def format_trade_levels(levels: Optional[dict], inline: bool = False) -> str:
    """將 suggest_trade_levels 輸出格式化做人類可讀文字，用於 Telegram 通知。
    inline=True 時輸出較短嘅單行版本（用喺清單類通知，一隻股票一行）。"""
    if not levels:
        return ""
    if levels["direction"] == "buy":
        if inline:
            return f"買入區 {levels['entry_low']}–{levels['entry_high']}｜止蝕 {levels['stop_loss']}｜目標 {levels['take_profit']}"
        return (f"💰 建議買入區間：{levels['entry_low']} – {levels['entry_high']}\n"
                f"　　止蝕價：{levels['stop_loss']}　目標價：{levels['take_profit']}")
    if inline:
        return f"賣出區 {levels['exit_low']}–{levels['exit_high']}｜反彈留意 {levels['rebound_watch']}｜下試 {levels['downside_target']}"
    return (f"💰 建議賣出區間：{levels['exit_low']} – {levels['exit_high']}\n"
            f"　　反彈留意價：{levels['rebound_watch']}　下試目標：{levels['downside_target']}")


def compute_short_term(ind: dict) -> dict:
    i, p = -1, -2
    score, reasons = 0.0, []

    sma5, sma20 = ind["sma5"], ind["sma20"]
    if len(sma5) >= 2 and pd.notna(sma5.iloc[i]) and pd.notna(sma20.iloc[i]) and pd.notna(sma5.iloc[p]) and pd.notna(sma20.iloc[p]):
        cross_up = sma5.iloc[p] <= sma20.iloc[p] and sma5.iloc[i] > sma20.iloc[i]
        cross_down = sma5.iloc[p] >= sma20.iloc[p] and sma5.iloc[i] < sma20.iloc[i]
        if cross_up:
            score += 2; reasons.append("MA5 上穿 MA20，短期黃金交叉")
        elif cross_down:
            score -= 2; reasons.append("MA5 下穿 MA20，短期死亡交叉")
        elif sma5.iloc[i] > sma20.iloc[i]:
            score += 1; reasons.append("MA5 高於 MA20，短期趨勢偏多")
        else:
            score -= 1; reasons.append("MA5 低於 MA20，短期趨勢偏空")

    rsi = ind["rsi14"].iloc[i] if len(ind["rsi14"]) else np.nan
    if pd.notna(rsi):
        if rsi < 30:
            score += 2; reasons.append(f"RSI(14)={rsi:.1f}，處於超賣區")
        elif rsi < 45:
            score += 1; reasons.append(f"RSI(14)={rsi:.1f}，偏弱但未超賣")
        elif rsi <= 55:
            reasons.append(f"RSI(14)={rsi:.1f}，中性")
        elif rsi <= 70:
            score -= 1; reasons.append(f"RSI(14)={rsi:.1f}，偏強但未超買")
        else:
            score -= 2; reasons.append(f"RSI(14)={rsi:.1f}，處於超買區")

    hist = ind["histogram"]
    if len(hist) >= 2 and pd.notna(hist.iloc[i]) and pd.notna(hist.iloc[p]):
        cross_up = hist.iloc[p] <= 0 and hist.iloc[i] > 0
        cross_down = hist.iloc[p] >= 0 and hist.iloc[i] < 0
        if cross_up:
            score += 2; reasons.append("MACD 柱狀圖轉正，動能轉強")
        elif cross_down:
            score -= 2; reasons.append("MACD 柱狀圖轉負，動能轉弱")
        elif hist.iloc[i] > 0:
            score += 1; reasons.append("MACD 柱狀圖持續為正")
        else:
            score -= 1; reasons.append("MACD 柱狀圖持續為負")

    if pd.notna(ind["bb_upper"].iloc[i]):
        price = ind["close"].iloc[i]
        width = ind["bb_upper"].iloc[i] - ind["bb_lower"].iloc[i]
        if width and price <= ind["bb_lower"].iloc[i] + 0.1 * width:
            score += 1.5; reasons.append("股價貼近布林帶下軌，短期超賣")
        elif width and price >= ind["bb_upper"].iloc[i] - 0.1 * width:
            score -= 1.5; reasons.append("股價貼近布林帶上軌，短期超買")

    if pd.notna(ind["vol_avg20"].iloc[i]) and ind["volume"].iloc[i] > ind["vol_avg20"].iloc[i] * 1.2:
        if ind["close"].iloc[i] > ind["close"].iloc[p]:
            score += 1; reasons.append("成交量放大且股價上升，量價配合")
        elif ind["close"].iloc[i] < ind["close"].iloc[p]:
            score -= 1; reasons.append("成交量放大但股價下跌，賣壓增加")

    norm = max(-100, min(100, (score / 8.5) * 100))
    label = label_from_score(norm)
    levels = None
    if label == "建議買入":
        levels = suggest_trade_levels(ind, "buy")
    elif label == "建議賣出":
        levels = suggest_trade_levels(ind, "sell")
    return dict(score=norm, label=label, reasons=reasons, levels=levels)


def compute_long_term(ind: dict) -> dict:
    i = -1
    score, reasons = 0.0, []
    sma50, sma200, close = ind["sma50"], ind["sma200"], ind["close"]
    n = len(close)
    if pd.notna(sma50.iloc[i]) and pd.notna(sma200.iloc[i]):
        crossed_up = crossed_down = False
        lookback = min(10, n - 1)
        for j in range(n - lookback, n):
            if j <= 0:
                continue
            a0, b0, a1, b1 = sma50.iloc[j - 1], sma200.iloc[j - 1], sma50.iloc[j], sma200.iloc[j]
            if pd.isna(a0) or pd.isna(b0) or pd.isna(a1) or pd.isna(b1):
                continue
            if a0 <= b0 and a1 > b1:
                crossed_up = True
            if a0 >= b0 and a1 < b1:
                crossed_down = True
        if crossed_up:
            score += 3; reasons.append("近期出現 MA50/MA200 黃金交叉，中長期轉強訊號")
        elif crossed_down:
            score -= 3; reasons.append("近期出現 MA50/MA200 死亡交叉，中長期轉弱訊號")
        elif sma50.iloc[i] > sma200.iloc[i]:
            score += 2; reasons.append("MA50 持續高於 MA200，中長期趨勢向上")
        else:
            score -= 2; reasons.append("MA50 持續低於 MA200，中長期趨勢向下")
    else:
        reasons.append("數據不足 200 個交易日，MA200 無法計算，中長期訊號參考性較低")

    if pd.notna(sma200.iloc[i]):
        diff_pct = (close.iloc[i] - sma200.iloc[i]) / sma200.iloc[i] * 100
        if diff_pct > 5:
            score += 1.5; reasons.append(f"股價高於 MA200 約 {diff_pct:.1f}%，長期偏多")
        elif diff_pct < -5:
            score -= 1.5; reasons.append(f"股價低於 MA200 約 {abs(diff_pct):.1f}%，長期偏空")

    lookback = min(252, n)
    recent = close.iloc[-lookback:]
    high52, low52 = recent.max(), recent.min()
    pos = (close.iloc[i] - low52) / ((high52 - low52) or 1)
    if pos > 0.7:
        score += 1; reasons.append("股價接近 52 週高位，長期動能強勁")
    elif pos < 0.3:
        score -= 1; reasons.append("股價接近 52 週低位，長期動能疲弱")

    if n > 20 and pd.notna(sma50.iloc[i]) and pd.notna(sma50.iloc[i - 20]):
        if sma50.iloc[i] > sma50.iloc[i - 20]:
            score += 1; reasons.append("MA50 呈上升趨勢")
        else:
            score -= 1; reasons.append("MA50 呈下降趨勢")

    norm = max(-100, min(100, (score / 7.5) * 100))
    label = label_from_score(norm)
    levels = None
    if label == "建議買入":
        levels = suggest_trade_levels(ind, "buy")
    elif label == "建議賣出":
        levels = suggest_trade_levels(ind, "sell")
    return dict(score=norm, label=label, reasons=reasons, levels=levels)


# ---------------------------------------------------------------------------
# 簡化回測（純 pandas，唔用 backtrader）
# ---------------------------------------------------------------------------
def simple_backtest(df: pd.DataFrame, ind: dict, cost_pct: float = 0.001) -> dict:
    n = len(df)
    if n < 40:
        return {"error": "數據不足，無法回測（建議至少 60 個交易日）"}

    positions = [0] * n
    for i in range(25, n):
        sub_ind = {k: v.iloc[: i + 1] for k, v in ind.items()}
        try:
            score = compute_short_term(sub_ind)["score"]
        except Exception:
            score = 0
        positions[i] = 1 if score >= 30 else 0

    close = df["Close"].reset_index(drop=True)
    daily_ret = close.pct_change().fillna(0)
    pos_series = pd.Series(positions)
    pos_shifted = pos_series.shift(1).fillna(0)
    trade_change = pos_series.diff().fillna(0)
    strategy_ret = pos_shifted * daily_ret - (trade_change.abs() * cost_pct)

    cum_strategy = float((1 + strategy_ret).cumprod().iloc[-1] - 1)
    cum_buyhold = float((1 + daily_ret).cumprod().iloc[-1] - 1)

    entries = trade_change[trade_change == 1].index.tolist()
    exits = trade_change[trade_change == -1].index.tolist()
    wins, total_trades = 0, 0
    for e in entries:
        exit_idx = next((x for x in exits if x > e), n - 1)
        trade_ret = close.iloc[exit_idx] / close.iloc[e] - 1
        total_trades += 1
        if trade_ret > 0:
            wins += 1
    win_rate = (wins / total_trades * 100) if total_trades else None

    return {
        "period_days": n,
        "strategy_return_pct": round(cum_strategy * 100, 2),
        "buy_hold_return_pct": round(cum_buyhold * 100, 2),
        "num_trades": total_trades,
        "win_rate_pct": round(win_rate, 1) if win_rate is not None else None,
        "note": "簡化回測僅供參考：以短期訊號分數 ≥30 作為進場依據，收盤價成交，"
                "已扣除假設 0.1% 交易成本，但未計滑點/流動性影響，且僅覆蓋單一策略窗口，"
                "小心過度擬合（overfitting）。",
    }


# ---------------------------------------------------------------------------
# 即時報價
# ---------------------------------------------------------------------------
def _safe(fi, *keys):
    for k in keys:
        try:
            v = fi[k]
            if v is not None:
                return v
        except Exception:
            continue
    return None


def get_quote(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    fi = t.fast_info
    price = _safe(fi, "lastPrice", "last_price")
    prev_close = _safe(fi, "previousClose", "previous_close", "regularMarketPreviousClose")
    change = (price - prev_close) if (price is not None and prev_close is not None) else None
    change_pct = (change / prev_close * 100) if (change is not None and prev_close) else None

    quote = {
        "ticker": ticker,
        "price": price,
        "prev_close": prev_close,
        "change": round(change, 2) if change is not None else None,
        "change_pct": round(change_pct, 2) if change_pct is not None else None,
        "open": _safe(fi, "open"),
        "day_high": _safe(fi, "dayHigh", "day_high"),
        "day_low": _safe(fi, "dayLow", "day_low"),
        "year_high": _safe(fi, "yearHigh", "year_high"),
        "year_low": _safe(fi, "yearLow", "year_low"),
        "volume": _safe(fi, "lastVolume", "last_volume"),
        "avg_volume_10d": _safe(fi, "tenDayAverageVolume", "ten_day_average_volume"),
        "market_cap": _safe(fi, "marketCap", "market_cap"),
        "currency": _safe(fi, "currency"),
        "pe_ratio": None, "eps": None, "dividend_yield": None, "name": ticker,
    }
    try:
        info = t.info
        quote["pe_ratio"] = info.get("trailingPE")
        quote["eps"] = info.get("trailingEps")
        dy = info.get("dividendYield")
        quote["dividend_yield"] = round(dy * 100, 2) if dy else None
        quote["name"] = info.get("shortName") or info.get("longName") or ticker
    except Exception:
        pass
    return quote


# ---------------------------------------------------------------------------
# 基本面／價值投資評分
# ---------------------------------------------------------------------------
# 呢個模組將幾套經典價值投資框架度化做可計算嘅評分準則，用嚟補足純技術分析
# 睇唔到嘅「呢間公司本身是否值得長揸」嘅角度：
#   - ROE／營運利潤率 → 巴菲特／芒格強調嘅「護城河」與資本回報品質
#   - 負債水平／流動比率 → 達里奧《債務危機》強調嘅槓桿與償債風險意識
#   - PEG 比率 → 彼得林區《戰勝華爾街》嘅「用增長幅度睇估值是否合理」準則
#   - 本益比 x 市淨率（Graham Number 概念）→ 葛拉漢《智慧型股票投資人》嘅保守估值安全邊際
#   - 自由現金流收益率 → 巴菲特股東信入面反覆強調嘅「owner earnings」盈利品質
#   - 營收增長 → 費雪《非常潛力股》強調嘅持續成長能力
#   - 盈利收益率 vs 10年期美債息率 → 呼應伯南克講及嘅利率環境如何影響資產估值
# 呢啲純粹係將公開嘅財務數據，按經典價值投資書籍入面公開討論過嘅概念量化，
# 並非逐字引用任何書籍內容，亦不構成投資建議。
_RISK_FREE_CACHE = {"value": None, "ts": None}


def get_risk_free_rate_pct() -> Optional[float]:
    """攞 10 年期美債息率（^TNX，Yahoo 報價要 /10 先係實際 %），加簡單快取。"""
    now = datetime.now()
    if _RISK_FREE_CACHE["value"] is not None and _RISK_FREE_CACHE["ts"] and (now - _RISK_FREE_CACHE["ts"]).total_seconds() < 3600:
        return _RISK_FREE_CACHE["value"]
    try:
        hist = yf.Ticker("^TNX").history(period="5d")
        if hist is None or hist.empty:
            return None
        val = float(hist["Close"].iloc[-1]) / 10.0
        _RISK_FREE_CACHE["value"] = val
        _RISK_FREE_CACHE["ts"] = now
        return val
    except Exception:
        return None


def _ratio(numerator, denominator):
    try:
        if numerator is None or denominator in (None, 0):
            return None
        return numerator / denominator
    except Exception:
        return None


def get_fundamentals(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    info = {}
    try:
        info = t.info or {}
    except Exception:
        info = {}

    pe = info.get("trailingPE")
    pb = info.get("priceToBook")
    peg = info.get("pegRatio") or info.get("trailingPegRatio")
    roe = info.get("returnOnEquity")
    roa = info.get("returnOnAssets")
    debt_to_equity = info.get("debtToEquity")
    current_ratio = info.get("currentRatio")
    op_margin = info.get("operatingMargins")
    gross_margin = info.get("grossMargins")
    profit_margin = info.get("profitMargins")
    revenue_growth = info.get("revenueGrowth")
    earnings_growth = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
    fcf = info.get("freeCashflow")
    market_cap = info.get("marketCap")
    insider_pct = info.get("heldPercentInsiders")
    institution_pct = info.get("heldPercentInstitutions")

    fcf_yield = _ratio(fcf, market_cap)
    if debt_to_equity is not None and debt_to_equity > 5:
        # yfinance 部分股票 debtToEquity 用百分比表示（例如 120 即 1.2），統一換算做比率
        debt_to_equity = debt_to_equity / 100.0

    return {
        "ticker": ticker,
        "name": info.get("shortName") or info.get("longName") or ticker,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "pe": pe,
        "pb": pb,
        "peg": peg,
        "roe": roe,
        "roa": roa,
        "debt_to_equity": debt_to_equity,
        "current_ratio": current_ratio,
        "operating_margin": op_margin,
        "gross_margin": gross_margin,
        "profit_margin": profit_margin,
        "revenue_growth": revenue_growth,
        "earnings_growth": earnings_growth,
        "fcf": fcf,
        "market_cap": market_cap,
        "fcf_yield": fcf_yield,
        "insider_pct": insider_pct,
        "institution_pct": institution_pct,
    }


def compute_fundamental_score(f: dict, risk_free_pct: Optional[float]) -> dict:
    score, reasons = 0.0, []
    have_data = False

    roe = f.get("roe")
    if roe is not None:
        have_data = True
        roe_pct = roe * 100
        if roe_pct >= 20:
            score += 2; reasons.append(f"ROE={roe_pct:.1f}%，資本回報能力強，反映一定護城河（巴菲特/芒格重視嘅質量指標）")
        elif roe_pct >= 10:
            score += 1; reasons.append(f"ROE={roe_pct:.1f}%，資本回報中等")
        elif roe_pct >= 0:
            reasons.append(f"ROE={roe_pct:.1f}%，資本回報偏弱")
        else:
            score -= 2; reasons.append(f"ROE={roe_pct:.1f}%，股東權益回報為負")

    op_margin = f.get("operating_margin")
    if op_margin is not None:
        have_data = True
        om_pct = op_margin * 100
        if om_pct >= 15:
            score += 1; reasons.append(f"營運利潤率={om_pct:.1f}%，反映一定定價能力（費雪《非常潛力股》重視嘅質量指標）")
        elif om_pct < 5:
            score -= 0.5; reasons.append(f"營運利潤率={om_pct:.1f}%，偏低")

    dte = f.get("debt_to_equity")
    if dte is not None:
        have_data = True
        if dte < 0.5:
            score += 1.5; reasons.append(f"負債權益比={dte:.2f}，槓桿低、財務體質穩健（呼應達里奧《債務危機》嘅去槓桿意識）")
        elif dte <= 1.5:
            reasons.append(f"負債權益比={dte:.2f}，槓桿中等")
        else:
            score -= 1.5; reasons.append(f"負債權益比={dte:.2f}，槓桿偏高，需留意債務週期風險")

    cr = f.get("current_ratio")
    if cr is not None:
        have_data = True
        if cr >= 1.5:
            score += 1; reasons.append(f"流動比率={cr:.2f}，短期償債能力充裕")
        elif cr < 1:
            score -= 1; reasons.append(f"流動比率={cr:.2f}，短期流動性偏緊")

    peg = f.get("peg")
    if peg is not None and peg > 0:
        have_data = True
        if peg < 1:
            score += 2; reasons.append(f"PEG={peg:.2f}，相對盈利增長被低估（彼得林區《戰勝華爾街》嘅選股準則）")
        elif peg <= 2:
            reasons.append(f"PEG={peg:.2f}，估值大致合理")
        else:
            score -= 1.5; reasons.append(f"PEG={peg:.2f}，估值相對增長偏貴")

    pe, pb = f.get("pe"), f.get("pb")
    if pe is not None and pb is not None and pe > 0 and pb > 0:
        have_data = True
        graham_number = pe * pb
        if graham_number < 15:
            score += 1.5; reasons.append(f"本益比×市淨率={graham_number:.1f}，估值保守，安全邊際充足（葛拉漢《智慧型股票投資人》準則）")
        elif graham_number <= 22.5:
            score += 0.5; reasons.append(f"本益比×市淨率={graham_number:.1f}，估值合理")
        else:
            score -= 1; reasons.append(f"本益比×市淨率={graham_number:.1f}，估值偏高，安全邊際不足")

    fcf_yield = f.get("fcf_yield")
    if fcf_yield is not None:
        have_data = True
        fy_pct = fcf_yield * 100
        if fy_pct >= 5:
            score += 1.5; reasons.append(f"自由現金流收益率={fy_pct:.1f}%，盈利品質高（巴菲特股東信強調嘅 owner earnings 概念）")
        elif fy_pct >= 2:
            score += 0.5; reasons.append(f"自由現金流收益率={fy_pct:.1f}%，尚可")
        elif fy_pct < 0:
            score -= 1.5; reasons.append(f"自由現金流收益率={fy_pct:.1f}%，現金流為負，盈利品質存疑")

    rev_g = f.get("revenue_growth")
    if rev_g is not None:
        have_data = True
        rg_pct = rev_g * 100
        if rg_pct >= 10:
            score += 1; reasons.append(f"營收年增長={rg_pct:.1f}%，成長動能強勁（費雪重視嘅持續成長能力）")
        elif rg_pct < 0:
            score -= 1; reasons.append(f"營收年增長={rg_pct:.1f}%，營收萎縮")

    if pe is not None and pe > 0 and risk_free_pct is not None:
        have_data = True
        earnings_yield_pct = (1 / pe) * 100
        spread = earnings_yield_pct - risk_free_pct
        if spread >= 3:
            score += 1; reasons.append(
                f"盈利收益率={earnings_yield_pct:.1f}% 高於10年期美債息率({risk_free_pct:.1f}%) {spread:.1f} 個百分點，"
                f"相對無風險利率仍具吸引力（呼應伯南克講及嘅利率與資產估值關係）"
            )
        elif spread < 0:
            score -= 1; reasons.append(
                f"盈利收益率={earnings_yield_pct:.1f}% 低於10年期美債息率({risk_free_pct:.1f}%)，估值相對無風險利率缺乏吸引力"
            )

    if not have_data:
        return {"score": 0.0, "label": "數據不足", "reasons": ["此股票缺乏足夠公開財務數據，無法計算基本面評分"], "insufficient": True}

    norm = max(-100.0, min(100.0, (score / 12.5) * 100))
    return {"score": norm, "label": label_from_score(norm), "reasons": reasons, "insufficient": False}


# ---------------------------------------------------------------------------
# 「品質／護城河」評分 —— 財報體質篩選
# ---------------------------------------------------------------------------
# 呢個模組獨立於上面嘅「估值」評分，專注睇公司財報體質是否符合「持久
# 競爭優勢」嘅量化特徵（毛利率、費用結構、槓桿成本、盈利穩定性），
# 概念上參考：
#   - 《巴菲特財報學》(Mary Buffett) 提出嘅「持久競爭優勢」財報篩選準則：
#     高毛利率、SG&A／折舊佔毛利比重低、利息支出佔營業利益比重低、
#     淨利率穩定偏高，反映公司有定價權、成本結構穩健
#   - 《窮查理寶典》(芒格) 強調嘅「護城河要用多年數據驗證，唔止睇一年」
#     思維 —— 呢度用毛利率／ROE 嘅近年波動度做穩定性代理指標
# 同樣地，呢啲只係將財報公開數字按呢類書籍討論過嘅概念量化，
# 並非引用書中原文，亦不構成投資建議。
def _latest_row(df: pd.DataFrame, names: list) -> Optional[float]:
    if df is None or df.empty:
        return None
    for name in names:
        if name in df.index:
            s = df.loc[name].dropna()
            if len(s):
                try:
                    return float(s.iloc[0])
                except Exception:
                    continue
    return None


def _row_series(df: pd.DataFrame, names: list) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    for name in names:
        if name in df.index:
            s = df.loc[name].dropna()
            if len(s):
                return s.astype(float)
    return None


def get_quality_metrics(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    try:
        fin = t.financials  # 年度損益表，欄位由新到舊
    except Exception:
        fin = pd.DataFrame()
    try:
        bs = t.balance_sheet
    except Exception:
        bs = pd.DataFrame()

    revenue = _latest_row(fin, ["Total Revenue", "TotalRevenue"])
    gross_profit = _latest_row(fin, ["Gross Profit", "GrossProfit"])
    sga = _latest_row(fin, ["Selling General And Administration", "Selling General And Administrative"])
    dep = _latest_row(fin, ["Reconciled Depreciation", "Depreciation And Amortization In Income Statement"])
    interest_exp = _latest_row(fin, ["Interest Expense", "Interest Expense Non Operating"])
    op_income = _latest_row(fin, ["Operating Income", "OperatingIncome"])
    net_income = _latest_row(fin, ["Net Income", "NetIncome", "Net Income Common Stockholders"])
    long_term_debt = _latest_row(bs, ["Long Term Debt", "LongTermDebt"])
    total_debt = _latest_row(bs, ["Total Debt", "TotalDebt"])

    gross_margin = _ratio(gross_profit, revenue)
    sga_ratio = _ratio(sga, gross_profit)
    dep_ratio = _ratio(dep, gross_profit)
    interest_ratio = _ratio(interest_exp, op_income)
    net_margin = _ratio(net_income, revenue)
    debt_for_payback = long_term_debt if long_term_debt is not None else total_debt
    debt_payback_years = _ratio(debt_for_payback, net_income) if (net_income and net_income > 0) else None

    # 多年淨利率穩定性（芒格式「用多年數據驗證護城河」代理指標）
    net_income_series = _row_series(fin, ["Net Income", "NetIncome", "Net Income Common Stockholders"])
    revenue_series = _row_series(fin, ["Total Revenue", "TotalRevenue"])
    margin_cv = None
    years_available = 0
    if net_income_series is not None and revenue_series is not None:
        common_idx = net_income_series.index.intersection(revenue_series.index)
        if len(common_idx) >= 2:
            margins = (net_income_series[common_idx] / revenue_series[common_idx]).dropna()
            years_available = len(margins)
            if len(margins) >= 2 and margins.mean() != 0:
                margin_cv = float(margins.std() / abs(margins.mean()))

    return {
        "ticker": ticker,
        "gross_margin": gross_margin,
        "sga_ratio": sga_ratio,
        "dep_ratio": dep_ratio,
        "interest_ratio": interest_ratio,
        "net_margin": net_margin,
        "debt_payback_years": debt_payback_years,
        "net_margin_volatility": margin_cv,
        "years_available": years_available,
    }


def compute_quality_score(q: dict) -> dict:
    score, reasons = 0.0, []
    have_data = False

    gm = q.get("gross_margin")
    if gm is not None:
        have_data = True
        gm_pct = gm * 100
        if gm_pct >= 40:
            score += 2; reasons.append(f"毛利率={gm_pct:.1f}%，反映較強定價權（《巴菲特財報學》嘅持久競爭優勢篩選準則）")
        elif gm_pct >= 20:
            score += 0.5; reasons.append(f"毛利率={gm_pct:.1f}%，中等定價能力")
        else:
            score -= 1; reasons.append(f"毛利率={gm_pct:.1f}%，偏低，可能身處競爭激烈嘅行業")

    sga_r = q.get("sga_ratio")
    if sga_r is not None:
        have_data = True
        sga_pct = sga_r * 100
        if sga_pct < 30:
            score += 1; reasons.append(f"SG&A／毛利比重={sga_pct:.1f}%，成本結構精簡（《巴菲特財報學》準則）")
        elif sga_pct > 60:
            score -= 1; reasons.append(f"SG&A／毛利比重={sga_pct:.1f}%，銷售管理費用偏重")

    int_r = q.get("interest_ratio")
    if int_r is not None:
        have_data = True
        int_pct = int_r * 100
        if int_pct < 15:
            score += 1; reasons.append(f"利息支出／營業利益={int_pct:.1f}%，槓桿成本負擔輕")
        elif int_pct > 40:
            score -= 1.5; reasons.append(f"利息支出／營業利益={int_pct:.1f}%，財務槓桿成本偏重")

    nm = q.get("net_margin")
    if nm is not None:
        have_data = True
        nm_pct = nm * 100
        if nm_pct >= 20:
            score += 2; reasons.append(f"淨利率={nm_pct:.1f}%，盈利能力優異")
        elif nm_pct >= 10:
            score += 1; reasons.append(f"淨利率={nm_pct:.1f}%，盈利能力中等")
        elif nm_pct < 0:
            score -= 2; reasons.append(f"淨利率={nm_pct:.1f}%，公司虧損")

    dpy = q.get("debt_payback_years")
    if dpy is not None:
        have_data = True
        if dpy <= 4:
            score += 1; reasons.append(f"長期負債／年度淨利≈{dpy:.1f}年，債務可於合理年期內以盈利償還")
        elif dpy > 10:
            score -= 1; reasons.append(f"長期負債／年度淨利≈{dpy:.1f}年，償債年期偏長")

    mcv = q.get("net_margin_volatility")
    years = q.get("years_available", 0)
    if mcv is not None and years >= 3:
        have_data = True
        if mcv < 0.2:
            score += 1.5; reasons.append(f"近{years}年淨利率波動度低（變異係數={mcv:.2f}），盈利穩定，護城河較可信（芒格《窮查理寶典》嘅多年驗證思維）")
        elif mcv > 0.6:
            score -= 1; reasons.append(f"近{years}年淨利率波動度高（變異係數={mcv:.2f}），盈利穩定性存疑")

    if not have_data:
        return {"score": 0.0, "label": "數據不足", "reasons": ["此股票缺乏足夠財報明細數據，無法計算品質評分"], "insufficient": True}

    norm = max(-100.0, min(100.0, (score / 8.5) * 100))
    return {"score": norm, "label": label_from_score(norm), "reasons": reasons, "insufficient": False}


# ---------------------------------------------------------------------------
# 風險分散／建議持倉比重 —— Kelly Criterion 啟發式簡化版
# ---------------------------------------------------------------------------
# 《戰勝一切市場的人》(Ed Thorp) 入面提到用 Kelly Criterion 概念做資金
# 部位管理：優勢（edge）越大、波動越低，理論上可承受嘅倉位越大。
# 呢度並非正式 Kelly 公式（無真實勝率/賠率數據），只係將「綜合訊號分數」
# 當做優勢強度嘅代理指標，並用近期年化波動度做風險調整，再取半 Kelly
# 並設上限，作為分散風險嘅參考數字 —— 絕非精確倉位建議。
def suggest_position_size(overall_score: float, ann_vol_pct: Optional[float]) -> dict:
    edge = max(0.0, overall_score) / 100.0  # 只喺分數為正（偏買入）先建議倉位
    if ann_vol_pct is None or ann_vol_pct <= 0:
        vol_factor = 1.0
    else:
        target_vol = 25.0  # 以年化 25% 波動度作為基準
        vol_factor = max(0.3, min(1.5, target_vol / ann_vol_pct))

    raw_kelly = edge * vol_factor
    half_kelly = raw_kelly * 0.5
    suggested_pct = max(0.0, min(20.0, half_kelly * 20))  # 上限封頂 20%，保守處理

    return {
        "suggested_position_pct": round(suggested_pct, 1),
        "annualized_volatility_pct": round(ann_vol_pct, 1) if ann_vol_pct is not None else None,
        "note": "此為半 Kelly 啟發式簡化估算（非正式 Kelly 公式），僅供單一持股於整體投資組合中"
                "分散風險嘅參考，已設 20% 封頂，並非投資建議，實際部位應配合個人風險承受能力自行決定。",
    }


# ---------------------------------------------------------------------------
# API 路由
# ---------------------------------------------------------------------------
@app.get("/api/quote/{ticker}")
def api_quote(ticker: str):
    try:
        return get_quote(ticker.upper())
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"無法取得 {ticker} 報價：{e}")


@app.get("/api/signals/{ticker}")
def api_signals(ticker: str, period: str = "2y"):
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"抓取歷史數據失敗：{e}")
    if df is None or df.empty or len(df) < 20:
        raise HTTPException(status_code=404, detail=f"{ticker} 數據不足或代號無效")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close"])
    ind = calc_indicators(df)
    short = compute_short_term(ind)
    long_ = compute_long_term(ind)

    def g(series, idx):
        v = series.loc[idx] if idx in series.index else np.nan
        return None if pd.isna(v) else round(float(v), 3)

    chart = [
        {
            "date": str(idx.date()) if hasattr(idx, "date") else str(idx),
            "close": g(ind["close"], idx),
            "sma20": g(ind["sma20"], idx),
            "sma50": g(ind["sma50"], idx),
            "sma200": g(ind["sma200"], idx),
            "rsi14": g(ind["rsi14"], idx),
            "macd": g(ind["macd_line"], idx),
            "macd_signal": g(ind["macd_signal"], idx),
            "macd_hist": g(ind["histogram"], idx),
            "kdj_k": g(ind["kdj_k"], idx),
            "kdj_d": g(ind["kdj_d"], idx),
            "kdj_j": g(ind["kdj_j"], idx),
        }
        for idx in df.index
    ]
    return {
        "ticker": ticker.upper(),
        "price": round(float(ind["close"].iloc[-1]), 2),
        "short": short,
        "long": long_,
        "chart": chart[-260:],
    }


@app.get("/api/fundamentals/{ticker}")
def api_fundamentals(ticker: str):
    ticker = ticker.upper()
    try:
        f = get_fundamentals(ticker)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"無法取得 {ticker} 基本面數據：{e}")
    risk_free = get_risk_free_rate_pct()
    result = compute_fundamental_score(f, risk_free)
    return {
        "ticker": ticker,
        "metrics": f,
        "risk_free_rate_pct": risk_free,
        "fundamental": result,
    }


@app.get("/api/quality/{ticker}")
def api_quality(ticker: str):
    ticker = ticker.upper()
    try:
        q = get_quality_metrics(ticker)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"無法取得 {ticker} 財報明細：{e}")
    result = compute_quality_score(q)
    return {"ticker": ticker, "metrics": q, "quality": result}


@app.get("/api/overview/{ticker}")
def api_overview(ticker: str, period: str = "2y"):
    """一次過攞短期/中長期技術訊號 + 基本面估值 + 財報品質評分，並計算加權綜合建議 + 建議倉位。"""
    ticker = ticker.upper()
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"抓取歷史數據失敗：{e}")
    if df is None or df.empty or len(df) < 20:
        raise HTTPException(status_code=404, detail=f"{ticker} 數據不足或代號無效")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close"])
    ind = calc_indicators(df)
    short = compute_short_term(ind)
    long_ = compute_long_term(ind)

    try:
        f = get_fundamentals(ticker)
        risk_free = get_risk_free_rate_pct()
        fundamental = compute_fundamental_score(f, risk_free)
    except Exception:
        fundamental = {"score": 0.0, "label": "數據不足", "reasons": ["基本面數據暫時無法取得"], "insufficient": True}

    try:
        q = get_quality_metrics(ticker)
        quality = compute_quality_score(q)
    except Exception:
        quality = {"score": 0.0, "label": "數據不足", "reasons": ["財報明細數據暫時無法取得"], "insufficient": True}

    # 加權綜合：短期技術20% + 中長期技術25% + 估值(Graham/Lynch/Dalio)25% + 財報品質(巴菲特財報學/芒格)30%
    weights_available = [("short", 0.20, short["score"], True),
                         ("long", 0.25, long_["score"], True),
                         ("fundamental", 0.25, fundamental["score"], not fundamental.get("insufficient")),
                         ("quality", 0.30, quality["score"], not quality.get("insufficient"))]
    usable = [(w, s) for _, w, s, ok in weights_available if ok]
    if usable:
        total_w = sum(w for w, _ in usable)
        overall_score = sum(w * s for w, s in usable) / total_w
    else:
        overall_score = 0.0
    used_names = [n for n, w, s, ok in weights_available if ok]
    weight_note = "綜合分數 = " + " + ".join(
        f"{n}({w*100:.0f}%)" for n, w, s, ok in weights_available if ok
    ) + "（缺數據嘅部分已按比例重新分配權重）"
    overall_score = max(-100.0, min(100.0, overall_score))

    # 近 60 個交易日年化波動度，用作建議倉位嘅風險調整
    daily_ret = df["Close"].pct_change().dropna()
    recent_ret = daily_ret.iloc[-60:] if len(daily_ret) >= 20 else daily_ret
    ann_vol_pct = float(recent_ret.std() * (252 ** 0.5) * 100) if len(recent_ret) >= 5 else None
    position = suggest_position_size(overall_score, ann_vol_pct)

    return {
        "ticker": ticker,
        "price": round(float(ind["close"].iloc[-1]), 2),
        "short": short,
        "long": long_,
        "fundamental": fundamental,
        "quality": quality,
        "overall": {"score": overall_score, "label": label_from_score(overall_score), "note": weight_note},
        "position": position,
    }


@app.get("/api/backtest/{ticker}")
def api_backtest(ticker: str, period: str = "2y"):
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"抓取歷史數據失敗：{e}")
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail=f"{ticker} 數據不足或代號無效")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close"])
    ind = calc_indicators(df)
    return simple_backtest(df, ind)


class WatchlistUpdate(BaseModel):
    action: str  # "add" | "remove"
    ticker: str


@app.get("/api/watchlist")
def api_get_watchlist():
    return {"watchlist": load_watchlist(), "poll_interval_minutes": POLL_INTERVAL_MINUTES}


@app.post("/api/watchlist")
def api_update_watchlist(update: WatchlistUpdate):
    items = load_watchlist()
    ticker = update.ticker.upper().strip()
    if update.action == "add" and ticker not in items:
        items.append(ticker)
    elif update.action == "remove" and ticker in items:
        items.remove(ticker)
    else:
        raise HTTPException(status_code=400, detail="action 必須是 add 或 remove")
    save_watchlist(items)
    return {"watchlist": items}


# ---------------------------------------------------------------------------
# 代號自動建議 + 建議買入/賣出掃描
# ---------------------------------------------------------------------------
def _quick_technical_score(df_ticker: pd.DataFrame) -> Optional[dict]:
    """輕量技術評分（唔攞基本面，淨係為咗掃描一批股票時速度夠快）。
    短期／中長期訊號分開計算同回傳，唔做加權平均。"""
    if df_ticker is None or df_ticker.empty:
        return None
    df_ticker = df_ticker.dropna(subset=["Close"]) if "Close" in df_ticker.columns else pd.DataFrame()
    if len(df_ticker) < 20:
        return None
    ind = calc_indicators(df_ticker)
    short = compute_short_term(ind)
    long_ = compute_long_term(ind)
    try:
        price = round(float(ind["close"].iloc[-1]), 2)
    except Exception:
        price = None
    return {
        "short_score": round(short["score"], 1),
        "short_label": short["label"],
        "short_levels": short.get("levels"),
        "long_score": round(long_["score"], 1),
        "long_label": long_["label"],
        "long_levels": long_.get("levels"),
        "price": price,
    }


@app.get("/api/search")
def api_search(q: str = ""):
    """代號輸入自動建議：按代號前綴或中/英文名稱關鍵字搜尋 STOCK_UNIVERSE。"""
    q = (q or "").strip()
    if not q:
        return {"results": []}
    q_upper = q.upper()
    results = [
        item for item in STOCK_UNIVERSE
        if item["ticker"].upper().startswith(q_upper)
        or q in item["name"]
        or q_upper in item["name_en"].upper()
    ]
    return {"results": results[:15]}


def compute_universe_scores() -> list:
    """對 STOCK_UNIVERSE 批次抓取數據並計算短期／中長期技術分數（共用邏輯，
    畀 /api/scan 同背景排程通知一齊用，避免重複寫兩次批次下載邏輯）。"""
    tickers = [item["ticker"] for item in STOCK_UNIVERSE]
    raw = yf.download(tickers, period="1y", interval="1d", progress=False,
                       auto_adjust=False, group_by="ticker", threads=True)
    scored = []
    for item in STOCK_UNIVERSE:
        t = item["ticker"]
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if t not in raw.columns.get_level_values(0):
                    continue
                df_t = raw[t]
            else:
                df_t = raw
            result = _quick_technical_score(df_t)
        except Exception:
            result = None
        if result is None:
            continue
        scored.append({**item, **result})
    return scored


@app.get("/api/scan")
def api_scan(type: str = "buy"):
    """建議買入／賣出掃描：對 STOCK_UNIVERSE 做批次技術訊號掃描。
    短期訊號同中長期訊號分開評分、分開篩選、分開排序（唔用加權平均合併），
    分別產生「短線值得買入/賣出」同「長期值得買入/賣出」兩張獨立清單，港股/美股再分開回傳。"""
    if type not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="type 必須是 buy 或 sell")

    try:
        scored = compute_universe_scores()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"批次抓取數據失敗：{e}")

    def pick(score_key: str, label_key: str):
        if type == "buy":
            picked = [s for s in scored if s[score_key] >= 20]
            picked.sort(key=lambda s: s[score_key], reverse=True)
        else:
            picked = [s for s in scored if s[score_key] <= -20]
            picked.sort(key=lambda s: s[score_key])
        # 統一輸出欄位名 score/label/levels，方便前端沿用同一套渲染邏輯
        levels_key = "short_levels" if score_key == "short_score" else "long_levels"
        return [
            {**s, "score": s[score_key], "label": s[label_key], "levels": s.get(levels_key)}
            for s in picked
        ]

    short_picked = pick("short_score", "short_label")
    long_picked = pick("long_score", "long_label")

    return {
        "type": type,
        "short_term": {
            "hk": [s for s in short_picked if s["market"] == "HK"],
            "us": [s for s in short_picked if s["market"] == "US"],
        },
        "long_term": {
            "hk": [s for s in long_picked if s["market"] == "HK"],
            "us": [s for s in long_picked if s["market"] == "US"],
        },
        "scanned_count": len(scored),
        "universe_count": len(STOCK_UNIVERSE),
        "note": "掃描範圍為精選常見港股/美股清單（非全市場），短期同中長期訊號分開評分、分開篩選，"
                "唔以加權平均合併，未包含基本面/財報品質評分，僅供參考，並非投資建議，請自行判斷風險。",
    }


# ---------------------------------------------------------------------------
# Telegram 自動通知（背景排程）
# ---------------------------------------------------------------------------
def _send_telegram_to(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=15)
        if resp.status_code != 200:
            print(f"[錯誤] Telegram 發送失敗（{chat_id}）：{resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[錯誤] Telegram 發送例外（{chat_id}）：{e}")


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[警告] 未設定 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID，只在終端機顯示：\n", text)
    else:
        _send_telegram_to(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, text)

    # 同步發送到第二個 Bot（如有設定）
    if TELEGRAM_BOT_TOKEN_2 and TELEGRAM_CHAT_ID_2:
        _send_telegram_to(TELEGRAM_BOT_TOKEN_2, TELEGRAM_CHAT_ID_2, text)


def build_quickchart_url(items: list, title: str) -> str:
    """將 [{'ticker':..., 'score_for_chart':...}, ...] 轉成 QuickChart 長條圖網址。"""
    tickers = [it["ticker"] for it in items]
    scores = [it["score_for_chart"] for it in items]
    chart_config = {
        "type": "bar",
        "data": {
            "labels": tickers,
            "datasets": [{
                "label": title,
                "data": scores,
                "backgroundColor": ["#2ecc71" if s >= 0 else "#e74c3c" for s in scores],
            }]
        },
        "options": {"plugins": {"title": {"display": True, "text": title}}}
    }
    encoded = urllib.parse.quote(json.dumps(chart_config))
    return f"https://quickchart.io/chart?c={encoded}&width=700&height=400&backgroundColor=white"


def _send_telegram_photo_to(bot_token: str, chat_id: str, photo_url: str, caption: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    try:
        resp = requests.post(url, json={
            "chat_id": chat_id, "photo": photo_url,
            "caption": caption, "parse_mode": "HTML",
        }, timeout=15)
        if resp.status_code != 200:
            print(f"[錯誤] Telegram 圖片發送失敗（{chat_id}）：{resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[錯誤] Telegram 圖片發送例外（{chat_id}）：{e}")


def send_telegram_photo(photo_url: str, caption: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[警告] 未設定 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID，跳過圖表推送")
    else:
        _send_telegram_photo_to(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, photo_url, caption)

    # 同步發送到第二個 Bot（如有設定）
    if TELEGRAM_BOT_TOKEN_2 and TELEGRAM_CHAT_ID_2:
        _send_telegram_photo_to(TELEGRAM_BOT_TOKEN_2, TELEGRAM_CHAT_ID_2, photo_url, caption)


def scan_watchlist_and_notify():
    state = load_state()
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    changed_messages = []

    for ticker in load_watchlist():
        try:
            df = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=False)
            if df is None or df.empty or len(df) < 20:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.dropna(subset=["Close"])
            ind = calc_indicators(df)
            short = compute_short_term(ind)
            long_ = compute_long_term(ind)
            price = float(ind["close"].iloc[-1])
        except Exception as e:
            print(f"[警告] {ticker} 掃描失敗：{e}")
            continue

        prev = state.get(ticker, {})
        prev_short, prev_long = prev.get("short_label"), prev.get("long_label")
        first_run = prev_short is None
        short_changed = not first_run and prev_short != short["label"]
        long_changed = not first_run and prev_long != long_["label"]

        if first_run or short_changed or long_changed:
            lines = [f"<b>{ticker}</b> 現價 {price:.2f}　({now})"]
            if first_run:
                lines.append(f"短期：{short['label']}（{short['score']:.0f}）｜中長期：{long_['label']}（{long_['score']:.0f}）")
            else:
                if short_changed:
                    lines.append(f"⚡ 短期訊號：{prev_short} → {short['label']}（{short['score']:.0f}）")
                    price_line = format_trade_levels(short.get("levels"))
                    if price_line:
                        lines.append(f"（短期）{price_line}")
                if long_changed:
                    lines.append(f"⚡ 中長期訊號：{prev_long} → {long_['label']}（{long_['score']:.0f}）")
                    price_line = format_trade_levels(long_.get("levels"))
                    if price_line:
                        lines.append(f"（中長期）{price_line}")
            lines.extend(f"• {r}" for r in short["reasons"][:3])
            changed_messages.append("\n".join(lines))

        state[ticker] = {"short_label": short["label"], "long_label": long_["label"], "price": price, "updated_at": now}

    save_state(state)
    if changed_messages:
        send_telegram("📊 <b>股票訊號更新</b>\n\n" + "\n\n".join(changed_messages))
    else:
        print(f"[{now}] 無訊號變更。")


def scan_universe_and_notify():
    """背景排程：掃描精選72隻股票（同 /api/scan 用緊嗰份 STOCK_UNIVERSE），
    短線／長期買入／賣出四張清單各自獨立追蹤，一旦有股票『新入選』就即刻 Telegram 通知。
    （唔係每次都推全部清單，淨係推『相比上次多咗嘅』，避免洗版。）"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        scored = compute_universe_scores()
    except Exception as e:
        print(f"[警告] 精選清單掃描失敗：{e}")
        return

    prev_state = load_scan_state()
    first_run = not prev_state
    current_state = {}
    new_entries = {"short_buy": [], "short_sell": [], "long_buy": [], "long_sell": []}

    for s in scored:
        t = s["ticker"]
        flags = {
            "short_buy": s["short_score"] >= 20,
            "short_sell": s["short_score"] <= -20,
            "long_buy": s["long_score"] >= 20,
            "long_sell": s["long_score"] <= -20,
        }
        current_state[t] = flags
        prev_flags = prev_state.get(t, {})
        if not first_run:
            for key, is_now in flags.items():
                if is_now and not prev_flags.get(key):
                    new_entries[key].append(s)

    save_scan_state(current_state)

    if first_run:
        print(f"[{now}] 精選清單首次掃描，已建立基準狀態（唔會推送通知）。")
        return

    section_labels = {
        "short_buy": "⚡ 短線新入選：建議買入",
        "short_sell": "⚡ 短線新入選：建議賣出",
        "long_buy": "🏔️ 長期新入選：建議買入",
        "long_sell": "🏔️ 長期新入選：建議賣出",
    }
    lines = []
    for key, label in section_labels.items():
        items = new_entries[key]
        if not items:
            continue
        lines.append(f"<b>{label}</b>")
        for s in items:
            score = s["short_score"] if key.startswith("short") else s["long_score"]
            levels = s["short_levels"] if key.startswith("short") else s["long_levels"]
            lines.append(f"• {s['ticker']} {s['name']}　現價 {fmt_price(s['price'])}　分數 {score:.0f}")
            price_line = format_trade_levels(levels, inline=True)
            if price_line:
                lines.append(f"　　{price_line}")

    if lines:
        send_telegram("📈 <b>精選清單掃描更新</b>（" + now + "）\n\n" + "\n".join(lines))
    else:
        print(f"[{now}] 精選清單無新入選股票。")


def fmt_price(price) -> str:
    return f"{price:.2f}" if isinstance(price, (int, float)) else "—"


def send_daily_morning_report():
    """每日早上（唔理訊號有冇變）主動推送一次『建議買入』清單嘅圖表。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        scored = compute_universe_scores()
    except Exception as e:
        print(f"[警告] 每日晨報掃描失敗：{e}")
        return

    short_buy = sorted([s for s in scored if s["short_score"] >= 20],
                        key=lambda s: -s["short_score"])[:15]
    long_buy = sorted([s for s in scored if s["long_score"] >= 20],
                       key=lambda s: -s["long_score"])[:15]

    if short_buy:
        chart_items = [{"ticker": s["ticker"], "score_for_chart": s["short_score"]} for s in short_buy]
        send_telegram_photo(build_quickchart_url(chart_items, "短期建議買入（分數）"),
                             f"☀️ <b>每日晨報 - 短期建議買入</b>（{now}）")
        detail_lines = []
        for s in short_buy[:10]:
            price_line = format_trade_levels(s.get("short_levels"), inline=True)
            detail_lines.append(f"• {s['ticker']} {s['name']}　現價 {fmt_price(s['price'])}"
                                 + (f"\n　　{price_line}" if price_line else ""))
        if detail_lines:
            send_telegram("☀️ <b>短期建議買入 - 參考價位</b>\n\n" + "\n".join(detail_lines))
    if long_buy:
        chart_items = [{"ticker": s["ticker"], "score_for_chart": s["long_score"]} for s in long_buy]
        send_telegram_photo(build_quickchart_url(chart_items, "中長期建議買入（分數）"),
                             f"🏔️ <b>每日晨報 - 中長期建議買入</b>（{now}）")
        detail_lines = []
        for s in long_buy[:10]:
            price_line = format_trade_levels(s.get("long_levels"), inline=True)
            detail_lines.append(f"• {s['ticker']} {s['name']}　現價 {fmt_price(s['price'])}"
                                 + (f"\n　　{price_line}" if price_line else ""))
        if detail_lines:
            send_telegram("🏔️ <b>中長期建議買入 - 參考價位</b>\n\n" + "\n".join(detail_lines))

    if not short_buy and not long_buy:
        send_telegram(f"☀️ <b>每日晨報</b>（{now}）\n目前無股票達到「建議買入」門檻（分數 ≥20）。")


scheduler = BackgroundScheduler()


@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(scan_watchlist_and_notify, "interval", minutes=POLL_INTERVAL_MINUTES, id="scan_job")
    scheduler.add_job(scan_universe_and_notify, "interval", minutes=POLL_INTERVAL_MINUTES,
                       id="scan_universe_job", next_run_time=datetime.now())
    # 每個交易日早上 8:30（香港時間）固定推送一次晨報，唔理訊號有冇變
    scheduler.add_job(send_daily_morning_report,
                       CronTrigger(day_of_week="mon-fri", hour=8, minute=30, timezone="Asia/Hong_Kong"),
                       id="daily_morning_report")
    scheduler.start()
    print(f"[啟動] 背景排程已啟動，每 {POLL_INTERVAL_MINUTES} 分鐘掃描一次追蹤清單同精選清單"
          f"（精選清單第一次會喺啟動後立即執行一次），並會喺平日 08:30（香港時間）推送每日晨報。")


@app.on_event("shutdown")
def stop_scheduler():
    scheduler.shutdown()


@app.get("/test-notify")
def test_notify():
    send_telegram("✅ 測試通知：如果你喺 Telegram 收到呢個訊息，代表 Bot 已經接通成功！")
    return {"status": "已發送測試訊息，請去 Telegram 檢查"}


@app.get("/test-morning-report")
def test_morning_report():
    send_daily_morning_report()
    return {"status": "已手動觸發一次每日晨報，請去 Telegram 檢查"}


# 前端靜態網頁（必須放在所有 /api 路由之後掛載）
app.mount("/", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="static")
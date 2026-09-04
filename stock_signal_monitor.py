#!/usr/bin/env python3
"""
stock_signal_monitor.py
========================
獨立嘅監察腳本：唔需要成個 FastAPI 伺服器長開，
啱晒用 cron / Windows工作排程器 / GitHub Actions 定時執行一次。

用法：
    1. 安裝套件： pip install yfinance pandas numpy requests
    2. 設定環境變數 TELEGRAM_BOT_TOKEN 及 TELEGRAM_CHAT_ID（見 README.md）
    3. 編輯下方 WATCHLIST，填入你想追蹤的股票代號
       美股例如： "AAPL", "TSLA"
       港股例如： "0700.HK", "9988.HK"
    4. 執行一次： python stock_signal_monitor.py
    5. 設定排程（cron / Task Scheduler / GitHub Actions）每天/每小時自動執行一次

本腳本邏輯與 main.py 完全一致（MA5/20/50/200、RSI14、MACD、布林帶、52週高低位）。
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# 1. 設定你想追蹤的股票代號（美股直接用代號；港股用 "XXXX.HK" 格式）
# ---------------------------------------------------------------------------
WATCHLIST = [
    "AAPL",
    "0700.HK",
]

STATE_FILE = Path(__file__).parent / "signal_state.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


# ---------------------------------------------------------------------------
# 技術指標計算
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

    return dict(
        close=close, volume=volume, sma5=sma5, sma20=sma20, sma50=sma50, sma200=sma200,
        rsi14=rsi14, histogram=histogram, bb_upper=bb_upper, bb_lower=bb_lower, vol_avg20=vol_avg20,
    )


def label_from_score(score: float) -> str:
    if score >= 30:
        return "建議買入"
    if score <= -30:
        return "建議賣出"
    return "觀望"


def compute_short_term(ind: dict) -> dict:
    i, p = -1, -2
    score, reasons = 0.0, []

    sma5, sma20 = ind["sma5"], ind["sma20"]
    if pd.notna(sma5.iloc[i]) and pd.notna(sma20.iloc[i]) and pd.notna(sma5.iloc[p]) and pd.notna(sma20.iloc[p]):
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

    rsi = ind["rsi14"].iloc[i]
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
    if pd.notna(hist.iloc[i]) and pd.notna(hist.iloc[p]):
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
        if price <= ind["bb_lower"].iloc[i] + 0.1 * width:
            score += 1.5; reasons.append("股價貼近布林帶下軌，短期超賣")
        elif price >= ind["bb_upper"].iloc[i] - 0.1 * width:
            score -= 1.5; reasons.append("股價貼近布林帶上軌，短期超買")

    if pd.notna(ind["vol_avg20"].iloc[i]) and ind["volume"].iloc[i] > ind["vol_avg20"].iloc[i] * 1.2:
        if ind["close"].iloc[i] > ind["close"].iloc[p]:
            score += 1; reasons.append("成交量放大且股價上升，量價配合")
        elif ind["close"].iloc[i] < ind["close"].iloc[p]:
            score -= 1; reasons.append("成交量放大但股價下跌，賣壓增加")

    norm = max(-100, min(100, (score / 8.5) * 100))
    return dict(score=norm, label=label_from_score(norm), reasons=reasons)


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
    pos = (close.iloc[i] - low52) / (high52 - low52 or 1)
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
    return dict(score=norm, label=label_from_score(norm), reasons=reasons)


# ---------------------------------------------------------------------------
# Telegram 通知
# ---------------------------------------------------------------------------
def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[警告] 未設定 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID，跳過推送，只在終端機顯示：")
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=15)
    if resp.status_code != 200:
        print(f"[錯誤] Telegram 發送失敗：{resp.status_code} {resp.text}", file=sys.stderr)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def analyze_ticker(ticker: str):
    df = yf.download(ticker, period="2y", interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty or len(df) < 20:
        print(f"[警告] {ticker} 數據不足或抓取失敗，略過")
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close"])
    ind = calc_indicators(df)
    short = compute_short_term(ind)
    long_ = compute_long_term(ind)
    price = float(ind["close"].iloc[-1])
    return {"price": price, "short": short, "long": long_}


def main():
    state = load_state()
    now = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")
    changed_messages = []

    for ticker in WATCHLIST:
        result = analyze_ticker(ticker)
        if result is None:
            continue

        prev = state.get(ticker, {})
        prev_short_label = prev.get("short_label")
        prev_long_label = prev.get("long_label")
        new_short_label = result["short"]["label"]
        new_long_label = result["long"]["label"]

        short_changed = prev_short_label is not None and prev_short_label != new_short_label
        long_changed = prev_long_label is not None and prev_long_label != new_long_label
        first_run = prev_short_label is None

        if short_changed or long_changed or first_run:
            lines = [f"<b>{ticker}</b> 現價 {result['price']:.2f}　({now})"]
            if first_run:
                lines.append(f"短期訊號：{new_short_label}（分數 {result['short']['score']:.0f}）")
                lines.append(f"中長期訊號：{new_long_label}（分數 {result['long']['score']:.0f}）")
            else:
                if short_changed:
                    lines.append(f"⚡ 短期訊號變更：{prev_short_label} → {new_short_label}（分數 {result['short']['score']:.0f}）")
                if long_changed:
                    lines.append(f"⚡ 中長期訊號變更：{prev_long_label} → {new_long_label}（分數 {result['long']['score']:.0f}）")
            lines.extend(f"• {r}" for r in result["short"]["reasons"][:3])
            changed_messages.append("\n".join(lines))

        state[ticker] = {
            "short_label": new_short_label,
            "long_label": new_long_label,
            "short_score": result["short"]["score"],
            "long_score": result["long"]["score"],
            "price": result["price"],
            "updated_at": now,
        }

    save_state(state)

    if changed_messages:
        send_telegram("📊 <b>股票訊號更新</b>\n\n" + "\n\n".join(changed_messages))
    else:
        print(f"[{now}] 無訊號變更，略過通知。")


if __name__ == "__main__":
    main()

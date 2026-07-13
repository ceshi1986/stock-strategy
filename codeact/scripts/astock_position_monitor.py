#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股盘中持仓监控脚本 — 交易时段每15分钟检查持仓涨跌幅并预警。

功能：
  - 通过新浪财经/腾讯财经免费行情接口获取实时股价
  - 仅在A股交易时段（周一至周五 9:30-11:30 / 13:00-15:00）执行
  - 任一持仓日涨跌幅超过阈值时触发预警（默认 ±5%）
  - 使用 SQLite 状态库同日去重，避免重复告警
  - result_mode=auto 时：触发预警→display_only(@主人)，未触发→no_reply

参数（codeact_args）：
  - result_mode : auto / display_only / notify / no_reply（默认 auto）
  - threshold   : 涨跌幅预警阈值百分比，默认 5（即 ±5%）
  - holdings    : 逗号分隔的持仓代码，默认 sz002709,sz002407,sz000426

状态表：
  - stock_alert_state（主键=日期+代码+方向，同日同股同方向只告警一次）
"""

import asyncio
import math
import re
import sqlite3
import sys
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from codeact_sdk import CodeActSDK

# ===== SDK 工具版本（由 Agent 填入实际值）=====
TOOL_SCHEMA_VERSIONS = {
    "codeact_fetch_web": "v1_2c8d0580b3f93a58",
    "codeact_search_web": "v1_5ac1b0eba8c26f2a",
}

# ===== 常量 =====
CST = timezone(timedelta(hours=8))
DB_PATH = "./codeact/output/astock_monitor_state.db"

SINA_API = "https://hq.sinajs.cn/list="
TENCENT_API = "https://qt.gtimg.cn/q="

# A股交易时段
MORNING_OPEN = (9, 30)
MORNING_CLOSE = (11, 30)
AFTERNOON_OPEN = (13, 0)
AFTERNOON_CLOSE = (15, 0)

# 2026年A股休市日期（节假日，持续更新）
HOLIDAYS_2026 = {
    # 元旦
    "2026-01-01", "2026-01-02", "2026-01-03",
    # 春节
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    # 清明
    "2026-04-04", "2026-04-05", "2026-04-06",
    # 劳动节
    "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
    # 端午
    "2026-05-30", "2026-05-31", "2026-06-01",
    # 中秋
    "2026-09-25", "2026-09-26", "2026-09-27",
    # 国庆
    "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
    "2026-10-05", "2026-10-06", "2026-10-07",
}

# 持仓代码 → 名称映射（API返回的名称也用作校验）
HOLDING_NAMES = {
    "sz002709": "天赐材料",
    "sz002407": "多氟多",
    "sz000426": "兴业银锡",
}


# ===== 工具函数 =====

def is_trading_time(now: Optional[datetime] = None) -> bool:
    """判断当前是否处于A股交易时段。"""
    if now is None:
        now = datetime.now(CST)
    # 周末
    if now.weekday() >= 5:
        return False
    # 节假日
    date_str = now.strftime("%Y-%m-%d")
    if date_str in HOLIDAYS_2026:
        return False
    # 时段判断
    t = (now.hour, now.minute)
    if MORNING_OPEN <= t <= MORNING_CLOSE:
        return True
    if AFTERNOON_OPEN <= t <= AFTERNOON_CLOSE:
        return True
    return False


def parse_holdings(raw: str) -> List[str]:
    """解析持仓代码参数，返回标准列表。"""
    codes = [c.strip().lower() for c in raw.split(",") if c.strip()]
    # 自动补全前缀
    result = []
    for code in codes:
        if code.startswith(("sz", "sh", "sz", "sh")):
            result.append(code)
        elif code.isdigit():
            prefix = "sz" if code.startswith(("0", "3")) else "sh"
            result.append(f"{prefix}{code}")
        else:
            result.append(code)
    return result


# ===== 数据源：新浪财经 =====

def fetch_sina(codes: List[str]) -> List[Dict[str, Any]]:
    """从新浪财经行情接口获取数据，返回解析后的列表。"""
    url = SINA_API + ",".join(codes)
    try:
        resp = requests.get(
            url,
            headers={"Referer": "https://finance.sina.com.cn"},
            timeout=10,
        )
        resp.encoding = "gbk"
        resp.raise_for_status()
    except Exception as e:
        print(f"[新浪] 请求失败: {e}")
        return []

    results = []
    for line in resp.text.strip().split("\n"):
        line = line.strip()
        if not line or '=""' in line:
            continue
        m = re.match(r'var hq_str_(s[zh]\d+)="(.+)"', line)
        if not m:
            continue
        code = m.group(1)
        fields = m.group(2).split(",")
        if len(fields) < 32:
            continue
        try:
            name = fields[0]
            open_price = float(fields[1])
            prev_close = float(fields[2])
            current = float(fields[3])
            high = float(fields[4])
            low = float(fields[5])
            volume = float(fields[8])
            amount = float(fields[9])
            date_str = fields[30]
            time_str = fields[31]

            if prev_close == 0:
                continue
            change = current - prev_close
            change_pct = round(change / prev_close * 100, 2)

            results.append({
                "code": code,
                "name": name,
                "open": open_price,
                "prev_close": prev_close,
                "current": current,
                "high": high,
                "low": low,
                "change": round(change, 2),
                "change_pct": change_pct,
                "volume": volume,
                "amount": amount,
                "date": date_str,
                "time": time_str,
                "source": "新浪财经",
            })
        except (ValueError, IndexError) as e:
            print(f"[新浪] 解析失败 {code}: {e}")
            continue
    return results


# ===== 数据源：腾讯财经 =====

def fetch_tencent(codes: List[str]) -> List[Dict[str, Any]]:
    """从腾讯财经行情接口获取数据，返回解析后的列表。"""
    url = TENCENT_API + ",".join(codes)
    try:
        resp = requests.get(url, timeout=10)
        resp.encoding = "gbk"
        resp.raise_for_status()
    except Exception as e:
        print(f"[腾讯] 请求失败: {e}")
        return []

    results = []
    for line in resp.text.strip().split(";"):
        line = line.strip()
        if not line or '=""' in line:
            continue
        m = re.match(r'v_(s[zh]\d+)="(.+)"', line)
        if not m:
            continue
        code = m.group(1)
        fields = m.group(2).split("~")
        if len(fields) < 35:
            continue
        try:
            name = fields[1]
            current = float(fields[3])
            prev_close = float(fields[4])
            open_price = float(fields[5])
            # 腾讯接口直接提供涨跌额和涨跌幅
            change_raw = fields[31]
            change_pct_raw = fields[32]
            high = float(fields[33]) if fields[33] else current
            low = float(fields[34]) if fields[34] else current

            if prev_close == 0:
                continue

            # 优先使用接口计算值，否则自行计算
            try:
                change = float(change_raw)
            except (ValueError, TypeError):
                change = round(current - prev_close, 2)
            try:
                change_pct = float(change_pct_raw)
            except (ValueError, TypeError):
                change_pct = round((current - prev_close) / prev_close * 100, 2)

            results.append({
                "code": code,
                "name": name,
                "open": open_price,
                "prev_close": prev_close,
                "current": current,
                "high": high,
                "low": low,
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "volume": 0,
                "amount": 0,
                "date": "",
                "time": "",
                "source": "腾讯财经",
            })
        except (ValueError, IndexError) as e:
            print(f"[腾讯] 解析失败 {code}: {e}")
            continue
    return results


# ===== 数据获取：主源 + 兜底 =====

async def fetch_quotes(sdk: CodeActSDK, codes: List[str]) -> List[Dict[str, Any]]:
    """两层降级获取行情：新浪→腾讯→搜索兜底。"""
    # 第一层：新浪财经
    quotes = fetch_sina(codes)
    if quotes and len(quotes) == len(codes):
        print(f"[数据源] 新浪财经成功，获取 {len(quotes)} 只股票")
        return quotes
    if quotes:
        print(f"[数据源] 新浪财经部分成功 {len(quotes)}/{len(codes)}，补腾讯")
    # 第二层：腾讯财经补全
    tencent_quotes = fetch_tencent(codes)
    if tencent_quotes:
        # 合并：新浪有的用新浪，没有的用腾讯
        sina_codes = {q["code"] for q in quotes}
        for tq in tencent_quotes:
            if tq["code"] not in sina_codes:
                quotes.append(tq)
        print(f"[数据源] 合并后共 {len(quotes)} 只股票")
    if quotes:
        return quotes
    # 第三层：搜索兜底
    print("[数据源] API均失败，尝试搜索兜底")
    return await _search_fallback(sdk, codes)


async def _search_fallback(sdk: CodeActSDK, codes: List[str]) -> List[Dict[str, Any]]:
    """搜索兜底：通过搜索获取股价信息。"""
    results = []
    for code in codes:
        name = HOLDING_NAMES.get(code, code)
        try:
            search = await sdk.call_tool(
                "codeact_search_web",
                {"query": f"{name} {code} 股票 实时行情 最新价", "response_length": "short"},
                schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
            )
            if not search or search.get("is_success") is False:
                continue
            items = search.get("results") or []
            if not items:
                continue
            # 尝试从搜索结果摘要提取价格
            snippet = items[0].get("snippet", "")
            price_match = re.search(r'(\d+\.\d+)', snippet)
            if price_match:
                results.append({
                    "code": code,
                    "name": name,
                    "current": float(price_match.group(1)),
                    "prev_close": 0,
                    "change_pct": 0,
                    "source": "搜索兜底",
                })
        except Exception as e:
            print(f"[搜索兜底] {code} 失败: {e}")
    return results


# ===== 状态库：同日告警去重 =====

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_alert_state (
            alert_date TEXT NOT NULL,
            code TEXT NOT NULL,
            direction TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (alert_date, code, direction)
        )
    """)
    conn.commit()
    return conn


def check_and_record_alert(
    conn: sqlite3.Connection, alert_date: str, code: str, direction: str
) -> bool:
    """检查是否已告警并记录。返回 True=本次新告警，False=已告警过。"""
    cur = conn.execute(
        "SELECT 1 FROM stock_alert_state WHERE alert_date=? AND code=? AND direction=?",
        (alert_date, code, direction),
    )
    if cur.fetchone():
        return False
    conn.execute(
        "INSERT OR IGNORE INTO stock_alert_state (alert_date, code, direction, created_at) VALUES (?, ?, ?, ?)",
        (alert_date, code, direction, datetime.now(CST).isoformat(timespec="seconds")),
    )
    conn.commit()
    return True


def cleanup_old_alerts(conn: sqlite3.Connection, keep_days: int = 7) -> None:
    """清理超过 keep_days 天的旧记录。"""
    cutoff = (datetime.now(CST) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn.execute("DELETE FROM stock_alert_state WHERE alert_date < ?", (cutoff,))
    conn.commit()


# ===== 消息构建 =====

def build_alert_message(
    alerts: List[Dict[str, Any]], all_quotes: List[Dict[str, Any]], threshold: float
) -> str:
    """构建预警消息。"""
    lines = ["⚠️ 持仓涨跌幅预警", ""]

    # 预警股票
    for a in alerts:
        arrow = "🔴" if a["direction"] == "down" else "🟢"
        lines.append(
            f"{arrow} {a['name']}({a['code'].upper()})  "
            f"现价 {a['current']:.2f}  "
            f"涨跌幅 {a['change_pct']:+.2f}%  "
            f"{'跌破' if a['direction'] == 'down' else '突破'} -{threshold}%/{threshold}%"
        )

    # 全部持仓概览
    lines.extend(["", "【持仓概览】"])
    lines.append(f"  {'股票':<16}{'现价':>8}{'涨跌幅':>10}{'今开':>8}{'最高':>8}{'最低':>8}")
    lines.append(f"  {'─'*16}{'─'*8}{'─'*10}{'─'*8}{'─'*8}{'─'*8}")
    for q in all_quotes:
        code_display = q["code"].upper()
        name_display = f"{q['name']}({code_display})"
        lines.append(
            f"  {name_display:<16}"
            f"{q['current']:>8.2f}"
            f"{q['change_pct']:>+9.2f}%"
            f"{q['open']:>8.2f}"
            f"{q['high']:>8.2f}"
            f"{q['low']:>8.2f}"
        )

    lines.extend(["", f"预警阈值：±{threshold}%"])
    return "\n".join(lines)


# ===== 主入口 =====

async def main() -> None:
    result_mode_raw = sys.argv[1] if len(sys.argv) > 1 else "auto"
    threshold_raw = sys.argv[2] if len(sys.argv) > 2 else "5"
    holdings_raw = sys.argv[3] if len(sys.argv) > 3 else "sz002709,sz002407,sz000426"

    sdk = CodeActSDK()
    try:
        # 参数解析
        mode = (result_mode_raw or "auto").strip().lower()
        if mode not in {"auto", "display_only", "notify", "no_reply"}:
            raise ValueError(f"result_mode 不合法: {mode}")
        threshold = float(threshold_raw)
        if threshold <= 0:
            raise ValueError("threshold 必须为正数")
        codes = parse_holdings(holdings_raw)

        print(f"[参数] result_mode={mode}, threshold={threshold}%, holdings={codes}")

        # 交易时段检查
        now = datetime.now(CST)
        if not is_trading_time(now):
            print(f"[时段] 当前 {now.strftime('%Y-%m-%d %H:%M:%S')} 非A股交易时段，跳过")
            if mode == "auto":
                actual_mode = "no_reply"
            else:
                actual_mode = mode
            await sdk.submit_result(
                result_mode=actual_mode,
                status="success",
                message="NO_REPLY",
            )
            return

        # 获取行情
        quotes = await fetch_quotes(sdk, codes)
        if not quotes:
            raise RuntimeError("所有数据源均无法获取行情数据")

        print(f"[行情] 成功获取 {len(quotes)}/{len(codes)} 只股票数据")
        for q in quotes:
            print(f"  {q['name']}({q['code']}) 现价={q['current']:.2f} 涨跌幅={q['change_pct']:+.2f}% 来源={q['source']}")

        # 判断预警
        alerts = []
        for q in quotes:
            if abs(q["change_pct"]) > threshold:
                direction = "down" if q["change_pct"] < 0 else "up"
                alerts.append({
                    "code": q["code"],
                    "name": q["name"],
                    "current": q["current"],
                    "change_pct": q["change_pct"],
                    "direction": direction,
                })

        # 状态库去重
        conn = init_db()
        cleanup_old_alerts(conn)
        today = now.strftime("%Y-%m-%d")
        new_alerts = []
        for a in alerts:
            is_new = check_and_record_alert(conn, today, a["code"], a["direction"])
            if is_new:
                new_alerts.append(a)
                print(f"[预警] 新增告警: {a['name']} {a['change_pct']:+.2f}% ({a['direction']})")
            else:
                print(f"[去重] 今日已告警: {a['name']} {a['direction']}")
        conn.close()

        # 结果提交
        if mode == "auto":
            actual_mode = "display_only" if new_alerts else "no_reply"
        else:
            actual_mode = mode

        if new_alerts:
            message = f"[主人](at://owner) " + build_alert_message(new_alerts, quotes, threshold)
        elif alerts and not new_alerts:
            # 有超阈值但今日已告警过，不重复打扰
            message = "NO_REPLY"
            if mode != "auto":
                # 非 auto 模式下如果用户主动查询，展示概览
                message = build_alert_message(alerts, quotes, threshold)
        else:
            message = "NO_REPLY"

        await sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message=message,
            data={
                "alert_count": len(new_alerts),
                "alert_codes": [a["code"] for a in new_alerts],
                "total_holdings": len(quotes),
                "threshold_pct": threshold,
                "trading_time": is_trading_time(now),
            },
        )
    except Exception as e:
        print(f"[错误] {e}\n{traceback.format_exc()}")
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"A股持仓监控执行失败：{e}",
        )


if __name__ == "__main__":
    asyncio.run(main())

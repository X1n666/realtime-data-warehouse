#!/usr/bin/env python3
"""Generate mock user action logs (JSON) for the behavior-log pipeline.

Realtime project copy (V2): adds event_id / event_time_ms / biz_date /
source_timezone fields and an --offset continuation counter for append mode,
without breaking the original fields. Offline repo copy stays frozen.

Output line format (one JSON per line):
    {"common": {"uid": 100001, "platform": "app", "province_id": 1,
                "ts": "2026-08-23 10:00:00"},
     "page": {"page_id": "home"},
     "action": {"action_id": "click", "item": 100101},
     "ts": "2026-08-23 10:00:00",
     "event_id": "<md5>", "event_time_ms": <epoch_ms>, "biz_date": "2026-08-23",
     "source_timezone": "Asia/Shanghai"}
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from datetime import datetime, timedelta, timezone

from generate_business_data import PROVINCE_IDS, SKU_CATALOG

# Asia/Shanghai = 固定 UTC+8（无夏令时），用固定偏移避免 Windows 缺少 IANA tzdata
SH_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")

PAGE_IDS = ["home", "detail", "category", "cart", "search", "mine"]
# 漏斗权重：launch(启动) 打底，浏览 > 点击 > 加购 > 下单 > 支付（与业务支付率 88% 呼应）
ACTION_IDS = ["launch", "browse", "click", "add_cart", "order", "pay"]
ACTION_WEIGHTS = [10, 30, 25, 15, 10, 5]

# uid 池 1000（日志域独立于业务用户表，只为放大漏斗形态）；
# 每日从池中抽取 active_users 人产生行为 → 不同天活跃集合不同，留存率自然 < 100%。
UID_POOL_SIZE = 1000


def build_line(
    rng: random.Random, ts: str, uid: int, seq: int,
    event_time_ms: int, biz_date: str, source_timezone: str,
) -> str:
    action_id = rng.choices(ACTION_IDS, weights=ACTION_WEIGHTS, k=1)[0]
    item = None
    if action_id in ("click", "add_cart", "order", "pay"):
        item = rng.choice(SKU_CATALOG)[0]
    page_id = "launch" if action_id == "launch" else rng.choice(PAGE_IDS)
    # event_id = md5(biz_date|seq|uid|ts|action_id|page_id|item)，seq 为文件内累积行号；
    # 同 seed + 同 --offset 可复现同 event_id；禁止 Flink 运行时自增（多并行不唯一、不可对齐）
    event_id = hashlib.md5(
        f"{biz_date}|{seq}|{uid}|{ts}|{action_id}|{page_id}|{item}".encode("utf-8")
    ).hexdigest()
    line = {
        "common": {
            "uid": uid,
            "platform": "app",
            "province_id": rng.choice(PROVINCE_IDS),
            "ts": ts,
        },
        "page": {"page_id": page_id},
        "action": {"action_id": action_id, "item": item},
        "ts": ts,
        "event_id": event_id,
        "event_time_ms": event_time_ms,
        "biz_date": biz_date,
        "source_timezone": source_timezone,
    }
    return json.dumps(line, ensure_ascii=False)


def resolve_timestamps(base_ts: datetime | None, i: int) -> tuple[str, int, str, str]:
    """返回 (ts字符串, event_time_ms(UTC epoch ms), biz_date, source_timezone)。

    字面回填模式（--ts 给定）：ts 逐秒+1 字面量无时区，按 Asia/Shanghai 解释转 UTC；
    实时模式（无 --ts）：ts 取 UTC 字符串（与原服务器 UTC 行为一致），标注 UTC。
    """
    if base_ts is not None:
        ts = (base_ts + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
        dt_aware = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=SH_TZ)
        event_time_ms = int(dt_aware.timestamp() * 1000)
        source_timezone = "Asia/Shanghai"
    else:
        now_utc = datetime.now(timezone.utc)
        ts = now_utc.strftime("%Y-%m-%d %H:%M:%S")
        event_time_ms = int(now_utc.timestamp() * 1000)
        source_timezone = "UTC"
    biz_date = datetime.fromtimestamp(event_time_ms / 1000, tz=SH_TZ).strftime("%Y-%m-%d")
    return ts, event_time_ms, biz_date, source_timezone


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mock user action logs (JSON).")
    parser.add_argument("--count", type=int, default=3000, help="Number of log lines to append.")
    parser.add_argument("--seed", type=int, default=20260823, help="Random seed.")
    parser.add_argument(
        "--output",
        default="logs/user_action_rt.log",
        help="Log file path (appended). Use '-' for stdout (pipe to Kafka producer).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Cumulative line counter start for event_id (must continue across "
        "append runs of the same seed, otherwise event_id collides).",
    )
    parser.add_argument(
        "--ts",
        default=None,
        help='Base timestamp "YYYY-MM-DD HH:MM:SS"; each line advances +1s. '
        "Omit to use current time (for Flume realtime tailing).",
    )
    parser.add_argument(
        "--active-users",
        type=int,
        default=500,
        help="Daily active users drawn from uid pool (default 500 of 1000). "
        "Different seed => different active set, so N-day retention < 100%%.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    base_ts = datetime.strptime(args.ts, "%Y-%m-%d %H:%M:%S") if args.ts else None
    # 当日活跃用户子集：固定由 seed 决定，多次追加同一天仍保持一致集合
    active_pool = rng.sample(range(1, UID_POOL_SIZE + 1), args.active_users)
    rng = random.Random(args.seed)  # 重摇：事件随机序列与活跃集合解耦

    if args.output == "-":
        for i in range(args.count):
            seq = args.offset + i
            ts, et_ms, biz_date, src_tz = resolve_timestamps(base_ts, i)
            uid = 100000 + rng.choice(active_pool)
            sys.stdout.write(build_line(rng, ts, uid, seq, et_ms, biz_date, src_tz) + "\n")
        print(f"Wrote {args.count} lines to stdout (offset {args.offset}->{args.offset + args.count - 1})",
              file=sys.stderr)
        return

    with open(args.output, "a", encoding="utf-8") as f:
        for i in range(args.count):
            seq = args.offset + i
            ts, et_ms, biz_date, src_tz = resolve_timestamps(base_ts, i)
            uid = 100000 + rng.choice(active_pool)
            f.write(build_line(rng, ts, uid, seq, et_ms, biz_date, src_tz) + "\n")

    print(f"Appended {args.count} lines to {args.output}")
    with open(args.output, encoding="utf-8") as f:
        first = f.readline().strip()
    print(f"Sample: {first}")


if __name__ == "__main__":
    main()

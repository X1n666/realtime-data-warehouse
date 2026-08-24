#!/usr/bin/env python3
"""Generate mock user action logs (JSON) for the behavior-log pipeline.

The generator appends JSON lines to a log file monitored by Flume taildir,
reusing the user pool and SKU catalog from generate_business_data.py so the
behavior chain can be joined against business data (uid 100001~100100).

Output line format (one JSON per line, trailing newline required by taildir):
    {"common": {"uid": 100001, "platform": "app", "province_id": 1,
                "ts": "2026-08-23 10:00:00"},
     "page": {"page_id": "home"},
     "action": {"action_id": "click", "item": 100101},
     "ts": "2026-08-23 10:00:00"}
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta

from generate_business_data import PROVINCE_IDS, SKU_CATALOG

PAGE_IDS = ["home", "detail", "category", "cart", "search", "mine"]
# 漏斗权重：launch(启动) 打底，浏览 > 点击 > 加购 > 下单 > 支付（与业务支付率 88% 呼应）
ACTION_IDS = ["launch", "browse", "click", "add_cart", "order", "pay"]
ACTION_WEIGHTS = [10, 30, 25, 15, 10, 5]

# uid 池 1000（日志域独立于业务用户表，只为放大漏斗形态）；
# 每日从池中抽取 active_users 人产生行为 → 不同天活跃集合不同，留存率自然 < 100%。
UID_POOL_SIZE = 1000


def build_line(rng: random.Random, ts: str, uid: int) -> str:
    action_id = rng.choices(ACTION_IDS, weights=ACTION_WEIGHTS, k=1)[0]
    item = None
    if action_id in ("click", "add_cart", "order", "pay"):
        item = rng.choice(SKU_CATALOG)[0]
    page_id = "launch" if action_id == "launch" else rng.choice(PAGE_IDS)
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
    }
    return json.dumps(line, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mock user action logs (JSON).")
    parser.add_argument("--count", type=int, default=3000, help="Number of log lines to append.")
    parser.add_argument("--seed", type=int, default=20260823, help="Random seed.")
    parser.add_argument(
        "--output",
        default="/home/csu/opt/logs/user_action.log",
        help="Log file path (appended, monitored by Flume taildir).",
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
        "Different seed => different active set, so N-day retention < 100%.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    base_ts = datetime.strptime(args.ts, "%Y-%m-%d %H:%M:%S") if args.ts else None
    # 当日活跃用户子集：固定由 seed 决定，多次追加同一天仍保持一致集合
    active_pool = rng.sample(range(1, UID_POOL_SIZE + 1), args.active_users)
    rng = random.Random(args.seed)  # 重摇：事件随机序列与活跃集合解耦

    with open(args.output, "a", encoding="utf-8") as f:
        for i in range(args.count):
            if base_ts is not None:
                ts = (base_ts + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            uid = 100000 + rng.choice(active_pool)
            f.write(build_line(rng, ts, uid) + "\n")

    print(f"Appended {args.count} lines to {args.output}")
    with open(args.output, encoding="utf-8") as f:
        first = f.readline().strip()
    print(f"Sample: {first}")


if __name__ == "__main__":
    main()

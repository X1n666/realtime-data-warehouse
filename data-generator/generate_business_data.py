#!/usr/bin/env python3
"""Generate deterministic MySQL seed data for the gmall business database.

The generator writes SQL instead of connecting to MySQL directly, so the output
can be reviewed before import and replayed safely during practice.
"""

from __future__ import annotations

import argparse
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path


SKU_CATALOG = [
    (100101, "星海 X1 8G+128G 黑色", Decimal("1999.00")),
    (100102, "星海 X1 12G+256G 银色", Decimal("2499.00")),
    (100201, "轻舟 Pro 14英寸 i5 16G", Decimal("5299.00")),
    (100202, "轻舟 Pro 16英寸 i7 32G", Decimal("7299.00")),
    (100301, "云厨 Air 电饭煲 4L", Decimal("399.00")),
    (200101, "森屿基础款 T 恤 黑色 L", Decimal("79.00")),
    (200201, "花序夏季连衣裙 米色 M", Decimal("239.00")),
    (200301, "逐风跑步鞋 白蓝 42码", Decimal("399.00")),
    (300101, "每日坚果礼盒 30袋", Decimal("129.00")),
    (300201, "冷萃咖啡组合 12瓶", Decimal("99.00")),
    (300301, "阳光橙礼盒 5kg", Decimal("88.00")),
]

PROVINCE_IDS = list(range(1, 13))
PAYMENT_TYPES = ["ALIPAY", "WECHAT", "CARD"]
GENDERS = ["M", "F"]
USER_LEVELS = ["NEW", "NORMAL", "VIP"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate gmall business SQL data.")
    parser.add_argument("--dt", required=True, help="Business date, format yyyy-MM-dd.")
    parser.add_argument("--users", type=int, default=100, help="Number of users to upsert.")
    parser.add_argument(
        "--changed-users",
        type=int,
        default=None,
        help="Only upsert this many users; omit it when initializing all users.",
    )
    parser.add_argument("--orders", type=int, default=200, help="Number of orders for dt.")
    parser.add_argument(
        "--user-pool",
        type=int,
        default=None,
        help=(
            "User pool referenced by orders; defaults to --users. "
            "Use --users 0 --user-pool 100 to generate orders only "
            "(users initialized once beforehand)."
        ),
    )
    parser.add_argument("--seed", type=int, default=20260809, help="Random seed.")
    parser.add_argument(
        "--output",
        default="data/generated/business_data.sql",
        help="Output SQL file path.",
    )
    return parser.parse_args()


def sql_quote(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def values_sql(rows: list[tuple[object, ...]]) -> str:
    return ",\n".join("(" + ", ".join(sql_quote(item) for item in row) + ")" for row in rows)


def random_datetime(rng: random.Random, day: date) -> datetime:
    second = rng.randint(8 * 3600, 23 * 3600 - 1)
    return datetime.combine(day, time()) + timedelta(seconds=second)


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def build_users(rng: random.Random, dt: date, user_count: int) -> list[tuple[object, ...]]:
    rows = []
    for idx in range(1, user_count + 1):
        user_id = 100000 + idx
        created_day = dt - timedelta(days=rng.randint(1, 60))
        created_at = random_datetime(rng, created_day)
        rows.append(
            (
                user_id,
                f"user_{user_id}",
                f"User{idx:05d}",
                "demo_pwd",
                f"Customer{idx:05d}",
                f"13{rng.randint(100000000, 999999999)}",
                f"user_{user_id}@example.com",
                f"/img/user/{user_id}.png",
                rng.choice(USER_LEVELS),
                created_day - timedelta(days=rng.randint(18 * 365, 35 * 365)),
                rng.choice(GENDERS),
                created_at,
                created_at,
                created_at,
            )
        )
    return rows


def build_orders(
    rng: random.Random, dt: date, user_count: int, order_count: int, user_pool: int | None = None
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], list[tuple[object, ...]], list[tuple[object, ...]]]:
    order_rows = []
    detail_rows = []
    payment_rows = []
    refund_rows = []
    dt_key = dt.strftime("%Y%m%d")
    pool = user_pool if user_pool is not None else user_count

    for idx in range(1, order_count + 1):
        order_id = int(f"{dt_key}{idx:05d}")
        user_id = 100000 + rng.randint(1, pool)
        province_id = rng.choice(PROVINCE_IDS)
        order_time = random_datetime(rng, dt)
        line_count = rng.randint(1, 3)
        selected_skus = rng.sample(SKU_CATALOG, line_count)

        original_total = Decimal("0.00")
        for line_no, (sku_id, sku_name, sku_price) in enumerate(selected_skus, start=1):
            sku_num = rng.randint(1, 3)
            original_total += sku_price * sku_num
            detail_rows.append(
                (
                    order_id * 10 + line_no,
                    order_id,
                    sku_id,
                    sku_name,
                    sku_price,
                    sku_num,
                    order_time,
                    order_time,
                )
            )

        benefit = money(original_total * Decimal(str(rng.choice([0, 0, 0.03, 0.05, 0.08]))))
        freight = Decimal(str(rng.choice([0, 0, 6, 8, 10])))
        total_amount = money(original_total - benefit + freight)
        paid = rng.random() < 0.88
        refunded = paid and rng.random() < 0.08
        order_status = "REFUNDED" if refunded else ("PAID" if paid else "UNPAID")
        process_status = "FINISHED" if paid else "CREATED"
        out_trade_no = f"OT{order_id}"

        order_rows.append(
            (
                order_id,
                f"Customer{user_id}",
                f"13{rng.randint(100000000, 999999999)}",
                total_amount,
                order_status,
                user_id,
                rng.choice(PAYMENT_TYPES) if paid else None,
                f"province-{province_id} demo road {rng.randint(1, 999)}",
                "",
                out_trade_no,
                f"order {order_id}",
                order_time,
                order_time,
                order_time + timedelta(hours=2),
                process_status,
                f"TRACK{order_id}" if paid else None,
                None,
                f"/img/order/{order_id}.png",
                province_id,
                benefit,
                money(original_total),
                freight,
                order_time,
            )
        )

        if paid:
            pay_time = order_time + timedelta(minutes=rng.randint(1, 60))
            payment_rows.append(
                (
                    order_id,
                    out_trade_no,
                    order_id,
                    user_id,
                    rng.choice(PAYMENT_TYPES),
                    f"PAY{order_id}",
                    total_amount,
                    f"order {order_id}",
                    "SUCCESS",
                    pay_time,
                    pay_time + timedelta(seconds=rng.randint(1, 30)),
                    pay_time,
                )
            )

        if refunded:
            sku_id = selected_skus[0][0]
            refund_amount = money(total_amount * Decimal(str(rng.choice([0.2, 0.5, 1]))))
            refund_time = order_time + timedelta(hours=rng.randint(3, 48))
            refund_rows.append(
                (
                    order_id,
                    user_id,
                    order_id,
                    sku_id,
                    "RETURN",
                    1,
                    refund_amount,
                    "QUALITY",
                    "demo refund",
                    "SUCCESS",
                    refund_time,
                    refund_time,
                )
            )

    return order_rows, detail_rows, payment_rows, refund_rows


def write_insert(
    lines: list[str],
    table: str,
    columns: list[str],
    rows: list[tuple[object, ...]],
    update_columns: list[str],
) -> None:
    if not rows:
        return
    lines.append(f"INSERT INTO {table} ({', '.join(columns)}) VALUES")
    lines.append(values_sql(rows))
    updates = ", ".join(f"{column} = VALUES({column})" for column in update_columns)
    lines.append(f"ON DUPLICATE KEY UPDATE {updates};")
    lines.append("")


def main() -> None:
    args = parse_args()
    if args.changed_users is not None and not 1 <= args.changed_users <= args.users:
        raise SystemExit("--changed-users must be between 1 and --users")

    dt = datetime.strptime(args.dt, "%Y-%m-%d").date()
    user_pool = args.user_pool if args.user_pool is not None else args.users
    if user_pool < 1:
        raise SystemExit("--user-pool must be >= 1")
    rng = random.Random(args.seed + int(dt.strftime("%Y%m%d")))

    users = build_users(rng, dt, args.users)
    users_to_upsert = users if args.changed_users is None else users[: args.changed_users]
    orders, details, payments, refunds = build_orders(rng, dt, args.users, args.orders, user_pool)

    lines = [
        "-- Generated by data-generator/generate_business_data.py",
        f"-- dt={args.dt}, users={args.users}, orders={args.orders}, seed={args.seed}",
        "USE gmall_rt;",
        "START TRANSACTION;",
        "",
    ]

    write_insert(
        lines,
        "user_info",
        [
            "id",
            "login_name",
            "nick_name",
            "passwd",
            "name",
            "phone_num",
            "email",
            "head_img",
            "user_level",
            "birthday",
            "gender",
            "create_time",
            "operate_time",
            "update_time",
        ],
        users_to_upsert,
        [
            "login_name",
            "nick_name",
            "passwd",
            "name",
            "phone_num",
            "email",
            "head_img",
            "user_level",
            "birthday",
            "gender",
            "create_time",
            "operate_time",
            "update_time",
        ],
    )

    write_insert(
        lines,
        "order_info",
        [
            "id",
            "consignee",
            "consignee_tel",
            "total_amount",
            "order_status",
            "user_id",
            "payment_way",
            "delivery_address",
            "order_comment",
            "out_trade_no",
            "trade_body",
            "create_time",
            "operate_time",
            "expire_time",
            "process_status",
            "tracking_no",
            "parent_order_id",
            "img_url",
            "province_id",
            "benefit_reduce_amount",
            "original_total_amount",
            "feight_fee",
            "update_time",
        ],
        orders,
        [
            "consignee",
            "consignee_tel",
            "total_amount",
            "order_status",
            "user_id",
            "payment_way",
            "delivery_address",
            "operate_time",
            "process_status",
            "tracking_no",
            "province_id",
            "benefit_reduce_amount",
            "original_total_amount",
            "feight_fee",
            "update_time",
        ],
    )

    write_insert(
        lines,
        "order_detail",
        [
            "id",
            "order_id",
            "sku_id",
            "sku_name",
            "order_price",
            "sku_num",
            "create_time",
            "update_time",
        ],
        details,
        ["order_id", "sku_id", "sku_name", "order_price", "sku_num", "create_time", "update_time"],
    )

    write_insert(
        lines,
        "payment_info",
        [
            "id",
            "out_trade_no",
            "order_id",
            "user_id",
            "payment_type",
            "trade_no",
            "payment_amount",
            "subject",
            "payment_status",
            "create_time",
            "callback_time",
            "update_time",
        ],
        payments,
        [
            "out_trade_no",
            "order_id",
            "user_id",
            "payment_type",
            "trade_no",
            "payment_amount",
            "subject",
            "payment_status",
            "create_time",
            "callback_time",
            "update_time",
        ],
    )

    write_insert(
        lines,
        "order_refund_info",
        [
            "id",
            "user_id",
            "order_id",
            "sku_id",
            "refund_type",
            "refund_num",
            "refund_amount",
            "refund_reason_type",
            "refund_reason_txt",
            "refund_status",
            "create_time",
            "update_time",
        ],
        refunds,
        [
            "user_id",
            "order_id",
            "sku_id",
            "refund_type",
            "refund_num",
            "refund_amount",
            "refund_reason_type",
            "refund_reason_txt",
            "refund_status",
            "create_time",
            "update_time",
        ],
    )

    lines.extend(["COMMIT;", ""])

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"Wrote {output_path}")
    print(
        f"users_generated={len(users)} users_upserted={len(users_to_upsert)} "
        f"orders={len(orders)} details={len(details)} payments={len(payments)} refunds={len(refunds)}"
    )


if __name__ == "__main__":
    main()

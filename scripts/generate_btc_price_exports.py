#!/usr/bin/env python3
"""Generate BTC-normalized price exports with granular spread metrics.

This script reads the latest raw price snapshot produced by the ingest pipeline
and emits three derived datasets under the same directory:

1. game_prices_latest_btc.csv        – Aggregated per-sellable view (legacy)
2. game_prices_latest_btc_summary.csv – Spread-focused summary metrics
3. game_prices_latest_btc_points.csv  – One row per jurisdiction price point

All derived datasets use BTC as the canonical base currency while retaining the
underlying local currency amounts for additional business logic (postage, VPN,
etc.).
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.request import Request, urlopen

getcontext().prec = 40

FX_API_URL = "https://open.er-api.com/v6/latest/USD"
BTC_PRICE_URL = "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD"
USER_AGENT = "GameCompare/price-export (+https://github.com/lowkey/i-miss-rust)"

BTC_QUANT = Decimal("0.00000001")
USD_QUANT = Decimal("0.01")


@dataclass
class PricePoint:
    amount_minor: Decimal
    minor_unit: int
    amount_local: Decimal
    amount_usd: Decimal
    amount_btc: Decimal
    currency: str
    country_iso2: str
    country_name: str
    region_label: str
    recorded_at_raw: str

    def formatted_local(self) -> str:
        if self.minor_unit <= 0:
            quant = Decimal(1)
            value = self.amount_local.quantize(quant, rounding=ROUND_HALF_UP)
            return f"{value:.0f}"
        quant = Decimal(10) ** (-self.minor_unit)
        value = self.amount_local.quantize(quant, rounding=ROUND_HALF_UP)
        return f"{value:.{self.minor_unit}f}"

    def formatted_usd(self) -> str:
        value = self.amount_usd.quantize(USD_QUANT, rounding=ROUND_HALF_UP)
        return f"{value:.2f}"

    def formatted_btc(self) -> str:
        value = self.amount_btc.quantize(BTC_QUANT, rounding=ROUND_HALF_UP)
        return f"{value:.8f}"

    def to_json_dict(self) -> Dict[str, object]:
        return {
            "amount_btc": self.formatted_btc(),
            "amount_local": self.formatted_local(),
            "amount_minor": int(self.amount_minor),
            "currency": self.currency,
            "minor_unit": self.minor_unit,
            "country": self.country_iso2,
            "country_name": self.country_name,
            "region_label": self.region_label,
            "recorded_at": self.recorded_at_raw,
        }


@dataclass
class SellableRecord:
    sellable_id: str
    sellable_kind: str
    title: str
    platform_code: str
    platform_name: str
    retailer_slug: str
    retailer_name: str
    product_slug: str
    product_name: str
    price_points: List[PricePoint]
    last_recorded_at: Optional[datetime]

    def min_point(self) -> PricePoint:
        return min(self.price_points, key=lambda p: p.amount_btc)

    def max_point(self) -> PricePoint:
        return max(self.price_points, key=lambda p: p.amount_btc)

    def min_btc(self) -> Decimal:
        return self.min_point().amount_btc

    def max_btc(self) -> Decimal:
        return self.max_point().amount_btc

    def spread_btc(self) -> Decimal:
        return self.max_btc() - self.min_btc()

    def spread_pct(self) -> Decimal:
        min_val = self.min_btc()
        if min_val == 0:
            return Decimal(0)
        return (self.max_btc() / min_val) - Decimal(1)


def fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req) as resp:  # noqa: S310 - trusted endpoints defined above
        return json.load(resp)


def fetch_rates() -> Tuple[Dict[str, Decimal], str, Decimal]:
    fx_data = fetch_json(FX_API_URL)
    if fx_data.get("result") != "success":  # pragma: no cover - sanity guard
        raise SystemExit(f"FX API error: {fx_data}")
    rates = {code: Decimal(str(value)) for code, value in fx_data["rates"].items()}
    fx_timestamp = fx_data.get("time_last_update_utc") or fx_data.get("time_next_update_utc") or ""

    btc_data = fetch_json(BTC_PRICE_URL)
    try:
        btc_usd_price = Decimal(str(btc_data["USD"]))
    except Exception as exc:  # pragma: no cover - defensive guard
        raise SystemExit(f"BTC price fetch failed: {btc_data}") from exc

    return rates, fx_timestamp, btc_usd_price


def format_btc(value: Decimal) -> str:
    return f"{value.quantize(BTC_QUANT, rounding=ROUND_HALF_UP):.8f}"


def format_pct(value: Decimal) -> str:
    percent = (value * Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{percent:.2f}"


def choose_title(row: Dict[str, str]) -> str:
    title = row.get("software_title") or ""
    if title:
        return title
    model = row.get("console_model") or ""
    variant = row.get("console_variant") or ""
    if model:
        return f"{model}{f' ({variant})' if variant else ''}"
    return row.get("product_name") or row.get("product_slug") or "Unknown"


def load_sellables(raw_path: Path, rates: Dict[str, Decimal], btc_usd_price: Decimal) -> List[SellableRecord]:
    sellables: Dict[Tuple[str, str, str, str, str, str, str, str, str], SellableRecord] = {}

    with raw_path.open(newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            currency = row["currency_code"]
            if currency not in rates:
                raise SystemExit(f"Missing FX rate for currency: {currency}")
            rate = rates[currency]
            if rate == 0:
                raise SystemExit(f"Zero FX rate for currency: {currency}")

            try:
                minor_unit = int(row.get("currency_minor_unit") or 2)
            except ValueError:
                minor_unit = 2

            amount_minor = Decimal(row["amount_minor"])
            divisor = Decimal(10) ** minor_unit
            amount_local = amount_minor / divisor
            amount_usd = amount_local / rate
            amount_btc = amount_usd / btc_usd_price

            country_code = row.get("country_iso2") or ""
            recorded_at_raw = row.get("recorded_at") or ""
            recorded_at = None
            if recorded_at_raw:
                try:
                    recorded_at = datetime.fromisoformat(recorded_at_raw)
                except ValueError:  # pragma: no cover - malformed timestamp guard
                    recorded_at = None

            key = (
                row["sellable_id"],
                row["sellable_kind"],
                row["retailer_slug"],
                row["retailer_name"],
                row.get("product_slug") or "",
                row.get("product_name") or "",
                row.get("platform_code") or "",
                row.get("platform_name") or "",
                choose_title(row),
            )

            price_point = PricePoint(
                amount_minor=amount_minor,
                minor_unit=minor_unit,
                amount_local=amount_local,
                amount_usd=amount_usd,
                amount_btc=amount_btc,
                currency=currency,
                country_iso2=country_code,
                country_name=row.get("country_name") or "",
                region_label=row.get("region_label") or "",
                recorded_at_raw=recorded_at_raw,
            )

            record = sellables.get(key)
            if not record:
                record = SellableRecord(
                    sellable_id=key[0],
                    sellable_kind=key[1],
                    title=key[8],
                    platform_code=key[6],
                    platform_name=key[7],
                    retailer_slug=key[2],
                    retailer_name=key[3],
                    product_slug=key[4],
                    product_name=key[5],
                    price_points=[],
                    last_recorded_at=recorded_at,
                )
                sellables[key] = record

            if recorded_at and (record.last_recorded_at is None or recorded_at > record.last_recorded_at):
                record.last_recorded_at = recorded_at

            record.price_points.append(price_point)

    return list(sellables.values())


def write_aggregated(path: Path, sellables: Iterable[SellableRecord], btc_usd_reference: Decimal, fx_timestamp: str) -> None:
    fieldnames = [
        "sellable_id",
        "sellable_kind",
        "title",
        "platform_code",
        "platform_name",
        "retailer_slug",
        "retailer_name",
        "product_slug",
        "product_name",
        "base_currency",
        "price_point_count",
        "price_points_json",
        "min_amount_btc",
        "max_amount_btc",
        "last_recorded_at",
        "btc_usd_reference",
        "fx_timestamp",
    ]

    rows = []
    for record in sellables:
        points_sorted = sorted(record.price_points, key=lambda p: p.amount_btc)
        rows.append({
            "sellable_id": record.sellable_id,
            "sellable_kind": record.sellable_kind,
            "title": record.title,
            "platform_code": record.platform_code,
            "platform_name": record.platform_name,
            "retailer_slug": record.retailer_slug,
            "retailer_name": record.retailer_name,
            "product_slug": record.product_slug,
            "product_name": record.product_name,
            "base_currency": "BTC",
            "price_point_count": len(points_sorted),
            "price_points_json": json.dumps([p.to_json_dict() for p in points_sorted], ensure_ascii=False, separators=(",", ":")),
            "min_amount_btc": format_btc(record.min_btc()),
            "max_amount_btc": format_btc(record.max_btc()),
            "last_recorded_at": record.last_recorded_at.isoformat() if record.last_recorded_at else "",
            "btc_usd_reference": f"{btc_usd_reference.quantize(USD_QUANT, rounding=ROUND_HALF_UP):.2f}",
            "fx_timestamp": fx_timestamp,
        })

    rows.sort(key=lambda row: (row["title"].lower(), row["retailer_slug"]))

    with path.open("w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, sellables: Iterable[SellableRecord], btc_usd_reference: Decimal, fx_timestamp: str) -> None:
    fieldnames = [
        "sellable_id",
        "sellable_kind",
        "title",
        "platform_code",
        "platform_name",
        "retailer_slug",
        "retailer_name",
        "product_slug",
        "product_name",
        "price_point_count",
        "min_amount_btc",
        "max_amount_btc",
        "spread_btc",
        "spread_percent",
        "cheapest_region",
        "costliest_region",
        "last_recorded_at",
        "btc_usd_reference",
        "fx_timestamp",
    ]

    rows = []
    for record in sellables:
        min_point = record.min_point()
        max_point = record.max_point()
        rows.append({
            "sellable_id": record.sellable_id,
            "sellable_kind": record.sellable_kind,
            "title": record.title,
            "platform_code": record.platform_code,
            "platform_name": record.platform_name,
            "retailer_slug": record.retailer_slug,
            "retailer_name": record.retailer_name,
            "product_slug": record.product_slug,
            "product_name": record.product_name,
            "price_point_count": len(record.price_points),
            "min_amount_btc": format_btc(record.min_btc()),
            "max_amount_btc": format_btc(record.max_btc()),
            "spread_btc": format_btc(record.spread_btc()),
            "spread_percent": format_pct(record.spread_pct()),
            "cheapest_region": min_point.region_label or min_point.country_iso2,
            "costliest_region": max_point.region_label or max_point.country_iso2,
            "last_recorded_at": record.last_recorded_at.isoformat() if record.last_recorded_at else "",
            "btc_usd_reference": f"{btc_usd_reference.quantize(USD_QUANT, rounding=ROUND_HALF_UP):.2f}",
            "fx_timestamp": fx_timestamp,
        })

    rows.sort(key=lambda row: (row["title"].lower(), row["retailer_slug"]))

    with path.open("w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_points(path: Path, sellables: Iterable[SellableRecord], btc_usd_reference: Decimal, fx_timestamp: str) -> None:
    fieldnames = [
        "sellable_id",
        "sellable_kind",
        "title",
        "platform_code",
        "platform_name",
        "retailer_slug",
        "retailer_name",
        "product_slug",
        "product_name",
        "country_iso2",
        "country_name",
        "region_label",
        "currency_code",
        "currency_minor_unit",
        "amount_minor",
        "amount_local",
        "amount_usd",
        "amount_btc",
        "delta_from_min_btc",
        "delta_pct_vs_min",
        "is_min_price",
        "recorded_at",
        "btc_usd_reference",
        "fx_timestamp",
    ]

    rows = []
    for record in sellables:
        min_btc = record.min_btc()
        for point in record.price_points:
            delta = point.amount_btc - min_btc
            delta_pct = Decimal(0)
            if min_btc != 0:
                delta_pct = (point.amount_btc / min_btc) - Decimal(1)

            rows.append({
                "sellable_id": record.sellable_id,
                "sellable_kind": record.sellable_kind,
                "title": record.title,
                "platform_code": record.platform_code,
                "platform_name": record.platform_name,
                "retailer_slug": record.retailer_slug,
                "retailer_name": record.retailer_name,
                "product_slug": record.product_slug,
                "product_name": record.product_name,
                "country_iso2": point.country_iso2,
                "country_name": point.country_name,
                "region_label": point.region_label,
                "currency_code": point.currency,
                "currency_minor_unit": point.minor_unit,
                "amount_minor": int(point.amount_minor),
                "amount_local": point.formatted_local(),
                "amount_usd": point.formatted_usd(),
                "amount_btc": point.formatted_btc(),
                "delta_from_min_btc": format_btc(delta if delta >= 0 else Decimal(0)),
                "delta_pct_vs_min": format_pct(delta_pct),
                "is_min_price": "1" if point.amount_btc == min_btc else "0",
                "recorded_at": point.recorded_at_raw,
                "btc_usd_reference": f"{btc_usd_reference.quantize(USD_QUANT, rounding=ROUND_HALF_UP):.2f}",
                "fx_timestamp": fx_timestamp,
            })

    rows.sort(key=lambda row: (
        row["title"].lower(),
        row["retailer_slug"],
        Decimal(row["amount_btc"])
    ))

    with path.open("w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw",
        default="tmp_ingest/all_prices_latest.csv",
        type=Path,
        help="Path to the raw price snapshot CSV",
    )
    parser.add_argument(
        "--out-dir",
        default="tmp_ingest",
        type=Path,
        help="Directory for derived exports",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_path: Path = args.raw
    out_dir: Path = args.out_dir

    if not raw_path.exists():
        raise SystemExit(f"Raw snapshot not found: {raw_path}")
    out_dir.mkdir(parents=True, exist_ok=True)

    rates, fx_timestamp, btc_usd_price = fetch_rates()
    sellables = load_sellables(raw_path, rates, btc_usd_price)

    aggregated_path = out_dir / "game_prices_latest_btc.csv"
    summary_path = out_dir / "game_prices_latest_btc_summary.csv"
    points_path = out_dir / "game_prices_latest_btc_points.csv"

    write_aggregated(aggregated_path, sellables, btc_usd_price, fx_timestamp)
    write_summary(summary_path, sellables, btc_usd_price, fx_timestamp)
    write_points(points_path, sellables, btc_usd_price, fx_timestamp)

    print(f"Aggregated export written to {aggregated_path}")
    print(f"Summary export written to    {summary_path}")
    print(f"Price-point export written to {points_path}")
    print(f"BTC/USD reference: {btc_usd_price.quantize(USD_QUANT, rounding=ROUND_HALF_UP):.2f}")
    print(f"FX timestamp: {fx_timestamp}")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent
DB_PATH = ROOT / "tracker.db"
LOGS_DIR = ROOT / "logs"

VAT_RATE = 0.22

DEFAULT_SOURCE_WEIGHTS = {
    "cex": 0.45,
    "ebay": 0.35,
    "subito": 0.20,
}


@dataclass
class PriceBucket:
    family: str
    canonical_model: str
    cex: list[float]
    ebay: list[float]
    subito: list[float]


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2:
        return float(sorted_vals[mid])
    return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2)


def _trimmed(values: list[float], trim_ratio: float = 0.15) -> list[float]:
    if len(values) < 6:
        return sorted(values)
    sorted_vals = sorted(values)
    cut = int(len(sorted_vals) * trim_ratio)
    if cut <= 0:
        return sorted_vals
    if len(sorted_vals) <= cut * 2:
        return sorted_vals
    return sorted_vals[cut:-cut]


def _safe_key(canonical_model: str | None, family: str | None) -> str:
    canonical = (canonical_model or "").strip()
    if canonical and canonical != "unknown":
        return canonical
    fam = (family or "other").strip() or "other"
    return f"family:{fam}"


def _collect_cex() -> dict[str, tuple[str, list[float]]]:
    if not DB_PATH.exists():
        return {}

    query = """
        SELECT canonical_model, console_family, last_price
        FROM products
        WHERE source = 'cex'
          AND last_available = 1
          AND last_price > 0
          AND model_segment = 'base'
          AND edition_class = 'standard'
    """

    buckets: dict[str, tuple[str, list[float]]] = {}
    with _connect(DB_PATH) as conn:
        rows = conn.execute(query).fetchall()

    for row in rows:
        key = _safe_key(row["canonical_model"], row["console_family"])
        family = row["console_family"] or "other"
        price = float(row["last_price"])
        if key not in buckets:
            buckets[key] = (family, [])
        buckets[key][1].append(price)

    return buckets


def _collect_subito() -> dict[str, tuple[str, list[float]]]:
    if not DB_PATH.exists():
        return {}

    query = """
        SELECT canonical_model, console_family, last_price
        FROM ads
        WHERE last_available = 1
          AND last_price > 0
          AND model_segment = 'base'
          AND edition_class = 'standard'
    """

    buckets: dict[str, tuple[str, list[float]]] = {}
    with _connect(DB_PATH) as conn:
        rows = conn.execute(query).fetchall()

    for row in rows:
        key = _safe_key(row["canonical_model"], row["console_family"])
        family = row["console_family"] or "other"
        price = float(row["last_price"])
        if key not in buckets:
            buckets[key] = (family, [])
        buckets[key][1].append(price)

    return buckets


def _collect_ebay() -> dict[str, tuple[str, list[float]]]:
    if not DB_PATH.exists():
        return {}

    query = """
        SELECT canonical_model, console_family, sold_price
        FROM sold_items
        WHERE sold_price > 0
          AND model_segment = 'base'
          AND edition_class = 'standard'
    """

    buckets: dict[str, tuple[str, list[float]]] = {}
    with _connect(DB_PATH) as conn:
        rows = conn.execute(query).fetchall()

    for row in rows:
        key = _safe_key(row["canonical_model"], row["console_family"])
        family = row["console_family"] or "other"
        price = float(row["sold_price"])
        if key not in buckets:
            buckets[key] = (family, [])
        buckets[key][1].append(price)

    return buckets


def _resolve_weights(
    weights: dict[str, float] | None = None,
    *,
    include_ebay: bool = True,
) -> dict[str, float]:
    base = dict(DEFAULT_SOURCE_WEIGHTS)
    if weights:
        for key in ("cex", "ebay", "subito"):
            if key in weights:
                try:
                    base[key] = max(0.0, float(weights[key]))
                except (TypeError, ValueError):
                    continue

    if not include_ebay:
        base["ebay"] = 0.0

    total = sum(base.values())
    if total <= 0:
        return {"cex": 0.5, "ebay": 0.0 if not include_ebay else 0.25, "subito": 0.5 if not include_ebay else 0.25}
    return {k: v / total for k, v in base.items()}


def compute_fair_values(
    *,
    weights: dict[str, float] | None = None,
    include_ebay: bool = True,
) -> dict:
    active_weights = _resolve_weights(weights, include_ebay=include_ebay)
    cex = _collect_cex()
    subito = _collect_subito()
    ebay = _collect_ebay()

    keys = sorted(set(cex.keys()) | set(subito.keys()) | set(ebay.keys()))
    result: list[dict] = []

    for key in keys:
        family = (
            (cex.get(key) or (None, []))[0]
            or (subito.get(key) or (None, []))[0]
            or (ebay.get(key) or ("other", []))[0]
        )

        cex_vals_raw = (cex.get(key) or (None, []))[1]
        subito_vals_raw = (subito.get(key) or (None, []))[1]
        ebay_vals_raw = (ebay.get(key) or (None, []))[1]

        cex_vals = _trimmed(cex_vals_raw)
        subito_vals = _trimmed(subito_vals_raw)
        ebay_vals = _trimmed(ebay_vals_raw)

        cex_med_gross = _median(cex_vals)
        cex_med_net = cex_med_gross * (1.0 - VAT_RATE) if cex_med_gross else None
        subito_med = _median(subito_vals)
        ebay_med = _median(ebay_vals)

        weighted_sum = 0.0
        weight_used = 0.0

        if cex_med_net is not None and active_weights["cex"] > 0:
            weighted_sum += active_weights["cex"] * cex_med_net
            weight_used += active_weights["cex"]
        if ebay_med is not None and active_weights["ebay"] > 0:
            weighted_sum += active_weights["ebay"] * ebay_med
            weight_used += active_weights["ebay"]
        if subito_med is not None and active_weights["subito"] > 0:
            weighted_sum += active_weights["subito"] * subito_med
            weight_used += active_weights["subito"]

        if weight_used == 0:
            continue

        fair_value = weighted_sum / weight_used

        medians = [v for v in (cex_med_net, ebay_med, subito_med) if v is not None]
        spread = (max(medians) - min(medians)) if len(medians) >= 2 else 0.0

        sample_count = len(cex_vals_raw) + len(subito_vals_raw) + len(ebay_vals_raw)
        source_count = sum(v is not None for v in (cex_med_net, ebay_med, subito_med))

        confidence = 0.3 + (0.18 * source_count) + min(sample_count, 60) * 0.006
        confidence = max(0.0, min(confidence, 1.0))

        result.append(
            {
                "key": key,
                "console_family": family,
                "canonical_model": None if key.startswith("family:") else key,
                "fallback_family_key": key.split(":", 1)[1] if key.startswith("family:") else family,
                "fair_value": round(fair_value, 2),
                "spread": round(spread, 2),
                "confidence": round(confidence, 3),
                "sample_count": sample_count,
                "samples": {
                    "cex": len(cex_vals_raw),
                    "ebay": len(ebay_vals_raw),
                    "subito": len(subito_vals_raw),
                },
                "medians": {
                    "cex_net": round(cex_med_net, 2) if cex_med_net is not None else None,
                    "ebay": round(ebay_med, 2) if ebay_med is not None else None,
                    "subito": round(subito_med, 2) if subito_med is not None else None,
                },
                "weights": active_weights,
            }
        )

    result.sort(key=lambda x: (x["console_family"], x["key"]))
    return {
        "total_models": len(result),
        "include_ebay": include_ebay,
        "weights": active_weights,
        "values": result,
    }


def _build_lookup(values: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    by_canonical: dict[str, dict] = {}
    by_family: dict[str, dict] = {}

    for item in values:
        canonical = item.get("canonical_model")
        family = item.get("fallback_family_key") or item.get("console_family") or "other"
        if canonical:
            by_canonical[canonical] = item

        # family fallback: prefer higher confidence, then larger sample
        current = by_family.get(family)
        if current is None:
            by_family[family] = item
        else:
            left = (item.get("confidence", 0), item.get("sample_count", 0))
            right = (current.get("confidence", 0), current.get("sample_count", 0))
            if left > right:
                by_family[family] = item

    return by_canonical, by_family


def score_subito_opportunities(limit: int = 300) -> dict:
    fair = compute_fair_values()
    by_canonical, by_family = _build_lookup(fair["values"])

    if not DB_PATH.exists():
        return {"total": 0, "items": []}

    query = """
        SELECT
            id, urn_id, name, console_family, canonical_model,
            model_segment, edition_class, seller_type,
            city, region, image_url,
            last_price, last_available, published_at, url
        FROM ads
        WHERE last_available = 1
          AND last_price > 0
        ORDER BY last_price ASC
        LIMIT ?
    """

    with _connect(DB_PATH) as conn:
        rows = conn.execute(query, (int(limit),)).fetchall()

    items: list[dict] = []
    for row in rows:
        ad = dict(row)
        canonical = ad.get("canonical_model")
        family = ad.get("console_family") or "other"

        fv_item = by_canonical.get(canonical) if canonical else None
        if fv_item is None:
            fv_item = by_family.get(family)
        if fv_item is None:
            continue

        fair_value = float(fv_item["fair_value"])
        price = float(ad["last_price"])
        delta_pct = ((fair_value - price) / fair_value) * 100 if fair_value > 0 else 0.0

        quality = 45.0
        if ad.get("image_url"):
            quality += 12
        if ad.get("city"):
            quality += 5
        if ad.get("region"):
            quality += 4
        if ad.get("seller_type") == "professionale":
            quality += 6
        if ad.get("model_segment") == "base" and ad.get("edition_class") == "standard":
            quality += 8
        if price < fair_value * 0.45:
            quality -= 18
        if ad.get("console_family") == "other":
            quality -= 14

        quality = max(0.0, min(100.0, quality))

        opportunity = max(0.0, min(100.0, (delta_pct * 0.7) + (quality * 0.3)))

        items.append(
            {
                "ad_id": ad["id"],
                "urn_id": ad["urn_id"],
                "name": ad["name"],
                "console_family": family,
                "canonical_model": canonical,
                "price": round(price, 2),
                "fair_value": round(fair_value, 2),
                "delta_pct": round(delta_pct, 2),
                "quality_score": round(quality, 1),
                "opportunity_score": round(opportunity, 1),
                "confidence": fv_item["confidence"],
                "city": ad.get("city") or "",
                "region": ad.get("region") or "",
                "seller_type": ad.get("seller_type") or "",
                "published_at": ad.get("published_at") or "",
                "url": ad.get("url") or "",
            }
        )

    items.sort(key=lambda x: x["opportunity_score"], reverse=True)
    return {
        "total": len(items),
        "items": items,
    }


def explain_fair_values(limit: int = 100) -> dict:
    fair = compute_fair_values()
    lines: list[dict] = []

    for item in fair["values"][: int(limit)]:
        med = item["medians"]
        smp = item["samples"]
        explanation = (
            f"Fair value {item['fair_value']:.2f} EUR da "
            f"CEX net={med['cex_net']} (n={smp['cex']}), "
            f"eBay={med['ebay']} (n={smp['ebay']}), "
            f"Subito={med['subito']} (n={smp['subito']}); "
            f"conf={item['confidence']:.3f}, spread={item['spread']:.2f}."
        )
        lines.append(
            {
                "key": item["key"],
                "console_family": item["console_family"],
                "canonical_model": item.get("canonical_model"),
                "fair_value": item["fair_value"],
                "confidence": item["confidence"],
                "explanation": explanation,
            }
        )

    return {
        "total": len(lines),
        "items": lines,
    }


def _ebay_medians_by_key() -> dict[str, float]:
    raw = _collect_ebay()
    out: dict[str, float] = {}
    for key, (_, values) in raw.items():
        med = _median(_trimmed(values))
        if med is not None:
            out[key] = med
    return out


def backtest_fair_values(weights: dict[str, float] | None = None) -> dict:
    predicted = compute_fair_values(weights=weights, include_ebay=False)["values"]
    actual = _ebay_medians_by_key()

    by_key = {item["key"]: item for item in predicted}
    pairs = []
    for key, actual_price in actual.items():
        pred_item = by_key.get(key)
        if not pred_item:
            continue
        fair_value = float(pred_item["fair_value"])
        if fair_value <= 0:
            continue
        ape = abs(actual_price - fair_value) / fair_value * 100
        pairs.append(
            {
                "key": key,
                "console_family": pred_item["console_family"],
                "predicted": round(fair_value, 2),
                "actual_ebay_median": round(actual_price, 2),
                "error_pct": round(ape, 2),
            }
        )

    if not pairs:
        return {
            "count": 0,
            "mape": None,
            "mae": None,
            "worst": [],
        }

    mape = sum(x["error_pct"] for x in pairs) / len(pairs)
    mae = sum(abs(x["actual_ebay_median"] - x["predicted"]) for x in pairs) / len(pairs)
    worst = sorted(pairs, key=lambda x: x["error_pct"], reverse=True)[:10]
    return {
        "count": len(pairs),
        "mape": round(mape, 2),
        "mae": round(mae, 2),
        "worst": worst,
    }


def tune_weights() -> dict:
    # Ricerca su combinazioni CEX/Subito (somma 1.0), senza leakage eBay nel predittore
    best: dict | None = None
    trials: list[dict] = []
    grid = [x / 100 for x in range(20, 91, 5)]  # 0.20..0.90

    for cex_w in grid:
        subito_w = 1.0 - cex_w
        weights = {"cex": cex_w, "ebay": 0.0, "subito": subito_w}
        bt = backtest_fair_values(weights)
        row = {
            "weights": weights,
            "count": bt["count"],
            "mape": bt["mape"],
            "mae": bt["mae"],
        }
        trials.append(row)
        if bt["count"] == 0 or bt["mape"] is None:
            continue
        if best is None or float(bt["mape"]) < float(best["mape"]):
            best = row

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "trials": len(trials),
        "best": best,
        "top5": sorted(
            [t for t in trials if t["mape"] is not None],
            key=lambda x: x["mape"],
        )[:5],
    }

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    (LOGS_DIR / "valuation_tuning_latest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload

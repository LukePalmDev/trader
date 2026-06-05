from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TAXONOMY_PATH = Path(__file__).with_name("taxonomy.json")

with TAXONOMY_PATH.open(encoding="utf-8") as _fh:
    _TAX = json.load(_fh)


def _compile(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.I)


VALID_SEGMENTS = set(_TAX["valid_segments"])
VALID_EDITIONS = set(_TAX["valid_editions"])

_BIBLE_ENTRIES: tuple[dict[str, Any], ...] = tuple(_TAX["bible_entries"])
_BIBLE_BY_ID = {str(row["id"]): row for row in _BIBLE_ENTRIES}
_CANONICAL_STANDARD_LABELS = _TAX["canonical_standard_labels"]
_FAMILY_STANDARD_LABELS = _TAX["family_standard_labels"]

_FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (key, _compile(pat)) for key, pat in _TAX["family_patterns"]
]
_FAMILY_SPECIFICITY = _TAX["family_specificity"]
_LEGACY_FAMILY_ALIASES = {
    key: tuple(val) for key, val in _TAX["legacy_family_aliases"].items()
}

_STORAGE_RE = _compile(_TAX["regexes"]["storage"])
_STORAGE_MB_RE = _compile(_TAX["regexes"]["storage_mb"])
_LIMITED_RE = _compile(_TAX["regexes"]["limited"])
_SPECIAL_RE = _compile(_TAX["regexes"]["special"])
_BUNDLE_RE = _compile(_TAX["regexes"]["bundle"])
_PREMIUM_MODEL_RE = _compile(_TAX["regexes"]["premium_model"])
_DIGITAL_RE = _compile(_TAX["regexes"]["digital"])
_FALLBACK_NOISE_RE = _compile(_TAX["regexes"]["fallback_noise"])
_XBOX_360_E_RE = _compile(_TAX["regexes"]["xbox_360_e"])
_XBOX_360_SLIM_RE = _compile(_TAX["regexes"]["xbox_360_slim"])
_XBOX_360_ELITE_RE = _compile(_TAX["regexes"]["xbox_360_elite"])
_XBOX_360_CORE_RE = _compile(_TAX["regexes"]["xbox_360_core"])
_XBOX_360_PREMIUM_RE = _compile(_TAX["regexes"]["xbox_360_premium"])
_XBOX_360_ARCADE_RE = _compile(_TAX["regexes"]["xbox_360_arcade"])
_KINECT_RE = _compile(_TAX["regexes"]["kinect"])
_NO_KINECT_RE = _compile(_TAX["regexes"]["no_kinect"])

_EDITION_NAME_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (label, _compile(pat)) for label, pat in _TAX["edition_name_patterns"]
]
_COLOR_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (label, _compile(pat)) for label, pat in _TAX["color_patterns"]
]

_SPACES_RE = re.compile(r"\s+")
_SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class ModelClassification:
    console_family: str
    sub_model: str
    model_segment: str
    edition_class: str
    canonical_model: str
    classify_confidence: float
    classify_method: str


@dataclass(frozen=True)
class StandardizedTitle:
    standard_name: str
    standard_key: str


def _canonical_family_key(family: str | None) -> str:
    key = (family or "").strip().lower()
    if not key:
        return "other"
    mapped = _LEGACY_FAMILY_ALIASES.get(key)
    if mapped:
        return str(mapped[0])
    if key in {"original", "360", "one", "series", "other"}:
        return key
    if key in {"original/og", "og"}:
        return "original"
    if key in {"serie", "series"}:
        return "series"
    return key


def _legacy_sub_model_hint(family: str | None) -> str | None:
    mapped = _LEGACY_FAMILY_ALIASES.get((family or "").strip().lower())
    if not mapped:
        return None
    return mapped[1]


def _row_family(row: dict[str, Any]) -> str:
    raw = str(row["famiglia"]).strip().lower()
    if raw == "360":
        return "360"
    if raw == "one":
        return "one"
    if raw == "serie":
        return "series"
    if raw in {"original/og", "original", "og"}:
        return "original"
    return "other"


def _family_label_for_csv(family: str) -> str:
    return {
        "original": "Original/OG",
        "360": "360",
        "one": "One",
        "series": "Serie",
    }.get(family, "")


def _sub_model_for_db(row_model: str) -> str:
    model = row_model.strip().lower()
    if model in {"base", "base/core", "originale/base"}:
        return "Base"
    if model == "slim/s":
        return "S"
    if model in {"s", "x", "e"}:
        return model.upper()
    if model == "elite":
        return "Elite"
    if model == "arcade":
        return "Arcade"
    if model == "premium/pro":
        return "Premium/Pro"
    return row_model or "Unknown"


def _model_for_bible(title: str, family: str, family_hint: str | None = None) -> str:
    hinted = _legacy_sub_model_hint(family_hint)
    family = _canonical_family_key(family)

    if family == "series":
        candidates: list[tuple[int, str]] = []
        for label, pattern in (
            ("X", re.compile(r"\bseries\s*x\b|\bserie\s+x\b", re.I)),
            ("S", re.compile(r"\bseries\s*s\b|\bserie\s+s\b", re.I)),
        ):
            match = pattern.search(title)
            if match:
                candidates.append((match.start(), label))
        if candidates:
            candidates.sort()
            return candidates[0][1]
        return hinted or "S"

    if family == "one":
        candidates = []
        for label, pattern in (
            ("X", re.compile(r"\bone\s*x\b", re.I)),
            ("S", re.compile(r"\bone\s*s\b", re.I)),
        ):
            match = pattern.search(title)
            if match:
                candidates.append((match.start(), label))
        if candidates:
            candidates.sort()
            return candidates[0][1]
        return hinted or "Originale/Base"

    if family == "360":
        if _XBOX_360_E_RE.search(title):
            return "E"
        if _XBOX_360_SLIM_RE.search(title):
            return "Slim/S"
        if _XBOX_360_ELITE_RE.search(title):
            return "Elite"
        if _XBOX_360_ARCADE_RE.search(title):
            return "Arcade"
        if _XBOX_360_PREMIUM_RE.search(title):
            return "Premium/Pro"
        if _XBOX_360_CORE_RE.search(title):
            return "Base/Core"
        return "Base/Core"

    if family == "original":
        return "Base"

    return hinted or "Unknown"


def detect_family(name: str) -> str:
    title = (name or "").strip()
    if not title:
        return "other"

    candidates: list[tuple[int, int, str]] = []
    for key, pattern in _FAMILY_PATTERNS:
        match = pattern.search(title)
        if match:
            candidates.append((match.start(), -_FAMILY_SPECIFICITY.get(key, 0), key))

    if candidates:
        candidates.sort()
        return candidates[0][2]

    if re.search(r"\bxbox\b", title, re.I):
        return "original"
    return "other"


def extract_storage_gb(name: str) -> int | None:
    match = _STORAGE_RE.search(name or "")
    if not match:
        return None

    raw_value = match.group(1).replace(".", "")
    unit = match.group(2).upper()
    try:
        value = int(raw_value)
    except ValueError:
        return None

    if unit in {"TB", "T"}:
        return value * 1024
    return value


def _memory_gb(memory: str) -> int | float | None:
    raw = memory.strip().upper().replace(" ", "")
    if raw.endswith("MB"):
        try:
            return round(int(raw[:-2]) / 1024, 4)
        except ValueError:
            return None
    if raw.endswith("GB"):
        try:
            return int(raw[:-2])
        except ValueError:
            return None
    if raw.endswith("TB"):
        try:
            return int(raw[:-2]) * 1024
        except ValueError:
            return None
    if raw.endswith("T"):
        try:
            return int(raw[:-1]) * 1024
        except ValueError:
            return None
    return None


def _title_memory_value(title: str) -> int | float | None:
    if _STORAGE_MB_RE.search(title):
        return round(256 / 1024, 4)
    return extract_storage_gb(title)


def _edition_class(name: str) -> str:
    if _BUNDLE_RE.search(name):
        return "bundle"
    if _LIMITED_RE.search(name):
        return "limited"
    if _SPECIAL_RE.search(name):
        return "special"
    return "standard"


def _normalized_text(text: str) -> str:
    return _SPACES_RE.sub(" ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def _cuscio_matches_title(cuscio: str, title: str) -> bool:
    c = _normalized_text(cuscio)
    t = _normalized_text(title)
    if not c or not t:
        return False
    if c in t:
        return True
    tokens = [tok for tok in c.split() if len(tok) >= 3]
    if not tokens:
        return False
    return all(tok in t for tok in tokens[:3])


def _row_matches_memory(row: dict[str, Any], title_memory: int | float | None) -> bool:
    if title_memory is None:
        return False
    return _memory_gb(str(row["memoria"])) == title_memory


def _default_row(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in candidates:
        if row["type"] == "Canonico":
            return row
    return candidates[0] if candidates else None


def _best_bible_row(title: str, family: str, model: str) -> dict[str, Any] | None:
    title_memory = _title_memory_value(title)
    candidates = [
        row
        for row in _BIBLE_ENTRIES
        if _row_family(row) == family and str(row["modello"]) == model
    ]
    if not candidates:
        return None

    memory_matches = [row for row in candidates if _row_matches_memory(row, title_memory)]
    pool = memory_matches or candidates

    special_matches = [
        row for row in pool if row["type"] == "Speciale" and _cuscio_matches_title(str(row["cuscio"]), title)
    ]
    if special_matches:
        return special_matches[0]

    if _DIGITAL_RE.search(title):
        digital_matches = [
            row for row in pool if "digital" in _normalized_text(str(row["cuscio"]))
        ]
        if digital_matches:
            return digital_matches[0]

    color_matches = [
        row for row in pool if row["type"] == "Canonico" and _cuscio_matches_title(str(row["cuscio"]), title)
    ]
    if color_matches:
        return color_matches[0]

    canonical_pool = [row for row in pool if row["type"] == "Canonico"]
    if canonical_pool:
        return canonical_pool[0]
    return _default_row(pool)


def _canonical_model(name: str, family: str, sub_model: str) -> str:
    family = _canonical_family_key(family)
    row = _best_bible_row(name, family, sub_model)
    if row:
        return str(row["id"])
    return "unknown"


def classify_title(name: str, family_hint: str | None = None) -> ModelClassification:
    title = (name or "").strip()
    family_hint_normalized = _canonical_family_key(family_hint)
    family = (
        family_hint_normalized
        if family_hint_normalized and family_hint_normalized != "other"
        else detect_family(title)
    )
    bible_model = _model_for_bible(title, family, family_hint=family_hint)
    canonical_model = _canonical_model(title, family, bible_model)
    row = _BIBLE_BY_ID.get(canonical_model)
    sub_model = _sub_model_for_db(str(row["modello"])) if row else _sub_model_for_db(bible_model)

    edition_class = _edition_class(title)
    if row:
        edition_class = "special" if row["type"] == "Speciale" else "standard"

    if family == "other" or canonical_model == "unknown":
        model_segment = "unknown"
        confidence = 0.35
    else:
        model_segment = "premium" if edition_class != "standard" or _PREMIUM_MODEL_RE.search(title) else "base"
        confidence = 0.88 if model_segment == "premium" else 0.9

    return ModelClassification(
        console_family=family,
        sub_model=sub_model,
        model_segment=model_segment,
        edition_class=edition_class,
        canonical_model=canonical_model,
        classify_confidence=round(confidence, 3),
        classify_method="rules:v3:bibbia-2026-06-05",
    )


def canonical_taxonomy_ids(*, include_other: bool = True) -> list[str]:
    ids = [str(row["id"]) for row in _BIBLE_ENTRIES]
    if include_other:
        ids.append("other")
    return ids


def taxonomy_entry(canonical_id: str) -> dict[str, Any] | None:
    return _BIBLE_BY_ID.get(str(canonical_id).strip())


def fields_from_canonical_id(canonical_id: str) -> ModelClassification:
    canonical = (canonical_id or "").strip()
    if canonical == "other":
        return ModelClassification(
            console_family="other",
            sub_model="Unknown",
            model_segment="unknown",
            edition_class="standard",
            canonical_model="other",
            classify_confidence=0.0,
            classify_method="taxonomy-map:v2:bibbia",
        )

    row = _BIBLE_BY_ID.get(canonical)
    if not row:
        return fields_from_canonical_id("other")

    family = _row_family(row)
    edition = "special" if row["type"] == "Speciale" else "standard"
    return ModelClassification(
        console_family=family,
        sub_model=_sub_model_for_db(str(row["modello"])),
        model_segment=("premium" if edition == "special" else "base"),
        edition_class=edition,
        canonical_model=canonical,
        classify_confidence=0.0,
        classify_method="taxonomy-map:v2:bibbia",
    )


def _slugify(text: str) -> str:
    base = _SLUG_RE.sub("-", text.lower()).strip("-")
    return _SPACES_RE.sub("-", base)[:96] or "unknown"


def _fallback_standard_name(title: str, family: str) -> str:
    cleaned = re.sub(r"\[[^\]]*\]", " ", title or "")
    cleaned = _FALLBACK_NOISE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"[(){}]", " ", cleaned)
    cleaned = _SPACES_RE.sub(" ", cleaned).strip(" -_,")
    if cleaned:
        return cleaned.title()
    return _FAMILY_STANDARD_LABELS.get(family, "Xbox")


def _extract_colors(title: str) -> list[str]:
    found: list[tuple[int, str]] = []
    for label, pattern in _COLOR_PATTERNS:
        match = pattern.search(title)
        if match:
            found.append((match.start(), label))
    if not found:
        return []
    found.sort(key=lambda x: x[0])
    result: list[str] = []
    for _, label in found:
        if label not in result:
            result.append(label)
    return result


def _extract_edition_descriptor(title: str) -> str:
    candidates: list[tuple[int, str]] = []
    for label, pattern in _EDITION_NAME_PATTERNS:
        match = pattern.search(title)
        if match:
            candidates.append((match.start(), label))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _compose_base_name(title: str, classification: ModelClassification) -> str:
    canonical = (classification.canonical_model or "").strip()
    base_name = _CANONICAL_STANDARD_LABELS.get(canonical)
    if base_name:
        return base_name
    return _fallback_standard_name(title, classification.console_family)


def _normalized_edition(title: str, classification: ModelClassification) -> str:
    if _LIMITED_RE.search(title):
        return "limited"
    if _SPECIAL_RE.search(title):
        return "special"
    if classification.edition_class == "bundle":
        return "standard"
    if classification.edition_class in {"limited", "special"}:
        return classification.edition_class
    return "standard"


def standardize_title(
    name: str,
    *,
    classification: ModelClassification | None = None,
    family_hint: str | None = None,
) -> StandardizedTitle:
    title = (name or "").strip()
    classified = classification or classify_title(title, family_hint=family_hint)

    canonical = (classified.canonical_model or "").strip()
    is_digital = bool(_DIGITAL_RE.search(title))
    edition = _normalized_edition(title, classified)
    base_name = _compose_base_name(title, classified)

    if is_digital and "digital" not in base_name.lower():
        base_name = f"{base_name} Digital"

    edition_descriptor = _extract_edition_descriptor(title)
    if canonical in _BIBLE_BY_ID:
        standard_name = base_name
    elif edition_descriptor:
        standard_name = f"{base_name} [{edition_descriptor}]"
    elif edition == "limited":
        standard_name = f"{base_name} [LIMITED]"
    elif edition == "special":
        standard_name = f"{base_name} [SPECIAL]"
    else:
        standard_name = base_name

    if canonical not in _BIBLE_BY_ID:
        colors = _extract_colors(title)
        if colors:
            standard_name = f"{standard_name} - {'/'.join(colors)}"

    key_base = canonical if canonical and canonical != "unknown" else _slugify(base_name)
    media_key = "digital" if is_digital else "standard"
    if canonical in _BIBLE_BY_ID:
        edition_key = classified.edition_class
    else:
        edition_key = _slugify(edition_descriptor) if edition_descriptor else edition
    standard_key = f"{key_base}|{media_key}|{edition_key}"

    return StandardizedTitle(
        standard_name=standard_name,
        standard_key=standard_key,
    )


def extract_sub_model(title: str, family: str) -> str:
    model = _model_for_bible(title or "", _canonical_family_key(family), family_hint=family)
    return _sub_model_for_db(model)


def extract_edition_name(title: str) -> str:
    for label, pattern in _EDITION_NAME_PATTERNS:
        if pattern.search(title):
            return label
    return "Standard"


def extract_color_str(title: str) -> str:
    colors = _extract_colors(title)
    return "/".join(colors) if colors else ""


def extract_kinect(title: str) -> int | None:
    if _KINECT_RE.search(title):
        if _NO_KINECT_RE.search(title):
            return 0
        return 1
    return None


def base_family_label(console_family: str) -> str:
    key = _canonical_family_key(console_family)
    if key == "series":
        return "Series"
    if key == "one":
        return "One"
    if key == "360":
        return "360"
    if key == "original":
        return "Original"
    return "other"

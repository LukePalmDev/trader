from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

# Tutta la tassonomia (famiglie, alias, etichette, colori, edizioni, regex) è
# definita in taxonomy.json, modificabile senza toccare il codice.
TAXONOMY_PATH = Path(__file__).with_name("taxonomy.json")

with TAXONOMY_PATH.open(encoding="utf-8") as _fh:
    _TAX = json.load(_fh)


def _compile(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.I)


VALID_SEGMENTS = set(_TAX["valid_segments"])
VALID_EDITIONS = set(_TAX["valid_editions"])

_FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (key, _compile(pat)) for key, pat in _TAX["family_patterns"]
]

_STORAGE_RE = _compile(_TAX["regexes"]["storage"])
_LIMITED_RE = _compile(_TAX["regexes"]["limited"])
_SPECIAL_RE = _compile(_TAX["regexes"]["special"])
_BUNDLE_RE = _compile(_TAX["regexes"]["bundle"])
_PREMIUM_MODEL_RE = _compile(_TAX["regexes"]["premium_model"])
_DIGITAL_RE = _compile(_TAX["regexes"]["digital"])

_FAMILY_SPECIFICITY = _TAX["family_specificity"]

_LEGACY_FAMILY_ALIASES = {
    key: tuple(val) for key, val in _TAX["legacy_family_aliases"].items()
}


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
        return mapped[0]
    if key in {"original", "360", "one", "series", "other"}:
        return key
    return key


def _legacy_sub_model_hint(family: str | None) -> str | None:
    mapped = _LEGACY_FAMILY_ALIASES.get((family or "").strip().lower())
    if not mapped:
        return None
    return mapped[1]


def detect_family(name: str) -> str:
    title = (name or "").strip()
    if not title:
        return "other"

    # Se il titolo contiene più famiglie (es. "Series S ... One controller"),
    # usa la prima occorrenza nel testo e, a parità, il pattern più specifico.
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

    if unit == "TB":
        return value * 1024
    return value


def _edition_class(name: str) -> str:
    if _BUNDLE_RE.search(name):
        return "bundle"
    if _LIMITED_RE.search(name):
        return "limited"
    if _SPECIAL_RE.search(name):
        return "special"
    return "standard"


def _storage_slug(storage_gb: int | None) -> str | None:
    if storage_gb is None:
        return None
    if storage_gb >= 1024 and storage_gb % 1024 == 0:
        return f"{storage_gb // 1024}tb"
    return f"{storage_gb}gb"


def _detect_sub_model(title: str, family: str, family_hint: str | None = None) -> str:
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
        return hinted or "Unknown"

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
        return hinted or "Base"

    if family == "360":
        if _XBOX_360_E_RE.search(title):
            return "E"
        if _XBOX_360_SLIM_RE.search(title):
            return "S"
        if _XBOX_360_ELITE_RE.search(title):
            return "Elite"
        return "Base"

    if family == "original":
        return "Base"

    return hinted or "Unknown"


def _canonical_model(name: str, family: str, sub_model: str) -> str:
    family = _canonical_family_key(family)
    storage_gb = extract_storage_gb(name)
    storage_slug = _storage_slug(storage_gb)
    has_digital = bool(_DIGITAL_RE.search(name))

    if family == "series" and sub_model == "X":
        if has_digital:
            return "series-x-digital-1tb"
        if storage_gb and storage_gb >= 1900:
            return "series-x-2tb"
        return "series-x-1tb"

    if family == "series" and sub_model == "S":
        if storage_gb and storage_gb >= 900:
            return "series-s-1tb"
        return "series-s-512gb"

    if family == "series":
        return "series-unknown"

    if family == "one" and sub_model == "X":
        return "one-x-1tb"

    if family == "one" and sub_model == "S":
        if has_digital:
            return "one-s-digital-1tb"
        if storage_gb and storage_gb >= 1900:
            return "one-s-2tb"
        if storage_gb and storage_gb >= 900:
            return "one-s-1tb"
        return "one-s-500gb"

    if family == "one":
        if storage_gb and storage_gb >= 900:
            return "one-base-1tb"
        return "one-base-500gb"

    if family == "360":
        prefix = {
            "E": "360-e",
            "S": "360-s",
            "Elite": "360-elite",
            "Base": "360-base",
        }.get(sub_model, "360-base")
        return f"{prefix}-{storage_slug}" if storage_slug else prefix

    if family == "original":
        return "original-base-8gb"

    return "unknown"


def classify_title(name: str, family_hint: str | None = None) -> ModelClassification:
    title = (name or "").strip()
    family_hint_normalized = _canonical_family_key(family_hint)
    family = family_hint_normalized if family_hint_normalized and family_hint_normalized != "other" else detect_family(title)
    sub_model = _detect_sub_model(title, family, family_hint=family_hint)

    edition_class = _edition_class(title)
    canonical_model = _canonical_model(title, family, sub_model)

    if family == "other":
        model_segment = "unknown"
        confidence = 0.35
    else:
        model_segment = "base"
        confidence = 0.9

        if edition_class != "standard" or _PREMIUM_MODEL_RE.search(title):
            model_segment = "premium"
            confidence = 0.88

    return ModelClassification(
        console_family=family,
        sub_model=sub_model,
        model_segment=model_segment,
        edition_class=edition_class,
        canonical_model=canonical_model,
        classify_confidence=round(confidence, 3),
        classify_method="rules:v2:2026-06-03",
    )


_CANONICAL_STANDARD_LABELS = _TAX["canonical_standard_labels"]

_FAMILY_STANDARD_LABELS = _TAX["family_standard_labels"]


def canonical_taxonomy_ids(*, include_other: bool = True) -> list[str]:
    """Return the stable canonical IDs accepted by AI classifiers."""
    ids = sorted(_CANONICAL_STANDARD_LABELS)
    if include_other:
        ids.append("other")
    return ids


def fields_from_canonical_id(canonical_id: str) -> ModelClassification:
    """Map a canonical taxonomy ID back to the DB classification fields."""
    canonical = (canonical_id or "").strip()
    if canonical == "other":
        return ModelClassification(
            console_family="other",
            sub_model="Unknown",
            model_segment="unknown",
            edition_class="standard",
            canonical_model="other",
            classify_confidence=0.0,
            classify_method="taxonomy-map:v1",
        )

    if canonical.startswith("series-x"):
        family, sub_model = "series", "X"
    elif canonical.startswith("series-s"):
        family, sub_model = "series", "S"
    elif canonical.startswith("one-x"):
        family, sub_model = "one", "X"
    elif canonical.startswith("one-s"):
        family, sub_model = "one", "S"
    elif canonical.startswith("one-base") or canonical.startswith("one-"):
        family, sub_model = "one", "Base"
    elif canonical.startswith("360-e"):
        family, sub_model = "360", "E"
    elif canonical.startswith("360-s"):
        family, sub_model = "360", "S"
    elif canonical.startswith("360-elite"):
        family, sub_model = "360", "Elite"
    elif canonical.startswith("360"):
        family, sub_model = "360", "Base"
    elif canonical.startswith("original"):
        family, sub_model = "original", "Base"
    else:
        family, sub_model, canonical = "other", "Unknown", "other"

    return ModelClassification(
        console_family=family,
        sub_model=sub_model,
        model_segment=("unknown" if family == "other" else "base"),
        edition_class="standard",
        canonical_model=canonical,
        classify_confidence=0.0,
        classify_method="taxonomy-map:v1",
    )

_FALLBACK_NOISE_RE = _compile(_TAX["regexes"]["fallback_noise"])
_SPACES_RE = re.compile(r"\s+")
_SLUG_RE = re.compile(r"[^a-z0-9]+")

_XBOX_360_E_RE = _compile(_TAX["regexes"]["xbox_360_e"])
_XBOX_360_SLIM_RE = _compile(_TAX["regexes"]["xbox_360_slim"])
# "Elite" è un sotto-modello del 360 fat (2007, nero/120GB); usato solo nel contesto family=="360"
_XBOX_360_ELITE_RE = _compile(_TAX["regexes"]["xbox_360_elite"])

_EDITION_NAME_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (label, _compile(pat)) for label, pat in _TAX["edition_name_patterns"]
]

_COLOR_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (label, _compile(pat)) for label, pat in _TAX["color_patterns"]
]


def _slugify(text: str) -> str:
    base = _SLUG_RE.sub("-", text.lower()).strip("-")
    return _SPACES_RE.sub("-", base)[:96] or "unknown"


def _format_storage(storage_gb: int | None) -> str:
    if storage_gb is None:
        return ""
    if storage_gb >= 1024 and storage_gb % 1024 == 0:
        return f"{storage_gb // 1024} TB"
    return f"{storage_gb} GB"


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


def _compose_base_name(
    title: str,
    classification: ModelClassification,
) -> str:
    family = classification.console_family
    canonical = (classification.canonical_model or "").strip()
    storage_gb = extract_storage_gb(title)

    if family == "360":
        if classification.sub_model == "E":
            prefix = "Xbox 360 E"
        elif classification.sub_model == "S":
            prefix = "Xbox 360 S"
        elif classification.sub_model == "Elite":
            prefix = "Xbox 360 Elite"
        else:
            prefix = "Xbox 360"

        storage = _format_storage(storage_gb)
        return f"{prefix} {storage}".strip()

    base_name = _CANONICAL_STANDARD_LABELS.get(canonical)
    if not base_name:
        base_name = _fallback_standard_name(title, family)
    return base_name


def _normalized_edition(title: str, classification: ModelClassification) -> str:
    if _LIMITED_RE.search(title):
        return "limited"
    if _SPECIAL_RE.search(title):
        return "special"
    # Tratta i "bundle" generici (es. controller incluso) come standard:
    # sono varianti commerciali dello stesso hardware.
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
    family = classified.console_family
    is_digital = bool(_DIGITAL_RE.search(title)) or canonical in {"series-x-digital", "series-x-digital-1tb", "one-s-digital-1tb"}
    edition = _normalized_edition(title, classified)

    base_name = _compose_base_name(title, classified)

    if is_digital and "digital" not in base_name.lower():
        base_name = f"{base_name} Digital"

    edition_descriptor = _extract_edition_descriptor(title)
    if edition_descriptor:
        standard_name = f"{base_name} [{edition_descriptor}]"
    elif edition == "limited":
        standard_name = f"{base_name} [LIMITED]"
    elif edition == "special":
        standard_name = f"{base_name} [SPECIAL]"
    else:
        standard_name = base_name

    colors = _extract_colors(title)
    if colors:
        standard_name = f"{standard_name} - {'/'.join(colors)}"

    # Per Xbox 360 la variante (base/E/Slim) fa parte dell'identità del modello.
    # Usiamo il nome base normalizzato per evitare accorpamenti non voluti.
    if family == "360":
        key_base = _slugify(base_name)
    else:
        key_base = canonical if canonical and canonical != "unknown" else _slugify(base_name)
    media_key = "digital" if is_digital else "standard"
    edition_key = _slugify(edition_descriptor) if edition_descriptor else edition
    color_key = _slugify("-".join(colors)) if colors else "nocolor"
    standard_key = f"{key_base}|{media_key}|{edition_key}|{color_key}"

    return StandardizedTitle(
        standard_name=standard_name,
        standard_key=standard_key,
    )


# ---------------------------------------------------------------------------
# Funzioni di estrazione attributi strutturati (per la tab Ricerca)
# ---------------------------------------------------------------------------

_KINECT_RE = _compile(_TAX["regexes"]["kinect"])
_NO_KINECT_RE = _compile(_TAX["regexes"]["no_kinect"])


def extract_sub_model(title: str, family: str) -> str:
    """Restituisce il sotto-modello: Base / E / S / Elite / X."""
    return _detect_sub_model(title or "", _canonical_family_key(family), family_hint=family)


def extract_edition_name(title: str) -> str:
    """Restituisce il nome specifico dell'edizione (es. 'Halo', 'Forza'), o 'Standard'."""
    for label, pattern in _EDITION_NAME_PATTERNS:
        if pattern.search(title):
            return label
    return "Standard"


def extract_color_str(title: str) -> str:
    """Restituisce il/i colore/i come stringa (es. 'Nero', 'Bianco/Rosso'), o '' se assente."""
    colors = _extract_colors(title)
    return "/".join(colors) if colors else ""


def extract_kinect(title: str) -> int | None:
    """Rileva presenza Kinect: 1 = sì, 0 = no, None = non specificato."""
    if _KINECT_RE.search(title):
        if _NO_KINECT_RE.search(title):
            return 0
        return 1
    return None


def base_family_label(console_family: str) -> str:
    """Mappa console_family alla famiglia base per la UI: Original/360/One/Series."""
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

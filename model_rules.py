from __future__ import annotations

import re
from dataclasses import dataclass

VALID_SEGMENTS = {"base", "premium", "unknown"}
VALID_EDITIONS = {"standard", "limited", "special", "bundle"}

_FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("series-x", re.compile(r"series\s*x", re.I)),
    ("series-s", re.compile(r"series\s*s", re.I)),
    ("one-x", re.compile(r"one\s*x", re.I)),
    ("one-s", re.compile(r"one\s*s", re.I)),
    ("one", re.compile(r"\bone\b", re.I)),
    ("360", re.compile(r"\b360[Ss]?\b|\bxbox\s*360", re.I)),
    ("original", re.compile(r"\boriginal\b|\bxbox\s+classic\b", re.I)),
]

_STORAGE_RE = re.compile(r"(\d[\d.]*)\s*(GB|TB)", re.I)

_LIMITED_RE = re.compile(
    r"\b(limited|Ltd|collector(?:'s)?|launch|day\s*one|project\s+scorpio|anniversary|"
    r"halo\w*|forza\w*|gears\w*|starfield|cyberpunk|spider\-?man)\b",
    re.I,
)
_SPECIAL_RE = re.compile(
    r"\b(special|editione\s+speciale|anniversary|commemorative|commemorativa|"
    r"galaxy\s+black|20th\s+anniversary|elite\s+series)\b",
    re.I,
)
_BUNDLE_RE = re.compile(
    r"\b(bundle|pack|con\s+gioco|with\s+game|inclus[oa]|include|bundle\s+edition)\b",
    re.I,
)
_PREMIUM_MODEL_RE = re.compile(
    r"\b(elite|project\s+scorpio|2\s*tb|2tb|galaxy\s+black)\b",
    re.I,
)
_DIGITAL_RE = re.compile(r"\b(digital|all\s*\-?\s*digital|senza\s+lettore|no\s+disc|edizione\s+digitale)\b", re.I)

_FAMILY_SPECIFICITY = {
    "series-x": 4,
    "series-s": 4,
    "one-x": 3,
    "one-s": 3,
    "one": 2,
    "360": 2,
    "original": 1,
}


@dataclass(frozen=True)
class ModelClassification:
    console_family: str
    model_segment: str
    edition_class: str
    canonical_model: str
    classify_confidence: float
    classify_method: str


@dataclass(frozen=True)
class StandardizedTitle:
    standard_name: str
    standard_key: str


def detect_family(name: str) -> str:
    title = (name or "").strip()
    if not title:
        return "other"

    # Se il titolo contiene più famiglie (es. "Series S ... Series X controller"),
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


def _canonical_model(name: str, family: str) -> str:
    storage_gb = extract_storage_gb(name)
    has_digital = bool(_DIGITAL_RE.search(name))

    if family == "series-x":
        if has_digital:
            return "series-x-digital"
        if storage_gb and storage_gb >= 1900:
            return "series-x-2tb"
        return "series-x-1tb"

    if family == "series-s":
        if storage_gb and storage_gb >= 900:
            return "series-s-1tb"
        return "series-s-512gb"

    if family == "one-x":
        return "one-x-1tb"

    if family == "one-s":
        if storage_gb and storage_gb >= 1900:
            return "one-s-2tb"
        if storage_gb and storage_gb >= 900:
            return "one-s-1tb"
        return "one-s-500gb"

    if family == "one":
        if storage_gb and storage_gb >= 900:
            return "one-1tb"
        return "one-500gb"

    if family == "360":
        if storage_gb and storage_gb >= 400:
            return "360-500gb"
        if storage_gb and storage_gb >= 200:
            return "360-250gb"
        if storage_gb and storage_gb >= 100:
            return "360-120gb"
        return "360"

    if family == "original":
        return "original"

    return "unknown"


def classify_title(name: str, family_hint: str | None = None) -> ModelClassification:
    title = (name or "").strip()
    family = family_hint if family_hint and family_hint != "other" else detect_family(title)

    edition_class = _edition_class(title)
    canonical_model = _canonical_model(title, family)

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
        model_segment=model_segment,
        edition_class=edition_class,
        canonical_model=canonical_model,
        classify_confidence=round(confidence, 3),
        classify_method="rules:v1",
    )


_CANONICAL_STANDARD_LABELS = {
    "series-x-1tb": "Xbox Series X 1 TB",
    "series-x-2tb": "Xbox Series X 2 TB",
    "series-x-digital": "Xbox Series X Digital 1 TB",
    "series-s-512gb": "Xbox Series S 512 GB",
    "series-s-1tb": "Xbox Series S 1 TB",
    "one-x-1tb": "Xbox One X 1 TB",
    "one-s-500gb": "Xbox One S 500 GB",
    "one-s-1tb": "Xbox One S 1 TB",
    "one-s-2tb": "Xbox One S 2 TB",
    "one-500gb": "Xbox One 500 GB",
    "one-1tb": "Xbox One 1 TB",
    "360-120gb": "Xbox 360 120 GB",
    "360-250gb": "Xbox 360 250 GB",
    "360-500gb": "Xbox 360 500 GB",
    "360": "Xbox 360",
    "original": "Xbox Original",
}

_FAMILY_STANDARD_LABELS = {
    "series-x": "Xbox Series X",
    "series-s": "Xbox Series S",
    "one-x": "Xbox One X",
    "one-s": "Xbox One S",
    "one": "Xbox One",
    "360": "Xbox 360",
    "original": "Xbox Original",
}

_FALLBACK_NOISE_RE = re.compile(
    r"\b(microsoft|console|wireless|controller|controllere?|inkl?|incl\.?|con|senza|colore|nero|bianco|rosso|blu|argento|gold|silver)\b",
    re.I,
)
_SPACES_RE = re.compile(r"\s+")
_SLUG_RE = re.compile(r"[^a-z0-9]+")

_XBOX_360_E_RE = re.compile(r"\b(?:xbox\s*)?360\s*\"?e\"?\b", re.I)
_XBOX_360_SLIM_RE = re.compile(r"\b360\s*slim\b|\bslim\b|\b360[Ss]\b", re.I)
# "Elite" è un sotto-modello del 360 fat (2007, nero/120GB); usato solo nel contesto family=="360"
_XBOX_360_ELITE_RE = re.compile(r"\belite\b", re.I)

_EDITION_NAME_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("Call Of Duty", re.compile(r"\bcall\s+of\s+duty\b", re.I)),
    ("Minecraft", re.compile(r"\bminecraft\b", re.I)),
    ("Halo", re.compile(r"\bhalo\w*\b", re.I)),
    ("Forza", re.compile(r"\bforza\w*\b", re.I)),
    ("Gears", re.compile(r"\bgears\w*\b", re.I)),
    ("Cyberpunk 2077", re.compile(r"\bcyberpunk\b", re.I)),
    ("Battlefield", re.compile(r"\bbattlefield\b", re.I)),
    ("Fortnite", re.compile(r"\bfortnite\b", re.I)),
    ("Sunset Overdrive", re.compile(r"\bsunset\s+overdrive\b", re.I)),
    ("Project Scorpio", re.compile(r"\bproject\s+scorpio\b", re.I)),
    ("Starfield", re.compile(r"\bstarfield\b", re.I)),
]

_COLOR_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("Nero", re.compile(r"\bner[oa]\b|\bblack\b", re.I)),
    ("Bianco", re.compile(r"\bbianc[oa]\b|\bwhite\b", re.I)),
    ("Rosso", re.compile(r"\bross[oa]\b|\bred\b", re.I)),
    ("Blu", re.compile(r"\bblu\b|\bblue\b", re.I)),
    ("Verde", re.compile(r"\bverd[ei]\b|\bgreen\b", re.I)),
    ("Argento", re.compile(r"\bargent[oa]\b|\bsilver\b", re.I)),
    ("Oro", re.compile(r"\boro\b|\bgold\b", re.I)),
    ("Grigio", re.compile(r"\bgrigi[oa]\b|\bgray\b|\bgrey\b", re.I)),
    ("Viola", re.compile(r"\bviola\b|\bpurple\b|\blilla\b", re.I)),
    ("Giallo", re.compile(r"\bgiall[oa]\b|\byellow\b", re.I)),
    ("Marrone", re.compile(r"\bmarrone\b|\bbrown\b", re.I)),
    ("Arancione", re.compile(r"\barancione\b|\borange\b", re.I)),
    ("Rosa", re.compile(r"\brosa\b|\bpink\b", re.I)),
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
        if _XBOX_360_E_RE.search(title):
            prefix = "Xbox 360 E"
        elif _XBOX_360_SLIM_RE.search(title):
            prefix = "Xbox 360 Slim"
        elif _XBOX_360_ELITE_RE.search(title):
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
    is_digital = bool(_DIGITAL_RE.search(title)) or canonical == "series-x-digital"
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

_KINECT_RE = re.compile(r"\bkinect\b", re.I)
_NO_KINECT_RE = re.compile(r"\bno\s+kinect\b|\bsenza\s+kinect\b|\(no\s+kinect\)", re.I)


def extract_sub_model(title: str, family: str) -> str:
    """Restituisce il sotto-modello: Base / E / Slim / Elite / X / S."""
    if family in ("series-x", "one-x"):
        return "X"
    if family in ("series-s", "one-s"):
        return "S"
    if family in ("one", "original", "other"):
        return "Base"
    if family == "360":
        if _XBOX_360_E_RE.search(title):
            return "E"
        if _XBOX_360_SLIM_RE.search(title):
            return "Slim"
        if _XBOX_360_ELITE_RE.search(title):
            return "Elite"
        return "Base"
    return "Base"


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
    if console_family in ("series-x", "series-s"):
        return "Series"
    if console_family in ("one", "one-s", "one-x"):
        return "One"
    if console_family == "360":
        return "360"
    if console_family == "original":
        return "Original"
    return "other"

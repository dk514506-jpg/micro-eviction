"""mev.topic_identity — W5 (A-F1): two-stage topic identity.

Stage 1 (ADVISORY): embedding-similarity proposes candidate clusters. Never
promotes, never gates. Embedding model id + version logged as proposal metadata.
Stage 2 (MECHANICAL, the only gate): a cluster promotes to rule only if its
members share a normalized logical core (lowercased, modifier-stripped, numbers
canonicalized) with at least one substantive phrase matching verbatim after
normalization. Calcified content is consensus-derived with per-member quoting
(record_id-tagged), never one member's text alone.
"""
from __future__ import annotations
import re

# Frozen modifier/stopword set: strength, mode, and function words that do not
# change the propositional content of a statement.
MODIFIER_WORDS: frozenset[str] = frozenset("""
never always must should shall strictly explicitly verbatim frozen
merely just only completely entirely totally absolutely certainly definitely
clearly obviously essentially basically actually really very quite rather
somewhat slightly also too even still yet already
do does did is are was were be been being have has had
the a an this that these those it its
of in on at to for with by from as into than then so if when while during
before after once until because since although though however
between among through across behind beyond within without against
toward towards upon along beside besides except unless whether
either neither both each every any all some no none not
and or but nor
pre-registered pre registered
""".split())

# Substantive-phrase floor: a shared phrase must be at least this many content
# words to count as a verbatim propositional match (not a keyword echo).
SUBSTANTIVE_MIN_WORDS = 3

_NUM_RE = re.compile(r"\d[\d,]*\.?\d*")

def _canonicalize_number(m: re.Match) -> str:
    s = m.group(0).replace(",", "")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

def normalize(text: str) -> str:
    """Lowercase, strip modifiers, canonicalize numbers."""
    s = text.lower()
    s = _NUM_RE.sub(_canonicalize_number, s)
    s = re.sub(r"[^\w\s-]", " ", s)  # strip punctuation except intra-word hyphens
    words = [w for w in s.split() if w not in MODIFIER_WORDS]
    return " ".join(words)

def logical_core(text: str) -> str:
    """Normalized logical core = modifier-stripped, number-canonicalized content."""
    return normalize(text)

def _content_words(core: str) -> set[str]:
    return set(core.split())

def share_logical_core(cores: list[str],
                       min_shared: int = SUBSTANTIVE_MIN_WORDS) -> bool:
    """All members share a logical core iff the intersection of their content-
    word sets has at least min_shared words (order-insensitive, modifier-
    stripped). Rejects keyword-only grouping (single shared token) while
    accepting paraphrase clusters with common propositional vocabulary."""
    if len(cores) < 2:
        return False
    sets = [_content_words(c) for c in cores]
    inter = sets[0]
    for s in sets[1:]:
        inter &= s
    return len(inter) >= min_shared

def _substantive_phrase_match(cores: list[str],
                              min_words: int = SUBSTANTIVE_MIN_WORDS) -> bool:
    """At least one contiguous run of >= min_words content words appears verbatim
    (after normalization) in every member's core."""
    if len(cores) < 2:
        return False
    word_lists = [c.split() for c in cores]
    # find longest common contiguous word-run across all members
    first = word_lists[0]
    for length in range(len(first), min_words - 1, -1):
        for start in range(len(first) - length + 1):
            phrase = first[start:start + length]
            if all(_contains_run(wl, phrase) for wl in word_lists[1:]):
                return True
    return False

def _contains_run(words: list[str], phrase: list[str]) -> bool:
    n = len(phrase)
    for i in range(len(words) - n + 1):
        if words[i:i + n] == phrase:
            return True
    return False

def mechanical_promotion_gate(members: list[dict]) -> bool:
    """Stage 2 mechanical gate: a cluster promotes to rule only if its members
    share a normalized logical core AND at least one substantive phrase matches
    verbatim after normalization. This is the ONLY gate — the advisory layer
    never promotes."""
    if len(members) < 2:
        return False
    cores = [logical_core(m["content"]) for m in members]
    return share_logical_core(cores) and _substantive_phrase_match(cores)

def consensus_content(members: list[dict]) -> dict:
    """Consensus-derived calcified content: shared core + per-member quoting
    (record_id-tagged). Never one member's text alone."""
    cores = [logical_core(m["content"]) for m in members]
    core_words = _content_words(cores[0]) if cores else set()
    for c in cores[1:]:
        core_words &= _content_words(c)
    consensus_core = " ".join(sorted(core_words))
    quotes = [{"record_id": m["record_id"], "quote": m["content"]} for m in members]
    rule_content = (
        f"core: {consensus_core}\n"
        + "\n".join(f"[{q['record_id']}] {q['quote']}" for q in quotes)
    )
    return {"consensus_core": consensus_core, "member_quotes": quotes,
            "rule_content": rule_content}

# ---------------------------------------------------------------------------
# Stage 1 — advisory embedding-similarity proposals (never gates)
# ---------------------------------------------------------------------------
def _embed(text: str) -> dict[str, float]:
    """Lightweight local embedding: character 3-gram counts (normalized).
    Deterministic, zero-dependency. Swap for a real model at deployment."""
    s = normalize(text)
    grams: dict[str, float] = {}
    for i in range(len(s) - 2):
        g = s[i:i + 3]
        grams[g] = grams.get(g, 0.0) + 1.0
    norm = sum(v * v for v in grams.values()) ** 0.5 or 1.0
    return {k: v / norm for k, v in grams.items()}

def _cosine(a: dict[str, float], b: dict[str, float]) -> float:
    return sum(a[k] * b.get(k, 0.0) for k in a)

def advisory_cluster_proposals(records: list[dict],
                               model_id: str = "builtin-bovw-v1",
                               model_version: str = "1.0",
                               threshold: float = 0.3) -> dict:
    """Stage 1 ADVISORY: propose candidate clusters by embedding similarity.
    NEVER promotes, NEVER gates. Returns proposal metadata for the run log."""
    embeds = [_embed(r["content"]) for r in records]
    n = len(records)
    parent = list(range(n))
    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for i in range(n):
        for j in range(i + 1, n):
            if _cosine(embeds[i], embeds[j]) >= threshold:
                parent[find(i)] = find(j)
    clusters: dict[int, list[int]] = {}
    for i in range(n):
        clusters.setdefault(find(i), []).append(i)
    candidates = [
        {"members": [records[i]["record_id"] for i in idxs],
         "size": len(idxs)}
        for idxs in clusters.values() if len(idxs) >= 2
    ]
    return {"model_id": model_id, "model_version": model_version,
            "threshold": threshold, "candidate_clusters": candidates,
            "note": "ADVISORY ONLY — never promotes, never gates"}

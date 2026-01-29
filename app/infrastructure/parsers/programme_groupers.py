#!/usr/bin/env python3
"""
programme_groupers.py

Programme-specific grouping strategies for Search API call records *and*
for fetched topic metadata records (your pillar metadata files).

Key fix:
- Your pillar metadata items look like:
    { identifier, summary, url, language, raw: { metadata: {...} } }
  NOT like Search API results with a top-level "metadata".
- Therefore we must read metadata from either:
    meta_wrapper["metadata"]  OR  meta_wrapper["raw"]["metadata"]

Additionally:
- HORIZON grouping for pillar metadata must be by destination:
    metadata.destinationDescription (when present)
  but CL3 and MISS frequently lack destinationDescription, so we must inject
  destination maps (call_id -> destination_title) for those families.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------
# Generic helpers
# ----------------------------

def safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(x)


def first(x: Any, default: Any = "") -> Any:
    if isinstance(x, list):
        return x[0] if x else default
    return x if x is not None else default


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def parse_date(s: str) -> Optional[str]:
    s = safe_str(s).strip()
    if not s:
        return None
    try:
        s2 = s.replace("Z", "+0000")
        s2 = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", s2)
        dt = datetime.fromisoformat(s2.replace("+0000", "+00:00"))
        return dt.isoformat()
    except Exception:
        return s


def extract_budget(meta: Dict[str, Any]) -> Tuple[float, float, float, Any]:
    """
    Budget fields differ across programmes.
    For HORIZON topics you usually have budgetOverview JSON string, but
    the fetcher may also yield minContribution/maxContribution/budget.
    Keep this tolerant and non-failing.
    """
    def _f(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    min_c = _f(first(meta.get("minContribution"), 0.0))
    max_c = _f(first(meta.get("maxContribution"), 0.0))
    b = first(meta.get("budget"), None)
    budget = _f(b) if b is not None else max_c
    n_projects = first(meta.get("expectedGrants"), None)
    return min_c, max_c, budget, n_projects


# ----------------------------
# Singleton-merge configuration
# ----------------------------

@dataclass(frozen=True)
class SingletonMergeConfig:
    enabled: bool = False
    target_bucket: str = "_other"
    sim_threshold: float = 0.28
    stopwords: Tuple[str, ...] = (
        "call", "calls", "topic", "topics", "programme", "program", "action", "actions", "open", "forthcoming"
    )


# ----------------------------
# Base strategy
# ----------------------------

class BaseProgrammeGrouper:
    """
    Strategy interface.

    meta_wrapper may be either:
    A) Search API result object: { ..., "metadata": {...} }
    B) Fetched topic metadata record: { identifier, url, ..., "raw": { "metadata": {...} } }
    """

    programme_code: str = "DEFAULT"

    def merge_config(self) -> SingletonMergeConfig:
        return SingletonMergeConfig(enabled=False)

    # ---- Metadata access (CRITICAL FIX) ----

    def get_metadata(self, meta_wrapper: Dict[str, Any]) -> Dict[str, Any]:
        """
        Return the metadata dict regardless of wrapper shape.
        """
        meta = meta_wrapper.get("metadata")
        if isinstance(meta, dict) and meta:
            return meta
        raw = meta_wrapper.get("raw") or {}
        meta2 = raw.get("metadata")
        if isinstance(meta2, dict) and meta2:
            return meta2
        return {}

    def get_language(self, meta_wrapper: Dict[str, Any]) -> str:
        meta = self.get_metadata(meta_wrapper)
        return safe_str(first(meta.get("language"), meta_wrapper.get("language") or "")).strip()

    # ---- Identity / dedupe ----

    def unique_record_key(self, meta_wrapper: Dict[str, Any]) -> str:
        """
        Default identity: (type, identifier, callccm2Id, language).

        Works for both wrapper shapes.
        """
        meta = self.get_metadata(meta_wrapper)

        typ = safe_str(meta_wrapper.get("type") or meta_wrapper.get("contentType") or "").strip()
        ident = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
        callccm2 = safe_str(first(meta.get("callccm2Id"), "")).strip()
        lang = self.get_language(meta_wrapper) or "und"

        if ident and callccm2:
            return f"{typ}|{ident}|{callccm2}|{lang}"
        if ident:
            url = safe_str(first(meta.get("url"), meta_wrapper.get("url") or "")).strip()
            return f"{typ}|{ident}|{url}|{lang}" if url else f"{typ}|{ident}|{lang}"
        # last resort
        return json.dumps(meta_wrapper, sort_keys=True)

    # ---- Grouping ----

    def group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        raise NotImplementedError

    def fallback_group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        return ""

    # ---- Normalization ----

    def normalize_one(self, meta_wrapper: Dict[str, Any]) -> Dict[str, Any]:
        meta = self.get_metadata(meta_wrapper)

        identifier = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
        url = safe_str(first(meta.get("url"), meta_wrapper.get("url") or "")).strip()
        topic_title = safe_str(first(meta.get("title"), meta_wrapper.get("title") or "")).strip()
        call_title = safe_str(first(meta.get("callTitle"), "")).strip()
        call_identifier = safe_str(first(meta.get("callIdentifier"), "")).strip()
        type_of_action = safe_str(first(meta.get("typesOfAction"), "")).strip()
        status = ""
        # "actions" is often a JSON string list in metadata; keep raw in output, and let DB layer parse if needed.
        # But many of your normalized pipelines just keep a human label if present elsewhere.
        # We'll preserve meta_wrapper["raw"] anyway.

        min_c, max_c, budget, n_projects = extract_budget(meta)

        # destinationDescription can be a list of HTML strings; store a normalized text form too.
        dest_desc = first(meta.get("destinationDescription"), "")
        dest_desc = safe_str(dest_desc).strip()
        dest_desc_norm = normalize_space(re.sub(r"<[^>]+>", " ", dest_desc)) if dest_desc else ""

        out: Dict[str, Any] = {
            "identifier": identifier,
            "url": url,
            "topic_title": topic_title or safe_str(meta_wrapper.get("summary") or ""),
            "call_title": call_title,
            "call_identifier": call_identifier,
            "type_of_action": type_of_action,

            "min_contribution": min_c,
            "max_contribution": max_c,
            "indicative_budget": budget,
            "indicative_number_of_projects": n_projects,

            # for destination-based grouping
            "destinationDescription": dest_desc,
            "destinationDescription_text": dest_desc_norm,

            "opening_date": parse_date(safe_str(first(meta.get("plannedOpeningDate"), ""))) or "",
            "deadline": parse_date(safe_str(first(meta.get("deadlineDate"), ""))) or "",
            "status": status,
            "deadline_model": safe_str(first(meta.get("submissionProcedure"), "")),

            "tags": meta.get("tags") or [],
            "keywords": meta.get("keywords") or [],

            # Always keep original wrapper for downstream parsing
            "raw": meta_wrapper.get("raw") or meta_wrapper,
        }
        return out

    # ---- Similarity helpers (used by singleton merge) ----

    def tokenize_for_similarity(self, s: str, stopwords: Tuple[str, ...]) -> set[str]:
        toks = set(re.findall(r"[a-z0-9]+", (s or "").lower()))
        return {t for t in toks if t and t not in set(stopwords)}

    def similarity(self, a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        union = len(a | b)
        return float(inter) / float(union) if union else 0.0


# ----------------------------
# Programme: HORIZON
# ----------------------------

def _invert_destination_map(dest_map: Dict[str, Any]) -> Dict[str, str]:
    """
    Input can be:
      { "Destination title": [ {"call_id": "...", "title": "..."}, ... ], ... }

    Output:
      { "HORIZON-CL3-...": "Destination title", ... }
    """
    call_to_dest: Dict[str, str] = {}
    if not isinstance(dest_map, dict):
        return call_to_dest
    for dest_title, arr in dest_map.items():
        if not isinstance(arr, list):
            continue
        for row in arr:
            if not isinstance(row, dict):
                continue
            cid = safe_str(row.get("call_id") or "").strip()
            if cid:
                call_to_dest[cid] = safe_str(dest_title).strip()
    return call_to_dest


class HorizonProgrammeGrouper(BaseProgrammeGrouper):
    programme_code = "HORIZON"

    def __init__(self, *, cl3_map_path: str = "", missions_map_path: str = "", cl1_map_path: str = ""):
        # NOTE: historically we injected destination maps for CL3 and missions
        # because many topic records were missing destinationDescription.
        #
        # We no longer group by destination name at all; for Pillar 2 we group by
        # the *group code embedded in the identifier* (e.g. DEMOCRACY, TRANSFO,
        # DIGITAL-EMERGING, D3, ...). For Pillars 1 and 3 we group by the pillar
        # programme family (ERC/INFRA/MSCA/EIC/EIE).
        #
        # Keep constructor signature for backwards compatibility.
        self._unused = (cl3_map_path, missions_map_path, cl1_map_path)

    def group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        """Return the grouping bucket for a Horizon topic/call.

        Rules:
        - Pillar 2 topics (CLx and MISS) are grouped by the *identifier-embedded*
          group code (e.g. HORIZON-CL2-2026-01-DEMOCRACY-01 -> DEMOCRACY).
        - Pillar 1 topics are grouped by: ERC / MSCA / INFRA.
        - Pillar 3 topics are grouped by: EIC / EIE.

        We do **not** group by destination name (destinationDescription), because
        it is missing for most topics.
        """
        meta = self.get_metadata(meta_wrapper)
        ident = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
        if not ident:
            return ""

        # --- Pillar 1 families (sometimes no "HORIZON-" prefix) ---
        if ident.startswith("ERC-") or ident.startswith("HORIZON-ERC-"):
            return "ERC"

        if ident.startswith("MSCA-") or ident.startswith("HORIZON-MSCA-"):
            return "MSCA"

        if ident.startswith("INFRA-") or ident.startswith("HORIZON-INFRA-"):
            return "INFRA"

        # --- Pillar 3 families ---
        if ident.startswith("HORIZON-EIC-"):
            return "EIC"

        if ident.startswith("HORIZON-EIE-"):
            return "EIE"

        # --- Pillar 2 (Clusters + Missions) ---
        if ident.startswith("HORIZON-CL") or ident.startswith("HORIZON-MISS-"):
            return _extract_horizon_group_code(ident)

        # Other Horizon-branded identifiers: best-effort family extraction
        if ident.startswith("HORIZON-"):
            parts = [p for p in ident.split("-") if p]
            if len(parts) >= 2:
                return parts[1]

        # Last resort: callIdentifier/callTitle
        ci = safe_str(first(meta.get("callIdentifier"), "")).strip()
        if ci:
            return ci
        return safe_str(first(meta.get("callTitle"), "")).strip()

    def fallback_group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        return "_unknown_group"


# ----------------------------
# Programme: DEP / DIGITAL
# ----------------------------

def _derive_call_family_from_identifier(identifier: str) -> str:
    ident = normalize_space(identifier or "")
    if not ident:
        return ""
    parts = [p for p in ident.split("-") if p]
    if len(parts) < 2:
        return ident

    if len(parts) >= 4 and parts[0] == "DIGITAL" and parts[1].isdigit():
        return "-".join(parts[:4])

    if len(parts) >= 4 and parts[0] == "DIGITAL" and parts[1] in {"ECCC", "JU"}:
        return "-".join(parts[:4])

    return "-".join(parts[:4]) if len(parts) >= 4 else ident


class DepProgrammeGrouper(BaseProgrammeGrouper):
    programme_code = "DEP"

    def merge_config(self) -> SingletonMergeConfig:
        return SingletonMergeConfig(enabled=True, target_bucket="_other", sim_threshold=0.28)

    def group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        meta = self.get_metadata(meta_wrapper)
        ci = safe_str(first(meta.get("callIdentifier"), "")).strip()
        if ci:
            return ci

        ident = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
        if ident:
            return _derive_call_family_from_identifier(ident)

        return safe_str(first(meta.get("callTitle"), "")).strip()

    def fallback_group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        meta = self.get_metadata(meta_wrapper)
        pd = meta.get("programmeDivision") or []
        if isinstance(pd, list) and pd:
            return ",".join([safe_str(x) for x in pd if safe_str(x)])
        return safe_str(first(meta.get("callTitle"), "")).strip()


# ----------------------------
# Programme: ERASMUS
# ----------------------------

class ErasmusEduProgrammeGrouper(BaseProgrammeGrouper):
    programme_code = "ERASMUS"

    def group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        meta = self.get_metadata(meta_wrapper)
        ident = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
        title = safe_str(first(meta.get("title"), meta_wrapper.get("title") or "")).strip().lower()

        if ident.startswith("ERASMUS-JMO-"):
            return "Jean Monnet Actions"

        if ident.startswith("ERASMUS-SPORT-"):
            if "sport events" in title or ident.endswith("-SNCESE"):
                return "Sport Events"
            if ident.endswith("-CB"):
                return "Sport Capacity Building"
            return "Sport Partnerships"

        if ident.startswith("ERASMUS-YOUTH-"):
            if "-CB-" in ident or "capacity building" in title:
                return "Youth Capacity Building"
            return "Youth Actions"

        if ident.startswith("ERASMUS-EDU-"):
            if "-POL-EXP-" in ident:
                return "Policy Experimentation"
            if "-PI-" in ident:
                return "Alliances & Policy Innovation"
            if "-PCOOP-" in ident:
                return "Cooperation Partnerships"
            if "-CBHE-" in ident:
                return "Capacity Building in Higher Education"
            if "-CB-VET-" in ident:
                return "Capacity Building in VET"
            if "-VIRT-EXCH-" in ident:
                return "Virtual Exchanges"

            if (
                any(x in ident for x in ["-PEX-", "-PE-", "-EUR-UNIV", "-EMJM-", "ECHE"])
                or "european universities" in title
                or "erasmus mundus" in title
                or "charter for higher education" in title
                or "european degree" in title
            ):
                return "Higher Education Excellence"

            return "Education Other Actions"

        return "Other ERASMUS Actions"

    def fallback_group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        meta = self.get_metadata(meta_wrapper)
        ident = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
        if ident.startswith("ERASMUS-"):
            parts = [p for p in ident.split("-") if p]
            if len(parts) >= 2:
                return f"{parts[0]}-{parts[1]}"
        return "Other ERASMUS Actions"

    def merge_config(self) -> SingletonMergeConfig:
        return SingletonMergeConfig(enabled=False)


# ----------------------------
# Default / CLI-driven grouper
# ----------------------------

class DefaultFieldGrouper(BaseProgrammeGrouper):
    def __init__(self, group_by: str, fallback_group_by: str):
        self._group_by = group_by
        self._fallback = fallback_group_by

    def group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        return self._get_value(meta_wrapper, self._group_by)

    def fallback_group_value(self, meta_wrapper: Dict[str, Any]) -> str:
        return self._get_value(meta_wrapper, self._fallback)

    def _get_value(self, meta_wrapper: Dict[str, Any], key: str) -> str:
        meta = self.get_metadata(meta_wrapper)

        if key == "callTitle":
            return safe_str(first(meta.get("callTitle"), "")).strip()

        if key == "callIdentifier":
            return safe_str(first(meta.get("callIdentifier"), "")).strip()

        if key == "programmeDivision":
            pd = meta.get("programmeDivision") or []
            if isinstance(pd, list) and pd:
                return ",".join([safe_str(x) for x in pd if safe_str(x)])
            return ""

        if key == "wpClass":
            ci = safe_str(first(meta.get("callIdentifier"), "")).strip()
            return ci or safe_str(first(meta.get("callTitle"), "")).strip()

        if key == "callFamily":
            ci = safe_str(first(meta.get("callIdentifier"), "")).strip()
            if ci:
                return ci
            ident = safe_str(first(meta.get("identifier"), meta_wrapper.get("identifier") or "")).strip()
            if ident:
                return _derive_call_family_from_identifier(ident)
            return safe_str(first(meta.get("callTitle"), "")).strip()

        return ""


# ----------------------------
# Registry
# ----------------------------

def get_programme_grouper(
    programme: str,
    *,
    group_by: str,
    fallback_group_by: str,
    cl3_map_path: str = "",
    missions_map_path: str = "",
    cl1_map_path: str = "",
) -> BaseProgrammeGrouper:
    p = (programme or "").strip().upper()
    if p == "HORIZON":
        return HorizonProgrammeGrouper(
            cl3_map_path=cl3_map_path,
            missions_map_path=missions_map_path,
            cl1_map_path=cl1_map_path,
        )
    if p == "DEP":
        return DepProgrammeGrouper()
    if p == "ERASMUS":
        return ErasmusEduProgrammeGrouper()

    return DefaultFieldGrouper(group_by=group_by, fallback_group_by=fallback_group_by)


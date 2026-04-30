"""
Helpers for mapping official Shenwan stock-history codes to the current taxonomy nodes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

from research.providers.base import OfficialIndustryHistorySnapshot


@dataclass(frozen=True)
class OfficialShenwanCandidateMatch:
    """One ranked taxonomy candidate for an official six-digit code."""

    taxonomy_industry_code: str
    overlap_count: int
    taxonomy_symbol_count: int
    precision: float
    recall: float


@dataclass(frozen=True)
class OfficialShenwanCodeMapping:
    """One inferred mapping from an official six-digit code to a taxonomy node."""

    official_industry_code: str
    best_taxonomy_industry_code: Optional[str]
    taxonomy_industry_code: Optional[str]
    overlap_count: int
    official_symbol_count: int
    taxonomy_symbol_count: int
    precision: float
    recall: float
    confidence: str
    mapping_source: str = "inferred"
    override_reason: Optional[str] = None
    candidate_rankings: List[OfficialShenwanCandidateMatch] = field(default_factory=list)


class OfficialShenwanCodeMapper:
    """Infer official-code mappings from overlapping stock memberships."""

    def __init__(
        self,
        *,
        min_overlap_count: int = 2,
        min_precision: float = 0.6,
        min_recall: float = 0.6,
        max_candidate_count: int = 3,
    ):
        self.min_overlap_count = max(1, int(min_overlap_count))
        self.min_precision = float(min_precision)
        self.min_recall = float(min_recall)
        self.max_candidate_count = max(1, int(max_candidate_count))

    def infer_mappings(
        self,
        *,
        official_snapshots: Iterable[OfficialIndustryHistorySnapshot],
        taxonomy_components: Dict[str, Set[str]],
    ) -> List[OfficialShenwanCodeMapping]:
        official_to_symbols = self._group_official_symbols(official_snapshots)
        mappings: List[OfficialShenwanCodeMapping] = []

        for official_code, official_symbols in sorted(official_to_symbols.items()):
            candidate_rankings: List[OfficialShenwanCandidateMatch] = []

            for taxonomy_code, taxonomy_symbols in taxonomy_components.items():
                overlap_count = len(official_symbols & taxonomy_symbols)
                if overlap_count == 0:
                    continue

                precision = overlap_count / max(len(taxonomy_symbols), 1)
                recall = overlap_count / max(len(official_symbols), 1)
                candidate_rankings.append(
                    OfficialShenwanCandidateMatch(
                        taxonomy_industry_code=taxonomy_code,
                        overlap_count=overlap_count,
                        taxonomy_symbol_count=len(taxonomy_symbols),
                        precision=precision,
                        recall=recall,
                    )
                )

            ranked_candidates = sorted(
                candidate_rankings,
                key=lambda item: (
                    item.overlap_count,
                    item.precision,
                    item.recall,
                    item.taxonomy_industry_code,
                ),
                reverse=True,
            )[: self.max_candidate_count]
            best_candidate = ranked_candidates[0] if ranked_candidates else None
            best_taxonomy_code = (
                best_candidate.taxonomy_industry_code if best_candidate else None
            )
            best_overlap = best_candidate.overlap_count if best_candidate else 0
            best_taxonomy_symbol_count = (
                best_candidate.taxonomy_symbol_count if best_candidate else 0
            )
            best_precision = best_candidate.precision if best_candidate else 0.0
            best_recall = best_candidate.recall if best_candidate else 0.0

            confidence = self._classify_confidence(
                overlap_count=best_overlap,
                precision=best_precision,
                recall=best_recall,
            )
            mappings.append(
                OfficialShenwanCodeMapping(
                    official_industry_code=official_code,
                    best_taxonomy_industry_code=best_taxonomy_code,
                    taxonomy_industry_code=best_taxonomy_code if confidence != "unmapped" else None,
                    overlap_count=best_overlap,
                    official_symbol_count=len(official_symbols),
                    taxonomy_symbol_count=best_taxonomy_symbol_count,
                    precision=best_precision,
                    recall=best_recall,
                    confidence=confidence,
                    candidate_rankings=ranked_candidates,
                )
            )

        return mappings

    @staticmethod
    def _group_official_symbols(
        official_snapshots: Iterable[OfficialIndustryHistorySnapshot],
    ) -> Dict[str, Set[str]]:
        grouped: Dict[str, Set[str]] = {}
        for snapshot in official_snapshots:
            code = str(snapshot.official_industry_code or "").strip()
            symbol = str(snapshot.symbol or "").strip()
            if not code or not symbol:
                continue
            grouped.setdefault(code, set()).add(symbol)
        return grouped

    def _classify_confidence(
        self,
        *,
        overlap_count: int,
        precision: float,
        recall: float,
    ) -> str:
        if (
            overlap_count < self.min_overlap_count
            or precision < self.min_precision
            or recall < self.min_recall
        ):
            return "unmapped"
        if precision >= 0.9 and recall >= 0.9:
            return "high"
        return "medium"

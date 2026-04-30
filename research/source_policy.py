"""
Research source routing policy resolver.

Phase 0 only resolves configuration-driven source plans. It does not fetch data.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from utils.config_manager import ResearchConfig, config_manager


VALID_BUDGET_MODES = {"free_only", "balanced", "availability_first"}


@dataclass(frozen=True)
class ResearchSourceCandidate:
    """One concrete source candidate for a research data domain."""

    source: str
    mode: str
    stage: str
    paid: bool
    cost_tier: str
    enabled: bool
    reason: str


@dataclass(frozen=True)
class ResearchSourcePlan:
    """Resolved source plan for one research data domain."""

    domain: str
    budget_mode: str
    allow_paid_proxy: bool
    candidates: List[ResearchSourceCandidate]

    @property
    def source_keys(self) -> List[Tuple[str, str]]:
        return [(candidate.source, candidate.mode) for candidate in self.candidates]


class ResearchSourcePolicyResolver:
    """Resolve source chains for research domains with cost-aware proxy handling."""

    _chain_order = {
        "free_only": ("free_chain", "fallback_chain"),
        "balanced": ("free_chain", "fallback_chain", "paid_chain"),
        "availability_first": ("free_chain", "paid_chain", "fallback_chain"),
    }

    def __init__(self, research_config: Optional[ResearchConfig] = None):
        self.research_config = research_config or config_manager.get_research_config()

    def resolve(
        self,
        domain: str,
        *,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        max_candidates: Optional[int] = None,
    ) -> ResearchSourcePlan:
        """Resolve the candidate chain for a research data domain."""
        effective_mode = budget_mode or self.research_config.budget.default_mode
        if effective_mode not in VALID_BUDGET_MODES:
            raise ValueError(
                f"Unsupported budget mode: {effective_mode}. "
                f"Expected one of {sorted(VALID_BUDGET_MODES)}"
            )

        effective_allow_paid = (
            self.research_config.budget.allow_paid_proxy
            if allow_paid_proxy is None
            else allow_paid_proxy
        )

        route = self.research_config.routing.get(domain, {})
        if not route:
            raise KeyError(f"Missing research route config for domain: {domain}")

        candidates: List[ResearchSourceCandidate] = []
        seen: set[Tuple[str, str]] = set()
        paid_count = 0
        paid_cap = self.research_config.budget.max_paid_candidates_per_domain

        for stage in self._chain_order[effective_mode]:
            if stage == "paid_chain" and not effective_allow_paid:
                continue

            for item in route.get(stage, []):
                candidate = self._build_candidate(item, stage)
                if not candidate.enabled:
                    continue

                key = (candidate.source, candidate.mode)
                if key in seen:
                    continue

                if candidate.paid:
                    if not effective_allow_paid:
                        continue
                    if paid_cap >= 0 and paid_count >= paid_cap:
                        continue
                    paid_count += 1

                candidates.append(candidate)
                seen.add(key)

                if max_candidates is not None and len(candidates) >= max_candidates:
                    return ResearchSourcePlan(
                        domain=domain,
                        budget_mode=effective_mode,
                        allow_paid_proxy=effective_allow_paid,
                        candidates=candidates,
                    )

        return ResearchSourcePlan(
            domain=domain,
            budget_mode=effective_mode,
            allow_paid_proxy=effective_allow_paid,
            candidates=candidates,
        )

    def _build_candidate(
        self,
        item: Dict[str, Any],
        stage: str,
    ) -> ResearchSourceCandidate:
        source_name = item["source"]
        mode = item.get("mode", "direct")

        source_cfg = self.research_config.sources.get(source_name, {})
        enabled = source_cfg.get("enabled", False)

        paid = False
        cost_tier = source_cfg.get("cost_tier", "unknown")
        reason = self._default_reason(stage)

        if mode == "proxy_patch":
            proxy_cfg = source_cfg.get("proxy_patch", {})
            enabled = enabled and source_cfg.get("supports_proxy_patch", False)
            paid = proxy_cfg.get("cost_tier", "free") != "free"
            cost_tier = proxy_cfg.get("cost_tier", cost_tier)
            reason = proxy_cfg.get("note", reason)

        return ResearchSourceCandidate(
            source=source_name,
            mode=mode,
            stage=stage,
            paid=paid,
            cost_tier=cost_tier,
            enabled=enabled,
            reason=reason,
        )

    @staticmethod
    def _default_reason(stage: str) -> str:
        if stage == "free_chain":
            return "low-cost direct source candidate"
        if stage == "paid_chain":
            return "higher-availability candidate that may incur proxy cost"
        return "fallback source candidate"

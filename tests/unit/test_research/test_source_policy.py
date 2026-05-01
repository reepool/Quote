from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)
from research.source_policy import ResearchSourcePolicyResolver


def _build_research_config(
    *,
    allow_paid_proxy: bool = True,
    default_mode: str = "availability_first",
    max_paid_candidates_per_domain: int = 1,
) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(),
        budget=ResearchBudgetConfig(
            default_mode=default_mode,
            allow_paid_proxy=allow_paid_proxy,
            max_paid_candidates_per_domain=max_paid_candidates_per_domain,
        ),
        sources={
            "efinance": {
                "enabled": True,
                "cost_tier": "free",
                "supports_proxy_patch": True,
                "proxy_patch": {
                    "cost_tier": "paid",
                    "note": "efinance proxy path",
                },
            },
            "baostock": {
                "enabled": True,
                "cost_tier": "free",
                "supports_proxy_patch": False,
            },
            "akshare": {
                "enabled": True,
                "cost_tier": "free",
                "supports_proxy_patch": True,
                "proxy_patch": {
                    "cost_tier": "paid",
                    "note": "akshare proxy path",
                },
            },
            "cninfo": {
                "enabled": True,
                "cost_tier": "free",
                "supports_proxy_patch": False,
            },
        },
        routing={
            "company_profile": {
                "free_chain": [
                    {"source": "baostock", "mode": "direct"},
                ],
                "paid_chain": [
                    {"source": "akshare", "mode": "proxy_patch"},
                ],
                "fallback_chain": [
                    {"source": "efinance", "mode": "direct"},
                    {"source": "cninfo", "mode": "direct"},
                ],
            },
            "shareholders": {
                "free_chain": [],
                "paid_chain": [
                    {"source": "akshare", "mode": "proxy_patch"},
                    {"source": "efinance", "mode": "proxy_patch"},
                ],
                "fallback_chain": [
                    {"source": "cninfo", "mode": "direct"},
                    {"source": "akshare", "mode": "direct"},
                    {"source": "efinance", "mode": "direct"},
                ],
            },
            "financial_statements": {
                "free_chain": [],
                "paid_chain": [
                    {"source": "akshare", "mode": "proxy_patch"},
                ],
                "fallback_chain": [
                    {"source": "akshare", "mode": "direct"},
                ],
            }
        },
    )


def test_balanced_plan_prefers_free_and_fallback_when_paid_proxy_disabled():
    resolver = ResearchSourcePolicyResolver(
        _build_research_config(
            allow_paid_proxy=False,
            default_mode="balanced",
        )
    )

    plan = resolver.resolve("company_profile")

    assert plan.source_keys == [
        ("baostock", "direct"),
        ("efinance", "direct"),
        ("cninfo", "direct"),
    ]


def test_availability_first_can_append_paid_proxy_candidates():
    resolver = ResearchSourcePolicyResolver(
        _build_research_config(
            allow_paid_proxy=True,
            default_mode="availability_first",
        )
    )

    plan = resolver.resolve("company_profile")

    assert plan.source_keys == [
        ("baostock", "direct"),
        ("akshare", "proxy_patch"),
        ("efinance", "direct"),
        ("cninfo", "direct"),
    ]
    assert plan.candidates[1].paid is True


def test_paid_proxy_candidates_respect_budget_cap():
    resolver = ResearchSourcePolicyResolver(
        _build_research_config(
            allow_paid_proxy=True,
            default_mode="availability_first",
            max_paid_candidates_per_domain=1,
        )
    )

    plan = resolver.resolve("shareholders")

    assert ("akshare", "proxy_patch") in plan.source_keys
    assert ("efinance", "proxy_patch") not in plan.source_keys


def test_disabled_source_is_skipped_from_plan():
    config = _build_research_config(
        allow_paid_proxy=True,
        default_mode="availability_first",
        max_paid_candidates_per_domain=2,
    )
    config.sources["efinance"]["enabled"] = False

    resolver = ResearchSourcePolicyResolver(config)
    plan = resolver.resolve("company_profile")

    assert ("efinance", "direct") not in plan.source_keys
    assert ("efinance", "proxy_patch") not in plan.source_keys
    assert plan.source_keys == [
        ("baostock", "direct"),
        ("akshare", "proxy_patch"),
        ("cninfo", "direct"),
    ]


def test_shareholder_plan_prefers_paid_proxy_before_unstable_direct_fallbacks():
    resolver = ResearchSourcePolicyResolver(_build_research_config())

    plan = resolver.resolve("shareholders")

    assert plan.source_keys == [
        ("akshare", "proxy_patch"),
        ("cninfo", "direct"),
        ("akshare", "direct"),
        ("efinance", "direct"),
    ]


def test_financial_statements_plan_prefers_proxy_patch_before_direct():
    resolver = ResearchSourcePolicyResolver(_build_research_config())

    plan = resolver.resolve("financial_statements")

    assert plan.source_keys == [
        ("akshare", "proxy_patch"),
        ("akshare", "direct"),
    ]


def test_financial_statements_plan_skips_domain_disabled_source():
    config = _build_research_config()
    config.routing["financial_statements"]["free_chain"] = [
        {"source": "cninfo", "mode": "direct"},
    ]
    config.sources["cninfo"]["financial_statements"] = {"enabled": False}
    resolver = ResearchSourcePolicyResolver(config)

    plan = resolver.resolve("financial_statements")

    assert ("cninfo", "direct") not in plan.source_keys
    assert plan.source_keys == [
        ("akshare", "proxy_patch"),
        ("akshare", "direct"),
    ]


def test_financial_statements_plan_prefers_enabled_official_source_before_akshare():
    config = _build_research_config()
    config.sources["sse"] = {
        "enabled": True,
        "cost_tier": "free",
        "supports_proxy_patch": False,
        "financial_statements": {"enabled": True},
    }
    config.routing["financial_statements"]["free_chain"] = [
        {"source": "sse", "mode": "direct"},
    ]
    resolver = ResearchSourcePolicyResolver(config)

    plan = resolver.resolve("financial_statements")

    assert plan.source_keys == [
        ("sse", "direct"),
        ("akshare", "proxy_patch"),
        ("akshare", "direct"),
    ]

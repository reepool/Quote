"""Versioned product entities, aliases, and commodity candidate mappings."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


DEFAULT_PRODUCT_CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "business_profile_product_catalog.json"
)
DEFAULT_FUTURES_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "11_futures.json"
)
PRODUCT_CATALOG_SCHEMA_VERSION = "business_profile_product_catalog.v2"

INDUSTRY_GROUPS = {
    "coal",
    "nonferrous_and_solid_mineral",
    "steel",
    "petrochemical",
    "basic_chemical",
    "building_material",
}
PRODUCT_KINDS = {
    "raw_material",
    "intermediate",
    "finished_product",
    "energy_input",
    "byproduct",
}
ALIAS_MATCH_MODES = {"normalized_exact"}
ALIAS_REVIEW_POLICIES = {"auto_candidate_if_unique", "review_required"}
REFERENCE_TYPES = {"futures_instrument", "special_commodity"}
EXPOSURE_ROLES = {
    "revenue",
    "feedstock_cost",
    "energy_cost",
    "inventory",
    "hedge",
    "spread_leg",
}
AMBIGUITY_POLICIES = {"single_target", "one_to_many_review"}
EVIDENCE_REQUIREMENTS = {
    "explicit_product",
    "explicit_raw_material",
    "explicit_inventory",
    "explicit_hedge",
}
ROLE_EVIDENCE_REQUIREMENTS = {
    "revenue": {"explicit_product"},
    "feedstock_cost": {"explicit_raw_material"},
    "energy_cost": {"explicit_raw_material"},
    "inventory": {"explicit_inventory"},
    "hedge": {"explicit_hedge"},
    "spread_leg": {"explicit_product", "explicit_raw_material"},
}


@dataclass(frozen=True)
class ProductEntity:
    product_id: str
    label_zh: str
    label_en: str
    product_kind: str
    industry_groups: tuple[str, ...]
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["industry_groups"] = list(self.industry_groups)
        return payload


@dataclass(frozen=True)
class ProductAlias:
    alias_id: str
    alias: str
    normalized_alias: str
    product_ids: tuple[str, ...]
    industry_groups: tuple[str, ...]
    match_mode: str
    review_policy: str

    @property
    def ambiguous(self) -> bool:
        return len(self.product_ids) > 1

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("product_ids", "industry_groups"):
            payload[key] = list(payload[key])
        return payload


@dataclass(frozen=True)
class CommodityReference:
    reference_type: str
    reference_id: str
    priority: int


@dataclass(frozen=True)
class ProductCommodityMapping:
    mapping_id: str
    product_id: str
    exposure_role: str
    targets: tuple[CommodityReference, ...]
    ambiguity_policy: str
    evidence_requirement: str
    candidate_only: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["targets"] = [asdict(target) for target in self.targets]
        return payload


@dataclass(frozen=True)
class AliasResolution:
    normalized_alias: str
    product_ids: tuple[str, ...]
    matched_alias_ids: tuple[str, ...]
    review_required: bool
    diagnostics: tuple[str, ...]


@dataclass(frozen=True)
class BusinessProductCatalog:
    schema_version: str
    catalog_version: str
    released_on: str
    document_applicable_from: str
    document_applicable_to: Optional[str]
    products: tuple[ProductEntity, ...]
    aliases: tuple[ProductAlias, ...]
    commodity_mappings: tuple[ProductCommodityMapping, ...]

    def require_product(self, product_id: str) -> ProductEntity:
        target = str(product_id or "").strip()
        product = next(
            (item for item in self.products if item.product_id == target),
            None,
        )
        if product is None:
            raise KeyError(f"unknown business product_id: {product_id}")
        return product

    def resolve_alias(
        self,
        value: str,
        *,
        industry_group: Optional[str] = None,
    ) -> AliasResolution:
        """Resolve only an exact normalized source label.

        This method deliberately does not inspect surrounding prose. Unknown and
        ambiguous labels remain review items instead of being semantically guessed.
        """
        normalized = normalize_product_alias(value)
        industry = str(industry_group or "").strip()
        if industry and industry not in INDUSTRY_GROUPS:
            raise ValueError(f"unsupported product industry_group: {industry_group}")
        matches: list[ProductAlias] = []
        diagnostics: list[str] = []
        for alias in self.aliases:
            if alias.normalized_alias != normalized:
                continue
            if industry and industry not in alias.industry_groups:
                continue
            matches.append(alias)
        product_ids = tuple(
            sorted(
                {product_id for alias in matches for product_id in alias.product_ids}
            )
        )
        review_required = len(product_ids) != 1 or any(
            alias.review_policy == "review_required" for alias in matches
        )
        if not matches:
            diagnostics.append("alias_not_found")
        elif len(product_ids) > 1:
            diagnostics.append("ambiguous_product_alias")
        return AliasResolution(
            normalized_alias=normalized,
            product_ids=product_ids,
            matched_alias_ids=tuple(sorted(alias.alias_id for alias in matches)),
            review_required=review_required,
            diagnostics=tuple(sorted(set(diagnostics))),
        )

    def commodity_candidates(
        self,
        product_id: str,
        *,
        exposure_role: Optional[str] = None,
        evidence_requirement: Optional[str] = None,
    ) -> tuple[ProductCommodityMapping, ...]:
        self.require_product(product_id)
        role = str(exposure_role or "").strip()
        if role and role not in EXPOSURE_ROLES:
            raise ValueError(f"unsupported commodity exposure_role: {exposure_role}")
        evidence = str(evidence_requirement or "").strip()
        if evidence and evidence not in EVIDENCE_REQUIREMENTS:
            raise ValueError(
                "unsupported commodity evidence_requirement: "
                f"{evidence_requirement}"
            )
        return tuple(
            mapping
            for mapping in self.commodity_mappings
            if mapping.product_id == product_id
            and (not role or mapping.exposure_role == role)
            and (not evidence or mapping.evidence_requirement == evidence)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "catalog_version": self.catalog_version,
            "released_on": self.released_on,
            "document_applicable_from": self.document_applicable_from,
            "document_applicable_to": self.document_applicable_to,
            "products": [item.to_dict() for item in self.products],
            "aliases": [item.to_dict() for item in self.aliases],
            "commodity_mappings": [item.to_dict() for item in self.commodity_mappings],
        }


@lru_cache(maxsize=8)
def load_business_product_catalog(
    path: str | Path = DEFAULT_PRODUCT_CATALOG_PATH,
    *,
    version: Optional[str] = None,
    document_date: Optional[str] = None,
) -> BusinessProductCatalog:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    catalog = parse_business_product_catalog(
        payload,
        known_references=load_known_commodity_references(),
    )
    if version is not None and catalog.catalog_version != str(version):
        raise ValueError(
            "unsupported business product catalog version: "
            f"{version}; configured={catalog.catalog_version}"
        )
    _check_document_applicability(catalog, document_date)
    return catalog


@lru_cache(maxsize=1)
def load_known_commodity_references(
    futures_config_path: str | Path = DEFAULT_FUTURES_CONFIG_PATH,
) -> dict[str, frozenset[str]]:
    from research.futures_market_data import DEFAULT_P0_FUTURES_INSTRUMENTS

    payload = json.loads(Path(futures_config_path).read_text(encoding="utf-8"))
    special_items = (
        payload.get("futures_config", {})
        .get("special_commodity_market_data", {})
        .get("commodities", [])
    )
    return {
        "futures_instrument": frozenset(
            str(item.get("instrument_id") or "").strip()
            for item in DEFAULT_P0_FUTURES_INSTRUMENTS
            if str(item.get("instrument_id") or "").strip()
        ),
        "special_commodity": frozenset(
            str(item.get("commodity_id") or "").strip()
            for item in special_items
            if str(item.get("commodity_id") or "").strip()
        ),
    }


def parse_business_product_catalog(
    payload: Mapping[str, Any],
    *,
    known_references: Optional[Mapping[str, set[str] | frozenset[str]]] = None,
) -> BusinessProductCatalog:
    if not isinstance(payload, Mapping):
        raise ValueError("business product catalog root must be an object")
    schema_version = _required_text(payload, "schema_version", "catalog")
    if schema_version != PRODUCT_CATALOG_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported business product catalog schema_version: {schema_version}"
        )
    catalog_version = _required_text(payload, "catalog_version", "catalog")
    released_on = _required_text(payload, "released_on", "catalog")
    _parse_date(released_on, "released_on")
    applicable_from = _required_text(
        payload,
        "document_applicable_from",
        "catalog",
    )
    start = _parse_date(applicable_from, "document_applicable_from")
    applicable_to = str(payload.get("document_applicable_to") or "").strip() or None
    if (
        applicable_to
        and _parse_date(
            applicable_to,
            "document_applicable_to",
        )
        < start
    ):
        raise ValueError("business product catalog has invalid applicability interval")

    products = _parse_products(payload.get("products"))
    product_ids = {item.product_id for item in products}
    aliases = _parse_aliases(payload.get("aliases"), product_ids=product_ids)
    mappings = _parse_mappings(
        payload.get("commodity_mappings"),
        product_ids=product_ids,
        known_references=known_references,
    )
    covered_industries = {
        group for product in products for group in product.industry_groups
    }
    missing_industries = INDUSTRY_GROUPS - covered_industries
    if missing_industries:
        raise ValueError(
            "business product catalog missing first-wave industries: "
            f"{sorted(missing_industries)}"
        )
    return BusinessProductCatalog(
        schema_version=schema_version,
        catalog_version=catalog_version,
        released_on=released_on,
        document_applicable_from=applicable_from,
        document_applicable_to=applicable_to,
        products=products,
        aliases=aliases,
        commodity_mappings=mappings,
    )


def normalize_product_alias(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = re.sub(r"[\s\u3000]+", "", normalized)
    return normalized.replace("（", "(").replace("）", ")")


def _parse_products(value: Any) -> tuple[ProductEntity, ...]:
    rows = _non_empty_object_array(value, "products")
    output: list[ProductEntity] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        location = f"products[{index}]"
        product_id = _required_text(row, "product_id", location)
        if product_id in seen:
            raise ValueError(f"duplicate business product_id: {product_id}")
        seen.add(product_id)
        product_kind = _required_text(row, "product_kind", location)
        if product_kind not in PRODUCT_KINDS:
            raise ValueError(f"{location} unsupported product_kind: {product_kind}")
        industry_groups = _enum_tuple(
            row.get("industry_groups"),
            INDUSTRY_GROUPS,
            "industry_groups",
            location,
        )
        output.append(
            ProductEntity(
                product_id=product_id,
                label_zh=_required_text(row, "label_zh", location),
                label_en=_required_text(row, "label_en", location),
                product_kind=product_kind,
                industry_groups=industry_groups,
                notes=str(row.get("notes") or "").strip(),
            )
        )
    return tuple(output)


def _parse_aliases(
    value: Any,
    *,
    product_ids: set[str],
) -> tuple[ProductAlias, ...]:
    rows = _non_empty_object_array(value, "aliases")
    output: list[ProductAlias] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        location = f"aliases[{index}]"
        forbidden = {
            "required_context_terms",
            "excluded_context_terms",
        } & set(row)
        if forbidden:
            raise ValueError(
                f"{location} contains prohibited prose-inference fields: "
                f"{sorted(forbidden)}"
            )
        alias_id = _required_text(row, "alias_id", location)
        if alias_id in seen:
            raise ValueError(f"duplicate business product alias_id: {alias_id}")
        seen.add(alias_id)
        alias = _required_text(row, "alias", location)
        configured_normalized = _required_text(
            row,
            "normalized_alias",
            location,
        )
        if configured_normalized != normalize_product_alias(alias):
            raise ValueError(f"{location} normalized_alias does not match alias")
        referenced_products = _string_tuple(
            row.get("product_ids"),
            "product_ids",
            location,
        )
        unknown_products = set(referenced_products) - product_ids
        if unknown_products:
            raise ValueError(
                f"{location} references unknown product_ids: "
                f"{sorted(unknown_products)}"
            )
        review_policy = _enum_text(
            row,
            "review_policy",
            ALIAS_REVIEW_POLICIES,
            location,
        )
        if len(referenced_products) > 1 and review_policy != "review_required":
            raise ValueError(
                f"{location} ambiguous alias requires review_required policy"
            )
        output.append(
            ProductAlias(
                alias_id=alias_id,
                alias=alias,
                normalized_alias=configured_normalized,
                product_ids=referenced_products,
                industry_groups=_enum_tuple(
                    row.get("industry_groups"),
                    INDUSTRY_GROUPS,
                    "industry_groups",
                    location,
                ),
                match_mode=_enum_text(
                    row,
                    "match_mode",
                    ALIAS_MATCH_MODES,
                    location,
                ),
                review_policy=review_policy,
            )
        )
    return tuple(output)


def _parse_mappings(
    value: Any,
    *,
    product_ids: set[str],
    known_references: Optional[Mapping[str, set[str] | frozenset[str]]],
) -> tuple[ProductCommodityMapping, ...]:
    rows = _non_empty_object_array(value, "commodity_mappings")
    output: list[ProductCommodityMapping] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        location = f"commodity_mappings[{index}]"
        forbidden = {"allowed_value_chain_roles", "exclusion_terms"} & set(row)
        if forbidden:
            raise ValueError(
                f"{location} contains prohibited semantic-role fields: "
                f"{sorted(forbidden)}"
            )
        mapping_id = _required_text(row, "mapping_id", location)
        if mapping_id in seen:
            raise ValueError(f"duplicate product commodity mapping_id: {mapping_id}")
        seen.add(mapping_id)
        product_id = _required_text(row, "product_id", location)
        if product_id not in product_ids:
            raise ValueError(f"{location} references unknown product_id: {product_id}")
        targets = _parse_targets(
            row.get("targets"),
            location=location,
            known_references=known_references,
        )
        ambiguity_policy = _enum_text(
            row,
            "ambiguity_policy",
            AMBIGUITY_POLICIES,
            location,
        )
        if len(targets) > 1 and ambiguity_policy != "one_to_many_review":
            raise ValueError(f"{location} multiple targets require one_to_many_review")
        candidate_only = row.get("candidate_only")
        if candidate_only is not True:
            raise ValueError(f"{location} must remain candidate_only")
        exposure_role = _enum_text(
            row,
            "exposure_role",
            EXPOSURE_ROLES,
            location,
        )
        evidence_requirement = _enum_text(
            row,
            "evidence_requirement",
            EVIDENCE_REQUIREMENTS,
            location,
        )
        if evidence_requirement not in ROLE_EVIDENCE_REQUIREMENTS[exposure_role]:
            raise ValueError(
                f"{location} evidence_requirement is incompatible with "
                f"exposure_role: {evidence_requirement} vs {exposure_role}"
            )
        output.append(
            ProductCommodityMapping(
                mapping_id=mapping_id,
                product_id=product_id,
                exposure_role=exposure_role,
                targets=targets,
                ambiguity_policy=ambiguity_policy,
                evidence_requirement=evidence_requirement,
                candidate_only=True,
            )
        )
    return tuple(output)


def _parse_targets(
    value: Any,
    *,
    location: str,
    known_references: Optional[Mapping[str, set[str] | frozenset[str]]],
) -> tuple[CommodityReference, ...]:
    rows = _non_empty_object_array(value, f"{location}.targets")
    output: list[CommodityReference] = []
    seen: set[tuple[str, str]] = set()
    priorities: set[int] = set()
    for index, row in enumerate(rows):
        target_location = f"{location}.targets[{index}]"
        reference_type = _enum_text(
            row,
            "reference_type",
            REFERENCE_TYPES,
            target_location,
        )
        reference_id = _required_text(row, "reference_id", target_location)
        key = (reference_type, reference_id)
        if key in seen:
            raise ValueError(f"{target_location} duplicates commodity reference")
        seen.add(key)
        priority = row.get("priority")
        if isinstance(priority, bool) or not isinstance(priority, int) or priority < 1:
            raise ValueError(f"{target_location} priority must be a positive integer")
        if priority in priorities:
            raise ValueError(f"{location} target priorities must be unique")
        priorities.add(priority)
        if known_references is not None and reference_id not in known_references.get(
            reference_type,
            set(),
        ):
            raise ValueError(
                f"{target_location} references unknown {reference_type}: "
                f"{reference_id}"
            )
        output.append(
            CommodityReference(
                reference_type=reference_type,
                reference_id=reference_id,
                priority=priority,
            )
        )
    return tuple(sorted(output, key=lambda item: item.priority))


def _check_document_applicability(
    catalog: BusinessProductCatalog,
    document_date: Optional[str],
) -> None:
    if document_date is None:
        return
    requested = _parse_date(document_date, "document_date")
    start = _parse_date(
        catalog.document_applicable_from,
        "document_applicable_from",
    )
    end = (
        _parse_date(
            catalog.document_applicable_to,
            "document_applicable_to",
        )
        if catalog.document_applicable_to
        else None
    )
    if requested < start or (end is not None and requested > end):
        raise ValueError(
            "business product catalog is not applicable to document_date: "
            f"{document_date}"
        )


def _non_empty_object_array(value: Any, key: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"business product catalog {key} must be a non-empty array")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"business product catalog {key} must contain objects")
    return value


def _required_text(row: Mapping[str, Any], key: str, location: str) -> str:
    value = str(row.get(key) or "").strip()
    if not value:
        raise ValueError(f"business product catalog {location} missing {key}")
    return value


def _enum_text(
    row: Mapping[str, Any],
    key: str,
    allowed: set[str],
    location: str,
) -> str:
    value = _required_text(row, key, location)
    if value not in allowed:
        raise ValueError(
            f"business product catalog {location} unsupported {key}: {value}"
        )
    return value


def _enum_tuple(
    value: Any,
    allowed: set[str],
    key: str,
    location: str,
) -> tuple[str, ...]:
    result = _string_tuple(value, key, location)
    unsupported = set(result) - allowed
    if unsupported:
        raise ValueError(
            f"business product catalog {location} unsupported {key}: "
            f"{sorted(unsupported)}"
        )
    return result


def _string_tuple(
    value: Any,
    key: str,
    location: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"business product catalog {location} {key} must be an array")
    result = tuple(str(item).strip() for item in value if str(item).strip())
    if not allow_empty and not result:
        raise ValueError(f"business product catalog {location} {key} must not be empty")
    if len(set(result)) != len(result):
        raise ValueError(
            f"business product catalog {location} {key} contains duplicates"
        )
    return result


def _parse_date(value: Any, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"business product catalog {field_name} must be an ISO date"
        ) from exc

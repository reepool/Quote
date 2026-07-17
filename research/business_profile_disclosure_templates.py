"""Versioned disclosure templates for business-profile document parsing."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from research.business_profile_fact_catalog import load_business_fact_catalog


DEFAULT_DISCLOSURE_TEMPLATE_CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "business_profile_disclosure_template_catalog.json"
)
DISCLOSURE_TEMPLATE_CATALOG_SCHEMA_VERSION = (
    "business_profile_disclosure_template_catalog.v1"
)

EXCHANGES = {"SSE", "SZSE", "BSE"}
BOARDS = {"main", "star", "chinext", "bse"}
EXCHANGE_BOARDS = {
    "SSE": {"main", "star"},
    "SZSE": {"main", "chinext"},
    "BSE": {"bse"},
}
DOCUMENT_TYPES = {
    "annual_report",
    "annual_report_correction",
    "semiannual_report",
    "semiannual_report_correction",
}
INDUSTRY_GROUPS = {
    "all",
    "coal",
    "nonferrous_and_solid_mineral",
    "steel",
    "petrochemical",
    "basic_chemical",
    "building_material",
}
AUTHORITY_TYPES = {
    "csrc_common_rule",
    "exchange_industry_rule",
    "observed_parser_pattern",
}
ROW_ROLES = {
    "total",
    "subtotal",
    "elimination",
    "unallocated",
    "not_applicable",
}
_EXCHANGE_ALIASES = {
    "SH": "SSE",
    "SSE": "SSE",
    "SHSE": "SSE",
    "SZ": "SZSE",
    "SZSE": "SZSE",
    "BJ": "BSE",
    "BSE": "BSE",
}
_BOARD_ALIASES = {
    "main": "main",
    "main_board": "main",
    "mainboard": "main",
    "主板": "main",
    "沪市主板": "main",
    "深市主板": "main",
    "star": "star",
    "star_market": "star",
    "science_technology_innovation_board": "star",
    "科创板": "star",
    "chinext": "chinext",
    "chi_next": "chinext",
    "gem": "chinext",
    "gem_board": "chinext",
    "创业板": "chinext",
    "bse": "bse",
    "bse_board": "bse",
    "beijing": "bse",
    "北交所": "bse",
}


@dataclass(frozen=True)
class DisclosureTableSignature:
    """Deterministic header signature for one disclosed table family."""

    signature_id: str
    fact_group: str
    section_keys: tuple[str, ...]
    required_headers: tuple[str, ...]
    optional_headers: tuple[str, ...]
    min_required_header_matches: int
    field_ids: tuple[str, ...]
    row_role_markers: tuple[tuple[str, tuple[str, ...]], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in (
            "section_keys",
            "required_headers",
            "optional_headers",
            "field_ids",
        ):
            payload[key] = list(payload[key])
        payload["row_role_markers"] = {
            role: list(markers) for role, markers in self.row_role_markers
        }
        return payload


@dataclass(frozen=True)
class DisclosureRuleScope:
    """One exact market and time scope for a regulatory or observed rule."""

    scope_id: str
    exchange: str
    boards: tuple[str, ...]
    document_types: tuple[str, ...]
    authority_type: str
    rule_version: str
    effective_from: str
    effective_to: Optional[str]
    source_rule_refs: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["boards"] = list(self.boards)
        payload["document_types"] = list(self.document_types)
        payload["source_rule_refs"] = list(self.source_rule_refs)
        return payload


@dataclass(frozen=True)
class DisclosureTemplate:
    """Reusable parsing signatures with explicit market rule scopes."""

    template_id: str
    document_types: tuple[str, ...]
    industry_groups: tuple[str, ...]
    scopes: tuple[DisclosureRuleScope, ...]
    section_aliases: tuple[tuple[str, tuple[str, ...]], ...]
    table_signatures: tuple[DisclosureTableSignature, ...]

    def section_alias_map(self) -> dict[str, tuple[str, ...]]:
        return dict(self.section_aliases)

    def to_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "document_types": list(self.document_types),
            "industry_groups": list(self.industry_groups),
            "scopes": [scope.to_dict() for scope in self.scopes],
            "section_aliases": {
                key: list(values) for key, values in self.section_aliases
            },
            "table_signatures": [
                signature.to_dict() for signature in self.table_signatures
            ],
        }


@dataclass(frozen=True)
class ResolvedDisclosureTemplate:
    """A parsing template bound to the exact rule scope used for lineage."""

    template: DisclosureTemplate
    scope: DisclosureRuleScope

    @property
    def template_id(self) -> str:
        return self.template.template_id

    @property
    def industry_groups(self) -> tuple[str, ...]:
        return self.template.industry_groups

    @property
    def rule_version(self) -> str:
        return self.scope.rule_version

    @property
    def authority_type(self) -> str:
        return self.scope.authority_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "template": self.template.to_dict(),
            "matched_scope": self.scope.to_dict(),
        }


@dataclass(frozen=True)
class DisclosureTemplateCatalog:
    """Validated collection of effective disclosure templates."""

    schema_version: str
    catalog_version: str
    fact_catalog_version: str
    templates: tuple[DisclosureTemplate, ...]

    def select(
        self,
        *,
        document_date: str,
        exchange: str,
        board: str,
        document_type: str,
        industry_group: Optional[str] = None,
    ) -> tuple[ResolvedDisclosureTemplate, ...]:
        normalized_exchange = normalize_exchange(exchange)
        normalized_board = normalize_board(board)
        normalized_document_type = str(document_type or "").strip().lower()
        normalized_industry = str(industry_group or "").strip().lower() or None
        if normalized_exchange not in EXCHANGES:
            raise ValueError(f"unsupported disclosure template exchange: {exchange}")
        if normalized_board not in BOARDS:
            raise ValueError(f"unsupported disclosure template board: {board}")
        if normalized_board not in EXCHANGE_BOARDS[normalized_exchange]:
            raise ValueError(
                "invalid disclosure template exchange and board combination: "
                f"{normalized_exchange}/{normalized_board}"
            )
        if normalized_document_type not in DOCUMENT_TYPES:
            raise ValueError(
                "unsupported disclosure template document_type: " f"{document_type}"
            )
        if normalized_industry is not None and normalized_industry not in (
            INDUSTRY_GROUPS - {"all"}
        ):
            raise ValueError(
                "unsupported disclosure template industry_group: " f"{industry_group}"
            )
        cutoff = _parse_date(document_date, "document_date")
        selected: list[ResolvedDisclosureTemplate] = []
        for template in self.templates:
            if normalized_document_type not in template.document_types or not (
                "all" in template.industry_groups
                or (
                    normalized_industry is not None
                    and normalized_industry in template.industry_groups
                )
            ):
                continue
            scopes = [
                scope
                for scope in template.scopes
                if scope.exchange == normalized_exchange
                and normalized_board in scope.boards
                and normalized_document_type in scope.document_types
                and _scope_is_effective(scope, cutoff)
            ]
            if len(scopes) > 1:
                raise ValueError(
                    "disclosure template selection matched overlapping scopes: "
                    f"template={template.template_id}, "
                    f"scopes={[scope.scope_id for scope in scopes]}"
                )
            if scopes:
                selected.append(
                    ResolvedDisclosureTemplate(
                        template=template,
                        scope=scopes[0],
                    )
                )
        ordered = tuple(
            sorted(
                selected,
                key=lambda item: (
                    0 if "all" in item.template.industry_groups else 1,
                    item.template_id,
                ),
            )
        )
        if not ordered:
            raise ValueError(
                "no effective disclosure template for selection: "
                f"date={document_date}, exchange={normalized_exchange}, "
                f"board={normalized_board}, document_type={normalized_document_type}, "
                f"industry_group={normalized_industry or 'none'}"
            )
        common_templates = [
            template
            for template in ordered
            if "all" in template.template.industry_groups
        ]
        if len(common_templates) != 1:
            raise ValueError(
                "disclosure template selection requires exactly one common "
                f"template; matched={len(common_templates)}"
            )
        if normalized_industry is not None:
            industry_templates = [
                template
                for template in ordered
                if normalized_industry in template.template.industry_groups
            ]
            if len(industry_templates) != 1:
                raise ValueError(
                    "disclosure template selection requires exactly one industry "
                    f"template for {normalized_industry}; "
                    f"matched={len(industry_templates)}"
                )
        return ordered

    def merged_section_aliases(
        self,
        **selection: Any,
    ) -> dict[str, tuple[str, ...]]:
        merged: dict[str, list[str]] = {}
        for resolved in self.select(**selection):
            for section_key, aliases in resolved.template.section_aliases:
                current = merged.setdefault(section_key, [])
                for alias in aliases:
                    if alias not in current:
                        current.append(alias)
        return {key: tuple(values) for key, values in sorted(merged.items())}


@lru_cache(maxsize=8)
def load_disclosure_template_catalog(
    path: str | Path = DEFAULT_DISCLOSURE_TEMPLATE_CATALOG_PATH,
    *,
    version: Optional[str] = None,
) -> DisclosureTemplateCatalog:
    """Load templates and validate all referenced business fact fields."""

    catalog_path = Path(path)
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    fact_catalog = load_business_fact_catalog()
    fact_field_ids = {definition.field_id for definition in fact_catalog.definitions}
    catalog = parse_disclosure_template_catalog(
        payload,
        fact_field_ids=fact_field_ids,
    )
    if catalog.fact_catalog_version != fact_catalog.catalog_version:
        raise ValueError(
            "disclosure template fact catalog version mismatch: "
            f"required={catalog.fact_catalog_version}; "
            f"configured={fact_catalog.catalog_version}"
        )
    if version is not None and catalog.catalog_version != str(version):
        raise ValueError(
            "unsupported disclosure template catalog version: "
            f"{version}; configured={catalog.catalog_version}"
        )
    return catalog


def parse_disclosure_template_catalog(
    payload: Mapping[str, Any],
    *,
    fact_field_ids: Optional[set[str]] = None,
) -> DisclosureTemplateCatalog:
    """Validate an in-memory disclosure template catalog."""

    if not isinstance(payload, Mapping):
        raise ValueError("disclosure template catalog root must be an object")
    schema_version = _required_text(payload, "schema_version")
    if schema_version != DISCLOSURE_TEMPLATE_CATALOG_SCHEMA_VERSION:
        raise ValueError(
            "unsupported disclosure template catalog schema_version: "
            f"{schema_version}"
        )
    catalog_version = _required_text(payload, "catalog_version")
    fact_catalog_version = _required_text(payload, "fact_catalog_version")
    rows = payload.get("templates")
    if not isinstance(rows, list) or not rows:
        raise ValueError("disclosure template catalog templates must be non-empty")

    templates: list[DisclosureTemplate] = []
    template_ids: set[str] = set()
    scope_ids: set[str] = set()
    signature_ids: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(
                f"disclosure template catalog templates[{index}] must be an object"
            )
        template = _parse_template(
            row,
            index=index,
            fact_field_ids=fact_field_ids,
        )
        if template.template_id in template_ids:
            raise ValueError(
                f"duplicate disclosure template_id: {template.template_id}"
            )
        template_ids.add(template.template_id)
        for scope in template.scopes:
            if scope.scope_id in scope_ids:
                raise ValueError(
                    f"duplicate disclosure rule scope_id: {scope.scope_id}"
                )
            scope_ids.add(scope.scope_id)
        for signature in template.table_signatures:
            if signature.signature_id in signature_ids:
                raise ValueError(
                    f"duplicate disclosure table signature_id: {signature.signature_id}"
                )
            signature_ids.add(signature.signature_id)
        templates.append(template)

    return DisclosureTemplateCatalog(
        schema_version=schema_version,
        catalog_version=catalog_version,
        fact_catalog_version=fact_catalog_version,
        templates=tuple(templates),
    )


def normalize_exchange(value: Any) -> str:
    return _EXCHANGE_ALIASES.get(str(value or "").strip().upper(), "")


def normalize_board(value: Any) -> str:
    raw = str(value or "").strip()
    return _BOARD_ALIASES.get(raw, _BOARD_ALIASES.get(raw.lower(), ""))


def _parse_template(
    row: Mapping[str, Any],
    *,
    index: int,
    fact_field_ids: Optional[set[str]],
) -> DisclosureTemplate:
    raw_scopes = row.get("scopes")
    if not isinstance(raw_scopes, list) or not raw_scopes:
        raise ValueError(
            f"disclosure template catalog templates[{index}] "
            "scopes must be non-empty"
        )
    scopes = tuple(
        _parse_scope(scope, template_index=index, scope_index=scope_index)
        for scope_index, scope in enumerate(raw_scopes)
    )
    document_types = _enum_tuple(row, "document_types", DOCUMENT_TYPES, index=index)
    industry_groups = _enum_tuple(row, "industry_groups", INDUSTRY_GROUPS, index=index)
    section_aliases = _parse_section_aliases(
        row.get("section_aliases"),
        index=index,
    )
    section_keys = {key for key, _ in section_aliases}
    raw_signatures = row.get("table_signatures")
    if not isinstance(raw_signatures, list) or not raw_signatures:
        raise ValueError(
            f"disclosure template catalog templates[{index}] "
            "table_signatures must be non-empty"
        )
    signatures = tuple(
        _parse_signature(
            signature,
            template_index=index,
            signature_index=signature_index,
            section_keys=section_keys,
            fact_field_ids=fact_field_ids,
        )
        for signature_index, signature in enumerate(raw_signatures)
    )
    return DisclosureTemplate(
        template_id=_required_text(row, "template_id", index=index),
        document_types=document_types,
        industry_groups=industry_groups,
        scopes=scopes,
        section_aliases=section_aliases,
        table_signatures=signatures,
    )


def _parse_scope(
    value: Any,
    *,
    template_index: int,
    scope_index: int,
) -> DisclosureRuleScope:
    location = f"templates[{template_index}].scopes[{scope_index}]"
    if not isinstance(value, Mapping):
        raise ValueError(f"disclosure template catalog {location} must be an object")
    exchange = _required_text(value, "exchange", location=location).upper()
    if exchange not in EXCHANGES:
        raise ValueError(
            f"disclosure template catalog {location} unsupported exchange: {exchange}"
        )
    boards = _enum_tuple_at_location(
        value,
        "boards",
        BOARDS,
        location=location,
    )
    invalid_boards = set(boards) - EXCHANGE_BOARDS[exchange]
    if invalid_boards:
        raise ValueError(
            f"disclosure template catalog {location} invalid boards for "
            f"{exchange}: {sorted(invalid_boards)}"
        )
    document_types = _enum_tuple_at_location(
        value,
        "document_types",
        DOCUMENT_TYPES,
        location=location,
    )
    authority_type = _required_text(
        value,
        "authority_type",
        location=location,
    )
    if authority_type not in AUTHORITY_TYPES:
        raise ValueError(
            f"disclosure template catalog {location} unsupported authority_type: "
            f"{authority_type}"
        )
    effective_from = _required_text(value, "effective_from", location=location)
    start = _parse_date(effective_from, "effective_from")
    effective_to = str(value.get("effective_to") or "").strip() or None
    if effective_to is not None and _parse_date(effective_to, "effective_to") < start:
        raise ValueError(f"disclosure template catalog {location} has invalid interval")
    return DisclosureRuleScope(
        scope_id=_required_text(value, "scope_id", location=location),
        exchange=exchange,
        boards=boards,
        document_types=document_types,
        authority_type=authority_type,
        rule_version=_required_text(value, "rule_version", location=location),
        effective_from=effective_from,
        effective_to=effective_to,
        source_rule_refs=_string_tuple(
            value.get("source_rule_refs"),
            "source_rule_refs",
            location,
        ),
    )


def _parse_section_aliases(
    value: Any,
    *,
    index: int,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(
            f"disclosure template catalog templates[{index}] "
            "section_aliases must be a non-empty object"
        )
    output: list[tuple[str, tuple[str, ...]]] = []
    for key, aliases in value.items():
        section_key = str(key or "").strip()
        if not section_key:
            raise ValueError(
                f"disclosure template catalog templates[{index}] "
                "contains an empty section key"
            )
        output.append(
            (
                section_key,
                _string_tuple(
                    aliases,
                    "aliases",
                    f"templates[{index}].section_aliases.{section_key}",
                ),
            )
        )
    return tuple(sorted(output))


def _parse_signature(
    value: Any,
    *,
    template_index: int,
    signature_index: int,
    section_keys: set[str],
    fact_field_ids: Optional[set[str]],
) -> DisclosureTableSignature:
    location = f"templates[{template_index}].table_signatures[{signature_index}]"
    if not isinstance(value, Mapping):
        raise ValueError(f"disclosure template catalog {location} must be an object")
    signature_sections = _string_tuple(
        value.get("section_keys"),
        "section_keys",
        location,
    )
    unknown_sections = set(signature_sections) - section_keys
    if unknown_sections:
        raise ValueError(
            f"disclosure template catalog {location} references unknown "
            f"section keys: {sorted(unknown_sections)}"
        )
    required_headers = _string_tuple(
        value.get("required_headers"),
        "required_headers",
        location,
    )
    minimum = value.get("min_required_header_matches")
    if isinstance(minimum, bool) or not isinstance(minimum, int):
        raise ValueError(
            f"disclosure template catalog {location} "
            "min_required_header_matches must be an integer"
        )
    if minimum < 1 or minimum > len(required_headers):
        raise ValueError(
            f"disclosure template catalog {location} has invalid "
            "min_required_header_matches"
        )
    field_ids = _string_tuple(value.get("field_ids"), "field_ids", location)
    if fact_field_ids is not None:
        unknown_fields = set(field_ids) - fact_field_ids
        if unknown_fields:
            raise ValueError(
                f"disclosure template catalog {location} references unknown "
                f"business fact fields: {sorted(unknown_fields)}"
            )
    return DisclosureTableSignature(
        signature_id=_required_text(value, "signature_id", location=location),
        fact_group=_required_text(value, "fact_group", location=location),
        section_keys=signature_sections,
        required_headers=required_headers,
        optional_headers=_string_tuple(
            value.get("optional_headers", ()),
            "optional_headers",
            location,
            allow_empty=True,
        ),
        min_required_header_matches=minimum,
        field_ids=field_ids,
        row_role_markers=_parse_row_role_markers(
            value.get("row_role_markers", {}),
            location=location,
        ),
    )


def _parse_row_role_markers(
    value: Any,
    *,
    location: str,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"disclosure template catalog {location} "
            "row_role_markers must be an object"
        )
    output: list[tuple[str, tuple[str, ...]]] = []
    for role, markers in value.items():
        normalized_role = str(role or "").strip()
        if normalized_role not in ROW_ROLES:
            raise ValueError(
                f"disclosure template catalog {location} unsupported row role: "
                f"{normalized_role}"
            )
        output.append(
            (
                normalized_role,
                _string_tuple(
                    markers,
                    "row_role_markers",
                    f"{location}.{normalized_role}",
                ),
            )
        )
    return tuple(sorted(output))


def _scope_is_effective(scope: DisclosureRuleScope, cutoff: date) -> bool:
    start = _parse_date(scope.effective_from, "effective_from")
    end = (
        _parse_date(scope.effective_to, "effective_to") if scope.effective_to else None
    )
    return start <= cutoff and (end is None or cutoff <= end)


def _required_text(
    row: Mapping[str, Any],
    key: str,
    *,
    index: Optional[int] = None,
    location: Optional[str] = None,
) -> str:
    value = str(row.get(key) or "").strip()
    if value:
        return value
    resolved = location or ("catalog" if index is None else f"templates[{index}]")
    raise ValueError(f"disclosure template catalog {resolved} missing {key}")


def _enum_tuple(
    row: Mapping[str, Any],
    key: str,
    allowed: set[str],
    *,
    index: int,
) -> tuple[str, ...]:
    values = _string_tuple(row.get(key), key, f"templates[{index}]")
    unsupported = set(values) - allowed
    if unsupported:
        raise ValueError(
            f"disclosure template catalog templates[{index}] unsupported "
            f"{key}: {sorted(unsupported)}"
        )
    return values


def _enum_tuple_at_location(
    row: Mapping[str, Any],
    key: str,
    allowed: set[str],
    *,
    location: str,
) -> tuple[str, ...]:
    values = _string_tuple(row.get(key), key, location)
    unsupported = set(values) - allowed
    if unsupported:
        raise ValueError(
            f"disclosure template catalog {location} unsupported "
            f"{key}: {sorted(unsupported)}"
        )
    return values


def _string_tuple(
    value: Any,
    key: str,
    location: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(
            f"disclosure template catalog {location} {key} must be an array"
        )
    result = tuple(str(item).strip() for item in value if str(item).strip())
    if not allow_empty and not result:
        raise ValueError(
            f"disclosure template catalog {location} {key} must not be empty"
        )
    if len(set(result)) != len(result):
        raise ValueError(
            f"disclosure template catalog {location} {key} contains duplicates"
        )
    return result


def _parse_date(value: Any, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"disclosure template catalog {field_name} must be an ISO date"
        ) from exc

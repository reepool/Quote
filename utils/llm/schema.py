"""JSON Schema normalization and local validation."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Mapping

from .errors import LlmConfigurationError, LlmSchemaValidationError


def normalize_schema(schema: Any) -> dict[str, Any] | None:
    """Normalize a JSON Schema mapping or Pydantic model into a JSON object."""

    if schema is None:
        return None
    if isinstance(schema, Mapping):
        result = deepcopy(dict(schema))
    elif isinstance(schema, type) and hasattr(schema, "model_json_schema"):
        result = deepcopy(schema.model_json_schema())
    elif hasattr(schema, "model_json_schema"):
        result = deepcopy(schema.model_json_schema())
    else:
        raise LlmConfigurationError(
            "response_schema must be a JSON Schema mapping or Pydantic model"
        )
    if not isinstance(result, dict):
        raise LlmConfigurationError("response_schema must normalize to a JSON object")
    try:
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(result)
    except ImportError as exc:
        raise LlmConfigurationError("JSON Schema validation dependency is unavailable") from exc
    except Exception as exc:
        raise LlmConfigurationError("response_schema is not a valid JSON Schema") from exc
    return result


def validate_data(data: Any, schema: Mapping[str, Any]) -> None:
    """Validate data while avoiding values from the payload in diagnostics."""

    try:
        from jsonschema import Draft202012Validator
    except ImportError as exc:
        raise LlmConfigurationError("JSON Schema validation dependency is unavailable") from exc
    validator = Draft202012Validator(schema)
    error = next(iter(sorted(validator.iter_errors(data), key=lambda item: list(item.path))), None)
    if error is None:
        return
    path = ".".join(str(part) for part in error.path) or "$"
    validator_name = str(error.validator or "unknown")
    raise LlmSchemaValidationError(f"schema validation failed at {path} ({validator_name})")


def compact_schema_instruction(schema: Mapping[str, Any]) -> str:
    return (
        "Return only valid JSON matching this schema. Do not add markdown fences, "
        "comments, inferred values, or extra explanatory text. Schema: "
        + json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )

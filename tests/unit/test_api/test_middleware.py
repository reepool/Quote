"""
Unit tests for API middleware helpers.
"""

import pytest

from api.middleware import normalize_repeated_slashes


@pytest.mark.unit
def test_normalize_repeated_slashes_collapses_path_segments():
    assert normalize_repeated_slashes("//api/v1/research/industry/component-sets") == (
        "/api/v1/research/industry/component-sets"
    )
    assert normalize_repeated_slashes("/api//v1//health") == "/api/v1/health"
    assert normalize_repeated_slashes("/api/v1/health") == "/api/v1/health"

import pytest


@pytest.fixture(autouse=True)
def _reset_cninfo_access_runtime():
    from research.providers.cninfo_http import reset_cninfo_access_runtime

    reset_cninfo_access_runtime()
    yield
    reset_cninfo_access_runtime()

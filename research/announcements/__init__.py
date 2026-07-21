"""Source-neutral official announcement acquisition infrastructure."""

from .base import (
    AnnouncementProvider,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementQueryNotSupported,
)
from .config import (
    AnnouncementAcquisitionConfig,
    AnnouncementRouteConfig,
    load_announcement_acquisition_config,
)
from .models import (
    AnnouncementAttachment,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    AnnouncementRouteAttempt,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    AnnouncementScope,
    ProviderCursor,
    build_announcement_key,
    normalize_published_at,
)
from .service import AnnouncementAcquisitionService, AnnouncementSelector
from .retrieval import (
    AnnouncementAttachmentRetriever,
    AttachmentRetrievalPolicy,
)

__all__ = [
    "AnnouncementAcquisitionConfig",
    "AnnouncementAcquisitionService",
    "AnnouncementAttachment",
    "AnnouncementAttachmentRetriever",
    "AnnouncementProvider",
    "AnnouncementProviderCapabilities",
    "AnnouncementProviderRegistry",
    "AnnouncementQuery",
    "AnnouncementQueryNotSupported",
    "AnnouncementRecord",
    "AnnouncementRetrievalResult",
    "AnnouncementRouteAttempt",
    "AnnouncementRouteConfig",
    "AnnouncementRouteResult",
    "AnnouncementScanResult",
    "AnnouncementScope",
    "AnnouncementSelector",
    "AttachmentRetrievalPolicy",
    "ProviderCursor",
    "build_announcement_key",
    "load_announcement_acquisition_config",
    "normalize_published_at",
]

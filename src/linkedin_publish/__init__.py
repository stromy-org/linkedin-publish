"""Official LinkedIn publishing client and durable, approval-gated publication service.

Client-neutral by construction: nothing here reads `client-data`, resolves a
`client_slug`, parses an Entra claim or knows what a brand is. It takes resolved
data and opaque trusted subjects. Brand knowledge lives in the L3 skills, client
context in the plugin layer, and credential resolution in the hosted MCP.

Two layers, usable independently:

* `LinkedInClient` — the transport. Two adapters, explicit media paths, typed
  failures. No database required, which is what makes it testable against wire
  fixtures with no credential at all.
* `PublicationService` — the durable, approval-gated ledger on top. Every hosted
  publishing path uses this; it is the only thing that can promise an accepted
  post is not sent a second time.
"""

from .auth import CredentialProvider, Credentials, StaticCredentialProvider, TokenObservation
from .client import LinkedInClient, PreparedPost
from .errors import (
    AuthorForbidden,
    CapabilityUnavailable,
    CredentialExpired,
    CredentialMissing,
    FailureCode,
    LinkedInPublishError,
    ProviderRejected,
    PublishOutcomeUnknown,
    QuotaDeferred,
    TransientReadFailure,
    ValidationFailure,
)
from .limits import BudgetStore, InMemoryBudgetStore, QuotaLimiter, QuotaProfile, profile_for
from .manifest import ManifestEntry, PublishManifest, canonical_digest
from .media import AssetReader, ResolvedAsset
from .models import (
    AccountBinding,
    Adapter,
    ArticleDraft,
    Capability,
    CapabilityStatus,
    DocumentDraft,
    ImageDraft,
    MediaRef,
    PostDraft,
    PublishReceipt,
    Visibility,
)
from .service import BindingLoader, PublicationService, TickResult
from .store import (
    ApprovalRecord,
    CommissioningGrant,
    InMemoryPublicationStore,
    PublicationKey,
    PublicationRecord,
    PublicationState,
    PublicationStore,
    StoreConflict,
)

__version__ = "0.1.0"

__all__ = [
    "AccountBinding",
    "Adapter",
    "ApprovalRecord",
    "ArticleDraft",
    "AssetReader",
    "AuthorForbidden",
    "BindingLoader",
    "BudgetStore",
    "Capability",
    "CapabilityStatus",
    "CapabilityUnavailable",
    "CommissioningGrant",
    "CredentialExpired",
    "CredentialMissing",
    "CredentialProvider",
    "Credentials",
    "DocumentDraft",
    "FailureCode",
    "ImageDraft",
    "InMemoryBudgetStore",
    "InMemoryPublicationStore",
    "LinkedInClient",
    "LinkedInPublishError",
    "ManifestEntry",
    "MediaRef",
    "PostDraft",
    "PreparedPost",
    "ProviderRejected",
    "PublicationKey",
    "PublicationRecord",
    "PublicationService",
    "PublicationState",
    "PublicationStore",
    "PublishManifest",
    "PublishOutcomeUnknown",
    "PublishReceipt",
    "QuotaDeferred",
    "QuotaLimiter",
    "QuotaProfile",
    "ResolvedAsset",
    "StaticCredentialProvider",
    "StoreConflict",
    "TickResult",
    "TokenObservation",
    "TransientReadFailure",
    "ValidationFailure",
    "Visibility",
    "__version__",
    "canonical_digest",
    "profile_for",
]

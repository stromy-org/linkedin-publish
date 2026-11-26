"""Platform-neutral, approval-gated social publishing — the canonical namespace.

`social_publish` is the canonical import namespace of this distribution
(ORG-PLAN-652 C5a). The distribution is still `linkedin-publish`, and every
public `linkedin_publish` import keeps working unchanged; C5b renames the
distribution after the Meta house canary.

**The core** owns what is the same on every platform: the immutable intent and
its digest (`canonical`), the scoped approval and its trusted import
(`approval`), the scheduling window, claims, attempt state and its transition
table (`states`), receipts and reconciliation (`ledger`, `dispatch`).

**An adapter** owns what only its provider knows: native identifiers,
credentials, format and visibility validation, media staging, rate-limit
interpretation and provider read-back (`adapters.meta`, `adapters.linkedin`).

Capability is declared per account, operation and format. The core never
silently downgrades a format, and never sends through another adapter after an
uncertain response.

Client-neutral by construction, like `linkedin_publish`: nothing here reads
client data, resolves a client slug or knows what a brand is.

`social_publish.linkedin` re-exports the supported LinkedIn API.
"""

from .adapters import (
    GRAPH_VERSION,
    MetaCredentials,
    MetaGraphAdapter,
    MetaTokenProvider,
    Resolution,
    SendResult,
    SignedMediaUrls,
    SocialAdapter,
    StageResult,
)
from .approval import ImportSource, TrustedKey, import_c3_approval, load_trust, verify_record
from .canonical import canonical_digest, canonical_json
from .dispatch import DispatchService, PolicyGate, SocialTick
from .errors import (
    ApprovalRefused,
    CapabilityRefused,
    Deferred,
    OutcomeUnknown,
    Refused,
    SocialPublishError,
    StoreConflict,
)
from .ledger import ImportOutcome, InMemorySocialStore, SocialApproval, SocialEvent, SocialStore
from .model import (
    META_PAYLOAD_VERSION,
    MetaAccount,
    MetaParent,
    MetaPayload,
    OperationCapability,
    Platform,
    SocialBinding,
    SocialKey,
    SocialRecord,
    meta_payload_digest,
)
from .states import NEVER_FRESH, TRANSITIONS, SocialState

__version__ = "0.8.0"

__all__ = [
    "ApprovalRefused",
    "CapabilityRefused",
    "Deferred",
    "DispatchService",
    "GRAPH_VERSION",
    "ImportOutcome",
    "ImportSource",
    "InMemorySocialStore",
    "META_PAYLOAD_VERSION",
    "MetaAccount",
    "MetaCredentials",
    "MetaGraphAdapter",
    "MetaParent",
    "MetaPayload",
    "MetaTokenProvider",
    "NEVER_FRESH",
    "OperationCapability",
    "OutcomeUnknown",
    "Platform",
    "PolicyGate",
    "Refused",
    "Resolution",
    "SendResult",
    "SignedMediaUrls",
    "SocialAdapter",
    "SocialApproval",
    "SocialBinding",
    "SocialEvent",
    "SocialKey",
    "SocialPublishError",
    "SocialRecord",
    "SocialState",
    "SocialStore",
    "SocialTick",
    "StageResult",
    "StoreConflict",
    "TRANSITIONS",
    "TrustedKey",
    "__version__",
    "canonical_digest",
    "canonical_json",
    "import_c3_approval",
    "load_trust",
    "meta_payload_digest",
    "verify_record",
]

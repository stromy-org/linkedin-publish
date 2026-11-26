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

**Commissioning** (`commissioning`, `bindings`, `tokens`) mirrors LinkedIn's
canary model for Meta: a short-lived grant for one stored publication and one
`unknown` (operation, format), usable only by a `DispatchService` built with a
commissioning actor and consumed at `sending`; enablement only from that
send's own published (or scheduled-and-read-back) receipt; and token-expiry
evidence measured from Meta's `debug_token`, including explicit no-expiry.

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
from .approval import (
    C3_HANDOVER_VERSION,
    ImportSource,
    TrustedKey,
    import_c3_approval,
    import_c3_handover,
    load_trust,
    verify_record,
)
from .bindings import InMemorySocialBindingStore, SocialBindingStore
from .canonical import canonical_digest, canonical_json
from .commissioning import (
    GRANT_TTL,
    commission_problems,
    enable_problems,
    enabled_binding,
    new_grant,
    reenable_problems,
    registration_problems,
    rotated_binding,
)
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
from .ledger import (
    ImportOutcome,
    InMemorySocialStore,
    SocialApproval,
    SocialEvent,
    SocialGrant,
    SocialStore,
    capability_key,
)
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
from .rights import normalize_rights_evidence, require_rights, rights_evidence_of, rights_problems
from .states import NEVER_FRESH, TRANSITIONS, SocialState
from .tokens import MetaTokenEvidence, inspect_meta_token, meta_token_status, parse_debug_token

__version__ = "0.10.0"

__all__ = [
    "C3_HANDOVER_VERSION",
    "GRANT_TTL",
    "ApprovalRefused",
    "CapabilityRefused",
    "Deferred",
    "DispatchService",
    "GRAPH_VERSION",
    "ImportOutcome",
    "ImportSource",
    "InMemorySocialBindingStore",
    "InMemorySocialStore",
    "META_PAYLOAD_VERSION",
    "MetaAccount",
    "MetaCredentials",
    "MetaGraphAdapter",
    "MetaParent",
    "MetaPayload",
    "MetaTokenEvidence",
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
    "SocialBindingStore",
    "SocialEvent",
    "SocialGrant",
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
    "capability_key",
    "commission_problems",
    "enable_problems",
    "enabled_binding",
    "import_c3_approval",
    "import_c3_handover",
    "inspect_meta_token",
    "load_trust",
    "meta_payload_digest",
    "meta_token_status",
    "new_grant",
    "normalize_rights_evidence",
    "parse_debug_token",
    "reenable_problems",
    "registration_problems",
    "require_rights",
    "rights_evidence_of",
    "rights_problems",
    "rotated_binding",
    "verify_record",
]

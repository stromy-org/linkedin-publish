"""The one switch for CMA organization capabilities (ORG-PLAN-285 C7).

Three capabilities act *as an organization* and need LinkedIn's Community
Management API product rather than the self-service Share product:

* **organization publishing** — any post whose author is `urn:li:organization:*`
  (text, article, image or document; `w_organization_social` plus an
  ADMINISTRATOR, DIRECT_SPONSORED_CONTENT_POSTER or CONTENT_ADMIN Page role);
* **organization documents** — a Documents API upload owned by an organization,
  then a post that references it (the same scope and roles);
* **organization share statistics** — `organizationalEntityShareStatistics`
  (`rw_organization_admin` plus an ADMINISTRATOR role).

All three ship **disabled by default**. The adapters, their wire shapes and
their fixture tests exist; what does not exist yet is an approved CMA app, so
the code refuses until a deployment turns on `cma_organization_enabled`
explicitly. External CMA approval is not a requirement of the personal pilot,
and a personal (`urn:li:person:*`) post never touches this switch.

The switch is one boolean, threaded down from `LinkedInClient` to both adapters
and to `fetch_share_statistics`, so every public entry point refuses on its own
rather than relying on the caller to have gone through the client. It is
necessary, never sufficient: commissioning evidence on the binding and the
organization allowlist still apply.
"""

from __future__ import annotations

from typing import Final

from .errors import CmaOrganizationDisabled
from .urns import is_organization

__all__ = ["CMA_ORGANIZATION_SWITCH", "require_cma_organization", "require_for_author"]

#: The name of the switch, as it appears in every refusal. One name everywhere:
#: the `LinkedInClient`, adapter and `fetch_share_statistics` keyword argument.
CMA_ORGANIZATION_SWITCH: Final = "cma_organization_enabled"


def require_cma_organization(enabled: bool, *, capability: str, what: str) -> None:
    """Refuse a CMA organization capability unless the switch is on.

    Raised before any network call, so a refusal costs nothing and leaks
    nothing.
    """
    if enabled:
        return
    raise CmaOrganizationDisabled(
        f"{what}: CMA organization capabilities are disabled by default; "
        f"the explicit switch is `{CMA_ORGANIZATION_SWITCH}` "
        f"(LinkedInClient({CMA_ORGANIZATION_SWITCH}=True)). Turn it on only for a "
        "deployment whose app has the Community Management API product; the binding "
        "must still carry commissioned evidence for this capability",
        capability=capability,
        switch=CMA_ORGANIZATION_SWITCH,
    )


def require_for_author(enabled: bool, author_urn: str, *, capability: str, what: str) -> None:
    """Refuse when `author_urn` is an organization and the switch is off.

    A person author returns at once: personal publishing never depends on it.
    """
    if is_organization(author_urn):
        require_cma_organization(enabled, capability=capability, what=what)

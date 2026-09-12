"""Protocol versions and endpoints, configured in one place.

`LINKEDIN_VERSION` is a *candidate* recorded at authoring time, not a promise.
LinkedIn's versioned REST surface retires versions on its own schedule, and this
library invents no sunset date. Recheck the supported list at each release and at
commissioning, and change it here — never per call site.
"""

from __future__ import annotations

from typing import Final

__all__ = [
    "API_BASE",
    "LINKEDIN_VERSION",
    "OAUTH_BASE",
    "RESTLI_PROTOCOL_VERSION",
    "UGC_POSTS_PATH",
    "USERINFO_PATH",
    "rest_headers",
    "ugc_headers",
]

API_BASE: Final = "https://api.linkedin.com"
OAUTH_BASE: Final = "https://www.linkedin.com"

#: Initial candidate from the plan. Verify against the portal's supported list.
LINKEDIN_VERSION: Final = "202608"
RESTLI_PROTOCOL_VERSION: Final = "2.0.0"

UGC_POSTS_PATH: Final = "/v2/ugcPosts"
UGC_ASSET_REGISTER_PATH: Final = "/v2/assets?action=registerUpload"
REST_POSTS_PATH: Final = "/rest/posts"
REST_IMAGES_INITIALIZE_PATH: Final = "/rest/images?action=initializeUpload"
REST_DOCUMENTS_INITIALIZE_PATH: Final = "/rest/documents?action=initializeUpload"
REST_SHARE_STATISTICS_PATH: Final = "/rest/organizationalEntityShareStatistics"
INTROSPECT_PATH: Final = "/oauth/v2/introspectToken"
USERINFO_PATH: Final = "/v2/userinfo"


def _auth(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def ugc_headers(access_token: str) -> dict[str, str]:
    """Headers for the legacy UGC surface.

    UGC carries the Rest.li protocol header but *not* `LinkedIn-Version`: it is
    not part of the versioned API, and sending one is not harmless noise — it is
    how the two surfaces get conflated.
    """
    return {
        **_auth(access_token),
        "X-Restli-Protocol-Version": RESTLI_PROTOCOL_VERSION,
        "Content-Type": "application/json",
    }


def rest_headers(access_token: str, *, version: str = LINKEDIN_VERSION) -> dict[str, str]:
    """Headers for the versioned REST surface."""
    return {
        **_auth(access_token),
        "LinkedIn-Version": version,
        "X-Restli-Protocol-Version": RESTLI_PROTOCOL_VERSION,
        "Content-Type": "application/json",
    }

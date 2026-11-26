"""The supported LinkedIn API under the canonical namespace.

`from social_publish.linkedin import LinkedInClient, PublicationService, ...` is
the same objects as `from linkedin_publish import ...`: one implementation, two
import paths, until C5b announces otherwise. `LinkedInAdapter` presents it to
the neutral core.
"""

from linkedin_publish import *  # noqa: F403 - the compatibility surface, by definition
from linkedin_publish import __all__ as _linkedin_all

from .adapters.linkedin import LINKEDIN_DECLARED, LinkedInAdapter, neutral_binding

__all__ = [*_linkedin_all, "LINKEDIN_DECLARED", "LinkedInAdapter", "neutral_binding"]  # pyright: ignore[reportUnsupportedDunderAll]

"""Transport adapters.

Two complete, independent paths. The adapter is selected from the trusted
binding *before* validation or upload, and is never reselected at runtime: a
403 from `rest_posts` does not cause a `share_ugc` attempt. That fallback is how
a post ends up published by the wrong product with the wrong asset URNs, and how
a permission problem gets misread as an adapter problem.
"""

from __future__ import annotations

from .base import PostAdapter, PublishOutcome
from .rest import RestPostsAdapter
from .ugc import ShareUgcAdapter

__all__ = ["PostAdapter", "PublishOutcome", "RestPostsAdapter", "ShareUgcAdapter"]

"""Exceptions raised by linkedin_publish."""

from __future__ import annotations


class LinkedinPublishError(Exception):
    """Base exception for linkedin_publish."""


class DependencyError(LinkedinPublishError):
    """An optional dependency is missing.

    Raised when an optional-extra feature is used but the required dependency
    isn't installed. The message should tell the caller exactly which extra to install.
    """

    def __init__(self, extra: str, package: str) -> None:
        super().__init__(
            f"Missing optional dependency '{package}'. Install with: "
            f"uv pip install 'linkedin-publish[{extra}]'"
        )
        self.extra = extra
        self.package = package

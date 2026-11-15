"""Shared Huawei SmartHome exceptions."""

from __future__ import annotations


class HuaweiSmartHomeError(Exception):
    """Base exception for the integration."""


class InvalidProtocolDataError(HuaweiSmartHomeError):
    """A remote response could not be normalized safely."""


class AuthenticationError(HuaweiSmartHomeError):
    """Authentication or token exchange failed."""

    def __init__(
        self,
        message: str,
        *,
        ui_error: str | None = None,
        remote_code: str | None = None,
        remote_description: str | None = None,
    ) -> None:
        super().__init__(message)
        self.ui_error = ui_error
        self.remote_code = remote_code
        self.remote_description = remote_description


class InvalidCredentialsError(AuthenticationError):
    """The account or challenge code was rejected."""


class TransientAuthenticationError(AuthenticationError):
    """Authentication transport or service failure that may recover."""


class ReauthenticationRequired(AuthenticationError):
    """The user must authenticate again."""

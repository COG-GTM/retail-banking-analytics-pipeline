"""Secret resolution for the Synapse Spark jobs.

Replaces the ``{SAS004}`` encoded passwords that were embedded in
sas/macros/connect_teradata.sas. Secrets now live in Azure Key Vault
(TICKET-02) and are resolved at runtime by the service principal that runs the
Synapse Spark pool.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

try:  # pragma: no cover - exercised only on a Synapse Spark pool
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
except ImportError:  # pragma: no cover - azure SDK absent in unit-test env
    DefaultAzureCredential = None
    SecretClient = None


class SecretResolver(Protocol):
    """Resolves a named secret to its value."""

    def get_secret(self, name: str) -> str: ...


class KeyVaultSecretResolver:
    """Resolves secrets from Azure Key Vault, caching values per process."""

    def __init__(self, vault_url: str, client: object | None = None) -> None:
        if client is None:
            if SecretClient is None or DefaultAzureCredential is None:
                raise RuntimeError(
                    "azure-identity and azure-keyvault-secrets are required to "
                    "resolve secrets from Azure Key Vault."
                )
            client = SecretClient(
                vault_url=vault_url, credential=DefaultAzureCredential()
            )
        self._client = client
        self._cache: dict[str, str] = {}

    def get_secret(self, name: str) -> str:
        if name not in self._cache:
            self._cache[name] = self._client.get_secret(name).value
        return self._cache[name]


class MappingSecretResolver:
    """Resolves secrets from an in-memory mapping (local runs and tests)."""

    def __init__(self, secrets: Mapping[str, str]) -> None:
        self._secrets = dict(secrets)

    def get_secret(self, name: str) -> str:
        try:
            return self._secrets[name]
        except KeyError as exc:
            raise KeyError(f"Secret '{name}' is not available") from exc

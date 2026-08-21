"""Secret resolution from Azure Key Vault.

Replaces the ``{SAS004}`` encoded passwords that were checked into
``sas/macros/connect_teradata.sas``. The Key Vault and secret names are the ones
provisioned by TICKET-02; nothing here ever logs a secret value.
"""

from __future__ import annotations

import logging
import os
from typing import Callable, Dict, Optional

LOGGER = logging.getLogger(__name__)


class SecretResolver:
    """Resolve named secrets from Azure Key Vault with an environment fallback.

    The environment fallback exists for local development and unit tests: a
    secret named ``snowflake-synapse-private-key`` is also read from the
    environment variable ``SNOWFLAKE_SYNAPSE_PRIVATE_KEY``.
    """

    def __init__(
        self,
        key_vault_url: str = "",
        client_factory: Optional[Callable[[str], object]] = None,
        environ: Optional[Dict[str, str]] = None,
    ) -> None:
        self._key_vault_url = key_vault_url
        self._client_factory = client_factory or self._default_client_factory
        self._environ = environ if environ is not None else os.environ
        self._client: Optional[object] = None
        self._cache: Dict[str, str] = {}

    @staticmethod
    def _default_client_factory(key_vault_url: str) -> object:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        return SecretClient(vault_url=key_vault_url, credential=DefaultAzureCredential())

    @staticmethod
    def env_var_name(secret_name: str) -> str:
        return secret_name.replace("-", "_").upper()

    def get(self, secret_name: str) -> str:
        if secret_name in self._cache:
            return self._cache[secret_name]

        value = self._from_key_vault(secret_name)
        if value is None:
            value = self._environ.get(self.env_var_name(secret_name))

        if value is None:
            raise KeyError(
                f"Secret '{secret_name}' not found in Key Vault "
                f"('{self._key_vault_url or 'unset'}') or the environment"
            )

        self._cache[secret_name] = value
        return value

    def _from_key_vault(self, secret_name: str) -> Optional[str]:
        if not self._key_vault_url:
            return None
        if self._client is None:
            self._client = self._client_factory(self._key_vault_url)
        try:
            return self._client.get_secret(secret_name).value  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - depends on live Key Vault
            LOGGER.warning("Key Vault lookup failed for secret '%s'", secret_name)
            return None

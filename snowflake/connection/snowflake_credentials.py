"""Snowflake credential resolution for the retail banking analytics pipeline.

Ticket: MBA-2203 (TICKET-02)

Replacement for ``sas/macros/connect_teradata.sas``: instead of {SAS004}
passwords held in source control, every credential is read from Azure Key Vault
at runtime using the caller's Azure identity (the Synapse workspace managed
identity in Synapse, an ``az login`` session on a workstation).

Usage from a Synapse Spark job::

    from snowflake_credentials import SnowflakeCredentials

    creds = SnowflakeCredentials.for_principal("synapse")
    df = (
        spark.read.format("snowflake")
        .options(**creds.spark_options(database="DATA_PRODUCTS_DB"))
        .option("dbtable", "CUSTOMER_MASTER_PROFILE")
        .load()
    )

Nothing in this module writes key material to disk or to the log.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

PRINCIPALS = {
    "synapse": {
        "user_env": "SF_SVC_SYNAPSE_USER",
        "user_default": "SVC_SYNAPSE_SPARK_{env}",
        "role_env": "SF_ROLE_TRANSFORMER",
        "role_default": "RB_TRANSFORMER_{env}",
        "warehouse_env": "SF_WH_SPARK",
        "warehouse_default": "WH_RB_SPARK_{env}",
        "private_key_secret": "snowflake-svc-synapse-private-key",
        "passphrase_secret": "snowflake-svc-synapse-key-passphrase",
    },
    "loader": {
        "user_env": "SF_SVC_LOADER_USER",
        "user_default": "SVC_RB_LOADER_{env}",
        "role_env": "SF_ROLE_LOADER",
        "role_default": "RB_LOADER_{env}",
        "warehouse_env": "SF_WH_ELT",
        "warehouse_default": "WH_RB_ELT_{env}",
        "private_key_secret": "snowflake-svc-loader-private-key",
        "passphrase_secret": "snowflake-svc-loader-key-passphrase",
    },
}


class CredentialError(RuntimeError):
    """Raised when a credential cannot be resolved from Azure Key Vault."""


@dataclass(frozen=True)
class SnowflakeCredentials:
    """Resolved, password-less Snowflake connection settings."""

    account: str
    host: str
    user: str
    role: str
    warehouse: str
    private_key_pem: str
    private_key_passphrase: str

    @classmethod
    def for_principal(cls, principal: str) -> "SnowflakeCredentials":
        """Resolve the key pair for ``synapse`` or ``loader`` from Key Vault."""
        try:
            spec = PRINCIPALS[principal]
        except KeyError:
            raise CredentialError(
                f"Unknown principal '{principal}'; expected one of {sorted(PRINCIPALS)}"
            ) from None

        env = os.environ.get("SF_ENV", "DEV")
        vault_name = os.environ.get("AZ_KEY_VAULT_NAME")
        if not vault_name:
            raise CredentialError(
                "AZ_KEY_VAULT_NAME is not set; source config/snowflake_config.cfg"
            )

        client = SecretClient(
            vault_url=f"https://{vault_name}.vault.azure.net",
            credential=DefaultAzureCredential(),
        )

        account = os.environ.get("SF_ACCOUNT")
        if not account:
            raise CredentialError(
                "SF_ACCOUNT is not set; source config/snowflake_config.cfg"
            )

        return cls(
            account=account,
            host=os.environ.get(
                "SF_HOST", f"{account}.{os.environ.get('SF_REGION', '')}.snowflakecomputing.com"
            ),
            user=os.environ.get(spec["user_env"], spec["user_default"].format(env=env)),
            role=os.environ.get(spec["role_env"], spec["role_default"].format(env=env)),
            warehouse=os.environ.get(
                spec["warehouse_env"], spec["warehouse_default"].format(env=env)
            ),
            private_key_pem=_read_secret(client, spec["private_key_secret"]),
            private_key_passphrase=_read_secret(client, spec["passphrase_secret"]),
        )

    def spark_options(self, database: str, schema: str = "PUBLIC") -> Dict[str, str]:
        """Options for the Spark Snowflake connector (net.snowflake.spark.snowflake)."""
        return {
            "sfUrl": self.host,
            "sfUser": self.user,
            "sfRole": self.role,
            "sfWarehouse": self.warehouse,
            "sfDatabase": database,
            "sfSchema": schema,
            "pem_private_key": _der_body(self.private_key_pem, self.private_key_passphrase),
        }

    def connector_options(self, database: str, schema: str = "PUBLIC") -> Dict[str, object]:
        """Keyword arguments for snowflake.connector.connect()."""
        return {
            "account": self.account,
            "user": self.user,
            "role": self.role,
            "warehouse": self.warehouse,
            "database": database,
            "schema": schema,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key": _der_bytes(self.private_key_pem, self.private_key_passphrase),
        }


def _read_secret(client: SecretClient, name: str) -> str:
    try:
        value = client.get_secret(name).value
    except Exception as exc:  # noqa: BLE001 - surfaced with the secret name for triage
        raise CredentialError(f"Could not read Key Vault secret '{name}'") from exc

    if not value:
        raise CredentialError(f"Key Vault secret '{name}' is empty")
    return value


def _der_bytes(private_key_pem: str, passphrase: str) -> bytes:
    """Decrypt the PEM key and re-encode it as unencrypted PKCS8 DER in memory."""
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=passphrase.encode("utf-8") if passphrase else None,
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _der_body(private_key_pem: str, passphrase: str) -> str:
    """Base64 body the Spark connector expects for pem_private_key."""
    import base64

    return base64.b64encode(_der_bytes(private_key_pem, passphrase)).decode("ascii")

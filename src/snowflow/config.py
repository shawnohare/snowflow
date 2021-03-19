from __future__ import annotations

import logging
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Union, Optional

import yaml

from .utils import filter_dict_nulls

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Loader for the file containing data flow configurations."""

    default_path = Path(
        os.getenv("XDG_CONFIG_HOME", "~/.config"), "snowflow", "config.yml"
    )
    _cache = {}
    path = None

    @classmethod
    def load(cls, val: Union[str, Path, dict, None] = None) -> dict:
        """Wrapper for the module-level load_config function."""
        # TODO: Watch file(s) and look for changes.
        if val is None:
            path = cls.default_path
        elif isinstance(val, (str, Path)):
            path = Path(val)
        else:
            path = None

        if isinstance(val, dict):
            conf = val
        else:
            conf = cls._cache.get(path, {})
            if not conf:
                with path.expanduser().open() as file:
                    conf: dict = yaml.safe_load(file)
                cls._cache[path] = conf
        return conf

    @staticmethod
    def load_snowflake_creds_from_env() -> dict[str, str]:
        creds = {
            "account": os.getenv("SNOWSQL_ACCOUNT", None),
            "user": os.getenv("SNOWSQL_USER", None),
            "password": os.getenv("SNOWSQL_PASSWORD", None),
            "database": os.getenv("SNOWSQL_DATABASE", None),
            "warehouse": os.getenv("SNOWSQL_WAREHOUSE", None),
            "schema": os.getenv("SNOWSQL_SCHEMA", None),
            "role": os.getenv("SNOWSQL_ROLE", None),
        }
        return filter_dict_nulls(creds)

    @staticmethod
    def load_snowflake_creds_from_snowsql_config(
        config_path: str = "~/.snowsql/config",
        connection: Optional[str] = None,
    ) -> dict[str, str]:
        """Parse a snowsql client configuration file to get credentials.

        Args:
            config_path: Path to the snowsql cli config.
            connection: Named connection. If "default", use the default
                connection values under `[connections]`. Otherwise, use
                `[connections.{connection}`. When None is passed,
                a dict with null values is returned for compatibility.

        Returns:
            Credential dict with keys renamed to those expected by a
            snowflake connection.

        """
        creds = {}
        if isinstance(connection, str):
            conf = ConfigParser()
            conf.read(Path(config_path).expanduser())
            if connection is "default":
                key = "connections"
            else:
                key = f"connections.{connection}"
            try:
                creds = dict(conf[key])
            except KeyError:
                pass

        # Now convert to kwargs expected by a Snowflake connection.
        creds = {
            "account": creds.get("accountname"),
            "user": creds.get("username"),
            "password": creds.get("password"),
            "warehouse": creds.get("warehousename"),
            "database": creds.get("dbname"),
            "schema": creds.get("schemaname"),
            "role": creds.get("rolename"),
            # proxy_host = defaultproxyhost
            # proxy_port = defaultproxyport
        }
        return filter_dict_nulls(creds)

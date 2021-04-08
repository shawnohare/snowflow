"""Configuration section for RDS Snapshot s3 Export inflow.
"""
from __future__ import annotations

import logging
from copy import deepcopy
from typing import Union
from pathlib import Path

from ....config import ConfigLoader
from ....typemap import TypeMap, KeywordMap
from ....utils import apply_filter, default_filter, filter_dict_nulls, list_to_dict
from .metadata import load_metadata

logger = logging.getLogger(__name__)


def merge_metadata(node: dict, meta: dict) -> dict:
    """Parse export metadata and apply configuration filters,
    such as exclusion of specific tables or columns.

    Export metadata contains information such as table schemas. This
    metadata is filtered and configuration overrides applied. The
    result is merged into the instances configuration.

    Args:
        node: RDS Instance configuration node.
        meta: Metadata for an RDS export.
    """
    for schema_name, schema in node["schemas"].items():
        # For each schema config defined in the config, get  all table metadata.
        # Look to see if the table should be included in an import, and
        # if so, apply configuration overrides to table metadata.
        tables = schema["tables"]
        print(tables)
        for table_meta in meta["schemas"][schema_name]["tables"]:
            table_filt = schema["filter"]
            table_name = table_meta["name"]
            if not apply_filter(table_filt, table_name):
                continue

            # Take the table metadata and apply any configuration overrides.
            columns_meta = table_meta.pop("columns", [])
            table = table_meta | tables.get(table_name, {})

            table.setdefault("filter", default_filter())
            table.setdefault('command', schema['command'])
            tables[table_name] = table
            table.setdefault("snowflake", {})
            table["snowflake"].setdefault("table", table_name)
            column_filt = table.setdefault("filter", default_filter())
            columns = table.setdefault("columns", {})

            # Filter out exported columns as determined in the
            # configuration. Convert source types to Snowflake types,
            # either functionally or by using a configuration override.
            for column_meta in columns_meta:
                column_name = column_meta["name"]
                if not apply_filter(column_filt, column_name):
                    continue
                column = column_meta | columns.get(column_name, {})
                columns[column_name] = column
                if "snowflake_type" not in column:
                    column['name'] = KeywordMap.map(column['name'])
                    column["snowflake_type"] = TypeMap.map(
                        meta=column,
                        src=node["type"],
                        tar='snowflake',
                    )
    return node


class Config(dict):
    """Configuration section for an RDS Snapshot Export to S3."""

    @staticmethod
    def load(val: Union[str, Path, dict, None] = None) -> Config:
        key = "inflow.s3.rdsexport"
        main_conf = ConfigLoader.load(val)
        conf = main_conf[key]
        conf['snowflake'] = main_conf.get('snowflake', {}) | conf.get('snowflake', {})
        conf['s3'] = main_conf.get('s3', {}) | conf.get('s3', {})
        conf = Config(conf).parse()
        main_conf[key] = conf
        return conf

    def parse(self) -> Config:
        """
        Access the main config's "inflow.s3.rdsexport" section and
        parse the subsections.

        The configuration serves primarily to
        1. Set the s3 bucket / prefix for RDS snapshot exports.
        2. Establish snowflake credentials and route RDS instance schemas
           to the appropriate Snowflake database / schema.
        3. Filter which RDS schemas to import.
        4. Filter which tables to import.
        5. Filter which columns to import.
        6. A way to explicitly set the Snowflake type, defaults and properties
           of a Snowflake table column.

        An RDS export provides metadata such as a complete list of tables and
        their schemas. This data is filtered and overriden by the configuration
        to produce a transformed subset of the metadata.

        Snowflake credentials will be loaded in the following order
        - From the snowsql config file at ~/.snowsql/config
        - From the application config.
        - From the environment.

        The resulting configuration covers all use-cases.
        Each RDS instance configuration within the "sources"
        section should be self-contained for the purposes of
        snapshot importing. Top-level Snowflake and s3
        configurations are used as defaults for RDS instance specific
        configurations.

        DB schema and table configuration lists are converted to order-preserving
        dicts upon load for ease of use when parsing export metadata.

        TODO: We should use the confuse package to manage configs
        instead of an adhoc method like we are doing here.

        """
        conf = self
        if conf.get("__parsed__", False):
            return self
        if "s3" not in conf or "bucket" not in conf["s3"]:
            raise ValueError("No s3 bucket.")
        conf["sources"] = list_to_dict(conf["sources"])
        conf.setdefault("snowflake", {})

        # Insert Snowflake credentials.
        sf_creds_from_snowsql = ConfigLoader.load_snowflake_creds_from_snowsql_config(
            connection=conf["snowflake"].get("connection", "default")
        )
        sf_creds_from_conf = filter_dict_nulls(conf["snowflake"])
        sf_creds_from_env = ConfigLoader.load_snowflake_creds_from_env()
        conf["snowflake"] = (
            sf_creds_from_snowsql | sf_creds_from_conf | sf_creds_from_env
        )

        # TODO: Move
        for instance_name, instance in conf["sources"].items():
            instance["s3"] = conf["s3"] | instance["s3"]
            instance["schemas"] = list_to_dict(instance.get("schemas", []))
            instance.setdefault('command', 'writenx')
            for schema in instance["schemas"].values():
                # Apply defaults / type transform for each schema config.
                schema.setdefault('command', instance['command'])
                schema["instance"] = instance_name
                schema["tables"] = list_to_dict(schema.get("tables", []))
                schema.setdefault("tables", [])
                schema.setdefault("filter", default_filter())
                schema["filter"]["items"] = set(schema["filter"]["items"])

                # Apply defaults / type transforms for each table config
                for table in schema["tables"].values():
                    table.setdefault("filter", default_filter())
                    table.setdefault('command', schema['command'])
                    table["filter"]["items"] = set(table["filter"]["items"])
                    table["columns"] = list_to_dict(table.get("columns", []))

            # Merge in export metadata into the instance (export) node.
            metadata = load_metadata(instance["s3"]["bucket"], instance["s3"]["prefix"])
            merge_metadata(instance, metadata)

        self["__parsed__"] = True
        return self

    def get_schema(self, instance: str, schema: str) -> dict:
        """Return a copy of an RDS schema configuration section with default values from
        the instance and global configuration set.

        Each schema configuration node should look like
            {
                "s3": {
                    "bucket": "RDS export snapshot bucket",
                    "prefix": "s3 prefix in bucket pointing to schema tables.",
                    "path": "The joined path/prefix."
                },
                "snowflake": {
                    "account": "Snowflake account.",
                    "user": "Snowflake user.",
                    "password": "Snowflake user password.",
                    "database": "Snowflake database.",
                    "schema": "Snowflake schema corresponding to RDS instance schema.",
                    "warehouse": "Snowflake warehouse",
                    "role": "Snowflake role to use.",
                    "aws_integration": "The Snowflake AWS Integration to use.",
                },
                "instance": "RDS instance name where this schema originates.",
                "name": "RDS schema name.",
                "filter": "dict, a table filter"
                "tables": "dict, a collection of table configurations."
            }
        The snowflake node is passed to the Snowflake connector as credentials.
        """
        inode = self["sources"][instance]
        snode = deepcopy(inode["schemas"][schema])
        snode["snowflake"] = (
            self["snowflake"] | inode.get("snowflake", {}) | snode.get("snowflake", {})
        )
        snode["snowflake"].setdefault("schema", snode["name"])
        snode["s3"] = self["s3"] | inode["s3"]
        snode["s3"]["prefix"] = f"{snode['s3']['prefix']}/{snode['name']}"
        snode["s3"]["path"] = "/".join([snode["s3"]["bucket"], snode["s3"]["prefix"]])
        return snode
